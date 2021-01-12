'''
The main web / websocket server
'''

import asyncio
from datetime import datetime
import functools
import importlib
import json
import logging
import os
import pwd, grp
import ssl
import sys
import traceback
from typing import (
    Any,
    AsyncIterator,
    Final,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    cast,
)

from aiohttp import web
import aiohttp_cors
import aiojobs.aiohttp
import aiotools
from aiopg.sa import create_engine
from aiopg.sa.engine import get_dialect
import click
from pathlib import Path
from setproctitle import setproctitle

from ai.backend.common import redis
from ai.backend.common.cli import LazyGroup
from ai.backend.common.utils import env_info, current_loop
from ai.backend.common.json import ExtendedJSONEncoder
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.plugin.hook import HookPluginContext, ALL_COMPLETED, PASSED
from ai.backend.common.plugin.monitor import (
    ErrorPluginContext,
    StatsPluginContext,
    INCREMENT,
)

from ..manager import __version__
from ..manager.background import BackgroundTaskManager
from ..manager.exceptions import InvalidArgument
from ..manager.idle import create_idle_checkers
from ..manager.models.storage import StorageSessionManager
from ..manager.plugin.webapp import WebappPluginContext
from ..manager.registry import AgentRegistry
from ..manager.scheduler.dispatcher import SchedulerDispatcher
from .config import (
    LocalConfig,
    SharedConfig,
    load as load_config,
    volume_config_iv,
)
from .defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB, REDIS_STREAM_DB
from .events import EventDispatcher
from .exceptions import (
    BackendError,
    MethodNotAllowed,
    GenericNotFound,
    GenericBadRequest,
    InternalServerError,
    InvalidAPIParameters,
)
from .types import (
    AppCreator,
    WebRequestHandler, WebMiddleware,
    CleanupContext,
)
from . import ManagerStatus

VALID_VERSIONS: Final = frozenset([
    # 'v1.20160915',  # deprecated
    # 'v2.20170315',  # deprecated
    # 'v3.20170615',  # deprecated

    # authentication changed not to use request bodies
    'v4.20181215',

    # added & enabled streaming-execute API
    'v4.20190115',

    # changed resource/image formats
    'v4.20190315',

    # added user mgmt and ID/password authentication
    # added domain/group/scaling-group
    # added domain/group/scaling-group ref. fields to user/keypair/vfolder objects
    'v4.20190615',

    # added mount_map parameter when creating kernel
    # changed GraphQL query structures for multi-container bundled sessions
    'v5.20191215',

    # rewrote vfolder upload/download APIs to migrate to external storage proxies
    'v6.20200815',
])
LATEST_REV_DATES: Final = {
    1: '20160915',
    2: '20170915',
    3: '20181215',
    4: '20190615',
    5: '20191215',
    6: '20200815',
}
LATEST_API_VERSION: Final = 'v6.20200815'

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))

PUBLIC_INTERFACES: Final = [
    'pidx',
    'background_task_manager',
    'local_config',
    'shared_config',
    'dbpool',
    'registry',
    'redis_live',
    'redis_stat',
    'redis_image',
    'redis_stream',
    'event_dispatcher',
    'idle_checkers',
    'storage_manager',
    'stats_monitor',
    'error_monitor',
    'hook_plugin_ctx',
]

public_interface_objs: MutableMapping[str, Any] = {}


async def hello(request: web.Request) -> web.Response:
    """
    Returns the API version number.
    """
    return web.json_response({
        'version': LATEST_API_VERSION,
        'manager': __version__,
    })


async def on_prepare(request: web.Request, response: web.StreamResponse) -> None:
    response.headers['Server'] = 'BackendAI'


@web.middleware
async def api_middleware(request: web.Request,
                         handler: WebRequestHandler) -> web.StreamResponse:
    _handler = handler
    method_override = request.headers.get('X-Method-Override', None)
    if method_override:
        request = request.clone(method=method_override)
        new_match_info = await request.app.router.resolve(request)
        if new_match_info is None:
            raise InternalServerError('No matching method handler found')
        _handler = new_match_info.handler
        request._match_info = new_match_info  # type: ignore  # this is a hack
    ex = request.match_info.http_exception
    if ex is not None:
        # handled by exception_middleware
        raise ex
    new_api_version = request.headers.get('X-BackendAI-Version')
    legacy_api_version = request.headers.get('X-Sorna-Version')
    api_version = new_api_version or legacy_api_version
    try:
        if api_version is None:
            path_major_version = int(request.match_info.get('version', 5))
            revision_date = LATEST_REV_DATES[path_major_version]
            request['api_version'] = (path_major_version, revision_date)
        elif api_version in VALID_VERSIONS:
            hdr_major_version, revision_date = api_version.split('.', maxsplit=1)
            request['api_version'] = (int(hdr_major_version[1:]), revision_date)
        else:
            return GenericBadRequest('Unsupported API version.')
    except (ValueError, KeyError):
        return GenericBadRequest('Unsupported API version.')
    resp = (await _handler(request))
    return resp


@web.middleware
async def exception_middleware(request: web.Request,
                               handler: WebRequestHandler) -> web.StreamResponse:
    app = request.app
    error_monitor = app['error_monitor']
    stats_monitor = app['stats_monitor']
    try:
        await stats_monitor.report_metric(INCREMENT, 'ai.backend.gateway.api.requests')
        resp = (await handler(request))
    except InvalidArgument as ex:
        if len(ex.args) > 1:
            raise InvalidAPIParameters(f"{ex.args[0]}: {', '.join(map(str, ex.args[1:]))}")
        elif len(ex.args) == 1:
            raise InvalidAPIParameters(ex.args[0])
        else:
            raise InvalidAPIParameters()
    except BackendError as ex:
        if ex.status_code == 500:
            log.warning('Internal server error raised inside handlers')
        await error_monitor.capture_exception()
        await stats_monitor.report_metric(INCREMENT, 'ai.backend.gateway.api.failures')
        await stats_monitor.report_metric(INCREMENT, f'ai.backend.gateway.api.status.{ex.status_code}')
        raise
    except web.HTTPException as ex:
        await stats_monitor.report_metric(INCREMENT, 'ai.backend.gateway.api.failures')
        await stats_monitor.report_metric(INCREMENT, f'ai.backend.gateway.api.status.{ex.status_code}')
        if ex.status_code == 404:
            raise GenericNotFound
        if ex.status_code == 405:
            concrete_ex = cast(web.HTTPMethodNotAllowed, ex)
            raise MethodNotAllowed(concrete_ex.method, concrete_ex.allowed_methods)
        log.warning('Bad request: {0!r}', ex)
        raise GenericBadRequest
    except asyncio.CancelledError as e:
        # The server is closing or the client has disconnected in the middle of
        # request.  Atomic requests are still executed to their ends.
        log.debug('Request cancelled ({0} {1})', request.method, request.rel_url)
        raise e
    except Exception as e:
        await error_monitor.capture_exception()
        log.exception('Uncaught exception in HTTP request handlers {0!r}', e)
        if app['local_config']['debug']['enabled']:
            raise InternalServerError(traceback.format_exc())
        else:
            raise InternalServerError()
    else:
        await stats_monitor.report_metric(INCREMENT, f'ai.backend.gateway.api.status.{resp.status}')
        return resp


@aiotools.actxmgr
async def shared_config_ctx(app: web.Application) -> AsyncIterator[None]:
    # populate public interfaces
    app['shared_config'] = SharedConfig(
        app,
        app['local_config']['etcd']['addr'],
        app['local_config']['etcd']['user'],
        app['local_config']['etcd']['password'],
        app['local_config']['etcd']['namespace'],
    )
    await app['shared_config'].reload()
    _update_public_interface_objs(app)
    yield
    await app['shared_config'].close()


@aiotools.actxmgr
async def webapp_plugin_ctx(root_app: web.Application) -> AsyncIterator[None]:
    ctx = WebappPluginContext(root_app['shared_config'].etcd, root_app['local_config'])
    await ctx.init()
    root_app['webapp_plugin_ctx'] = ctx
    for plugin_name, plugin_instance in ctx.plugins.items():
        if root_app['pidx'] == 0:
            log.info('Loading webapp plugin: {0}', plugin_name)
        subapp, global_middlewares = await plugin_instance.create_app(root_app['cors_opts'])
        _init_subapp(plugin_name, root_app, subapp, global_middlewares)
    yield
    await ctx.cleanup()


async def manager_status_ctx(app: web.Application) -> AsyncIterator[None]:
    if app['pidx'] == 0:
        mgr_status = await app['shared_config'].get_manager_status()
        if mgr_status is None or mgr_status not in (ManagerStatus.RUNNING, ManagerStatus.FROZEN):
            # legacy transition: we now have only RUNNING or FROZEN for HA setup.
            await app['shared_config'].update_manager_status(ManagerStatus.RUNNING)
            mgr_status = ManagerStatus.RUNNING
        log.info('Manager status: {}', mgr_status)
        tz = app['shared_config']['system']['timezone']
        log.info('Configured timezone: {}', tz.tzname(datetime.now()))
    yield


async def redis_ctx(app: web.Application) -> AsyncIterator[None]:
    app['redis_live'] = await redis.connect_with_retries(
        str(app['shared_config'].get_redis_url(db=REDIS_LIVE_DB)),
        timeout=3.0,
        encoding='utf8',
    )
    app['redis_stat'] = await redis.connect_with_retries(
        str(app['shared_config'].get_redis_url(db=REDIS_STAT_DB)),
        timeout=3.0,
        encoding='utf8',
    )
    app['redis_image'] = await redis.connect_with_retries(
        str(app['shared_config'].get_redis_url(db=REDIS_IMAGE_DB)),
        timeout=3.0,
        encoding='utf8',
    )
    app['redis_stream'] = await redis.connect_with_retries(
        str(app['shared_config'].get_redis_url(db=REDIS_STREAM_DB)),
        timeout=3.0,
        encoding='utf8',
    )
    _update_public_interface_objs(app)
    yield
    app['redis_image'].close()
    await app['redis_image'].wait_closed()
    app['redis_stat'].close()
    await app['redis_stat'].wait_closed()
    app['redis_live'].close()
    await app['redis_live'].wait_closed()
    app['redis_stream'].close()
    await app['redis_stream'].wait_closed()


async def database_ctx(app: web.Application) -> AsyncIterator[None]:
    app['dbpool'] = await create_engine(
        host=app['local_config']['db']['addr'].host, port=app['local_config']['db']['addr'].port,
        user=app['local_config']['db']['user'], password=app['local_config']['db']['password'],
        dbname=app['local_config']['db']['name'],
        echo=bool(app['local_config']['logging']['level'] == 'DEBUG'),
        minsize=8, maxsize=256,
        timeout=60, pool_recycle=120,
        dialect=get_dialect(
            json_serializer=functools.partial(json.dumps, cls=ExtendedJSONEncoder),
        ),
    )
    _update_public_interface_objs(app)
    yield
    app['dbpool'].close()
    await app['dbpool'].wait_closed()


async def event_dispatcher_ctx(app: web.Application) -> AsyncIterator[None]:
    app['event_dispatcher'] = await EventDispatcher.new(app['local_config'], app['shared_config'])
    _update_public_interface_objs(app)
    yield
    await app['event_dispatcher'].close()


async def idle_checker_ctx(app: web.Application) -> AsyncIterator[None]:
    app['idle_checkers'] = await create_idle_checkers(
        app['dbpool'], app['shared_config'], app['event_dispatcher'],
    )
    _update_public_interface_objs(app)
    yield
    for instance in app['idle_checkers']:
        await instance.aclose()


async def storage_manager_ctx(app: web.Application) -> AsyncIterator[None]:
    shared_config: SharedConfig = app['shared_config']
    raw_vol_config = await shared_config.etcd.get_prefix('volumes')
    config = volume_config_iv.check(raw_vol_config)
    app['storage_manager'] = StorageSessionManager(config)
    _update_public_interface_objs(app)
    yield
    await app['storage_manager'].aclose()


async def hook_plugin_ctx(app: web.Application) -> AsyncIterator[None]:
    ctx = HookPluginContext(app['shared_config'].etcd, app['local_config'])
    app['hook_plugin_ctx'] = ctx
    _update_public_interface_objs(app)
    await ctx.init()
    hook_result = await ctx.dispatch(
        'ACTIVATE_MANAGER',
        (),
        return_when=ALL_COMPLETED,
    )
    if hook_result.status != PASSED:
        raise RuntimeError('Could not activate the manager instance.')
    yield
    await ctx.cleanup()


async def agent_registry_ctx(app: web.Application) -> AsyncIterator[None]:
    app['registry'] = AgentRegistry(
        app['shared_config'],
        app['dbpool'],
        app['redis_stat'],
        app['redis_live'],
        app['redis_image'],
        app['event_dispatcher'],
        app['storage_manager'],
        app['hook_plugin_ctx'],
    )
    await app['registry'].init()
    _update_public_interface_objs(app)
    yield
    await app['registry'].shutdown()


async def sched_dispatcher_ctx(app: web.Application) -> AsyncIterator[None]:
    sched_dispatcher = await SchedulerDispatcher.new(
        app['local_config'], app['shared_config'], app['event_dispatcher'], app['registry'])
    yield
    await sched_dispatcher.close()


async def monitoring_ctx(app: web.Application) -> AsyncIterator[None]:
    ectx = ErrorPluginContext(app['shared_config'].etcd, app['local_config'])
    sctx = StatsPluginContext(app['shared_config'].etcd, app['local_config'])
    await ectx.init(context={'app': app})
    await sctx.init()
    app['error_monitor'] = ectx
    app['stats_monitor'] = sctx
    _update_public_interface_objs(app)
    yield
    await sctx.cleanup()
    await ectx.cleanup()


async def background_task_ctx(app: web.Application) -> AsyncIterator[None]:
    app['background_task_manager'] = BackgroundTaskManager(app['event_dispatcher'])
    _update_public_interface_objs(app)
    yield


async def background_task_ctx_shutdown(app: web.Application) -> None:
    if 'background_task_manager' in app:
        await app['background_task_manager'].shutdown()


setattr(background_task_ctx, 'shutdown', background_task_ctx_shutdown)


def handle_loop_error(
    root_app: web.Application,
    loop: asyncio.AbstractEventLoop,
    context: Mapping[str, Any],
) -> None:
    if isinstance(loop, aiojobs.Scheduler):
        loop = current_loop()
    exception = context.get('exception')
    msg = context.get('message', '(empty message)')
    if exception is not None:
        if sys.exc_info()[0] is not None:
            log.exception('Error inside event loop: {0}', msg)
            if 'error_monitor' in root_app:
                loop.create_task(root_app['error_monitor'].capture_exception())
        else:
            exc_info = (type(exception), exception, exception.__traceback__)
            log.error('Error inside event loop: {0}', msg, exc_info=exc_info)
            if 'error_monitor' in root_app:
                loop.create_task(root_app['error_monitor'].capture_exception(exc_instance=exception))


def _init_subapp(pkg_name: str,
                 root_app: web.Application,
                 subapp: web.Application,
                 global_middlewares: Iterable[WebMiddleware]) -> None:
    subapp.on_response_prepare.append(on_prepare)

    async def _copy_public_interface_objs(subapp: web.Application):
        # Allow subapp's access to the root app properties.
        # These are the public APIs exposed to plugins as well.
        for key, obj in public_interface_objs.items():
            subapp[key] = obj

    # We must copy the public interface prior to all user-defined startup signal handlers.
    subapp.on_startup.insert(0, _copy_public_interface_objs)
    prefix = subapp.get('prefix', pkg_name.split('.')[-1].replace('_', '-'))
    aiojobs.aiohttp.setup(subapp, **root_app['scheduler_opts'])
    root_app.add_subapp('/' + prefix, subapp)
    root_app.middlewares.extend(global_middlewares)


def init_subapp(pkg_name: str, root_app: web.Application, create_subapp: AppCreator) -> None:
    subapp, global_middlewares = create_subapp(root_app['cors_opts'])
    _init_subapp(pkg_name, root_app, subapp, global_middlewares)


def _update_public_interface_objs(root_app: web.Application) -> None:
    # This must be called in clean_ctx functions so that
    # we keep the public interface up-to-date.
    for key in PUBLIC_INTERFACES:
        if key in root_app and key not in public_interface_objs:
            public_interface_objs[key] = root_app[key]


def build_root_app(
    pidx: int,
    local_config: LocalConfig, *,
    cleanup_contexts: Sequence[CleanupContext] = None,
    subapp_pkgs: Sequence[str] = None,
    scheduler_opts: Mapping[str, Any] = None,
) -> web.Application:
    public_interface_objs.clear()
    app = web.Application(middlewares=[
        exception_middleware,
        api_middleware,
    ])
    global_exception_handler = functools.partial(handle_loop_error, app)
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(global_exception_handler)
    app['local_config'] = local_config
    app['cors_opts'] = {
        '*': aiohttp_cors.ResourceOptions(
            allow_credentials=False,
            expose_headers="*", allow_headers="*"),
    }
    default_scheduler_opts = {
        'limit': 2048,
        'close_timeout': 30,
        'exception_handler': global_exception_handler,
    }
    app['scheduler_opts'] = {
        **default_scheduler_opts,
        **(scheduler_opts if scheduler_opts is not None else {}),
    }
    app['pidx'] = pidx
    _update_public_interface_objs(app)
    app.on_response_prepare.append(on_prepare)

    if cleanup_contexts is None:
        cleanup_contexts = [
            manager_status_ctx,
            redis_ctx,
            database_ctx,
            event_dispatcher_ctx,
            idle_checker_ctx,
            storage_manager_ctx,
            hook_plugin_ctx,
            monitoring_ctx,
            agent_registry_ctx,
            sched_dispatcher_ctx,
            background_task_ctx,
        ]
    for cleanup_ctx in cleanup_contexts:
        if shutdown_cb := getattr(cleanup_ctx, 'shutdown', None):
            app.on_shutdown.append(shutdown_cb)
    app.cleanup_ctx.extend(cleanup_contexts)
    aiojobs.aiohttp.setup(app, **app['scheduler_opts'])
    cors = aiohttp_cors.setup(app, defaults=app['cors_opts'])
    # should be done in create_app() in other modules.
    cors.add(app.router.add_route('GET', r'', hello))
    cors.add(app.router.add_route('GET', r'/', hello))
    if subapp_pkgs is None:
        subapp_pkgs = []
    for pkg_name in subapp_pkgs:
        if pidx == 0:
            log.info('Loading module: {0}', pkg_name[1:])
        subapp_mod = importlib.import_module(pkg_name, 'ai.backend.gateway')
        init_subapp(pkg_name, app, getattr(subapp_mod, 'create_app'))
    return app


@aiotools.actxmgr
async def server_main(loop: asyncio.AbstractEventLoop,
                      pidx: int,
                      _args: List[Any]) -> AsyncIterator[None]:
    subapp_pkgs = [
        '.etcd', '.events',
        '.auth', '.ratelimit',
        '.vfolder', '.admin',
        '.session',
        '.stream',
        '.manager',
        '.resource',
        '.scaling_group',
        '.cluster_template',
        '.session_template',
        '.image',
        '.userconfig',
        '.domainconfig',
        '.groupconfig',
        '.logs',
    ]
    app = build_root_app(pidx, _args[0], subapp_pkgs=subapp_pkgs)

    # Plugin webapps should be loaded before runner.setup(),
    # which freezes on_startup event.
    async with shared_config_ctx(app), \
               webapp_plugin_ctx(app):
        ssl_ctx = None
        if app['local_config']['manager']['ssl-enabled']:
            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(
                str(app['local_config']['manager']['ssl-cert']),
                str(app['local_config']['manager']['ssl-privkey']),
            )
        runner = web.AppRunner(app)
        await runner.setup()
        service_addr = app['local_config']['manager']['service-addr']
        site = web.TCPSite(
            runner,
            str(service_addr.host),
            service_addr.port,
            backlog=1024,
            reuse_port=True,
            ssl_context=ssl_ctx,
        )
        await site.start()

        if os.geteuid() == 0:
            uid = app['local_config']['manager']['user']
            gid = app['local_config']['manager']['group']
            os.setgroups([
                g.gr_gid for g in grp.getgrall()
                if pwd.getpwuid(uid).pw_name in g.gr_mem
            ])
            os.setgid(gid)
            os.setuid(uid)
            log.info('changed process uid and gid to {}:{}', uid, gid)
        log.info('started handling API requests at {}', service_addr)

        try:
            yield
        finally:
            log.info('shutting down...')
            await runner.cleanup()


@aiotools.actxmgr
async def server_main_logwrapper(loop: asyncio.AbstractEventLoop,
                                 pidx: int, _args: List[Any]) -> AsyncIterator[None]:
    setproctitle(f"backend.ai: manager worker-{pidx}")
    log_endpoint = _args[1]
    logger = Logger(_args[0]['logging'], is_master=False, log_endpoint=log_endpoint)
    try:
        with logger:
            async with server_main(loop, pidx, _args):
                yield
    except Exception:
        traceback.print_exc()


@click.group(invoke_without_command=True)
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./manager.toml and /etc/backend.ai/manager.toml)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(ctx: click.Context, config_path: Path, debug: bool) -> None:

    cfg = load_config(config_path, debug)

    if ctx.invoked_subcommand is None:
        cfg['manager']['pid-file'].write_text(str(os.getpid()))
        log_sockpath = Path(f'/tmp/backend.ai/ipc/manager-logger-{os.getpid()}.sock')
        log_sockpath.parent.mkdir(parents=True, exist_ok=True)
        log_endpoint = f'ipc://{log_sockpath}'
        try:
            logger = Logger(cfg['logging'], is_master=True, log_endpoint=log_endpoint)
            with logger:
                ns = cfg['etcd']['namespace']
                setproctitle(f"backend.ai: manager {ns}")
                log.info('Backend.AI Gateway {0}', __version__)
                log.info('runtime: {0}', env_info())
                log_config = logging.getLogger('ai.backend.gateway.config')
                log_config.debug('debug mode enabled.')

                if cfg['manager']['event-loop'] == 'uvloop':
                    import uvloop
                    uvloop.install()
                    log.info('Using uvloop as the event loop backend')
                try:
                    aiotools.start_server(
                        server_main_logwrapper,
                        num_workers=cfg['manager']['num-proc'],
                        args=(cfg, log_endpoint),
                    )
                finally:
                    log.info('terminated.')
        finally:
            if cfg['manager']['pid-file'].is_file():
                # check is_file() to prevent deleting /dev/null!
                cfg['manager']['pid-file'].unlink()
    else:
        # Click is going to invoke a subcommand.
        pass


@main.group(cls=LazyGroup, import_name='ai.backend.gateway.auth:cli')
def auth() -> None:
    pass


if __name__ == '__main__':
    sys.exit(main())
