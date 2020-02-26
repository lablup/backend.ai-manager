'''
The main web / websocket server
'''

import asyncio
from datetime import datetime
import functools
import importlib
import logging
import multiprocessing
import os
import pwd, grp
import ssl
import sys
import traceback
from typing import (
    cast,
    Any, Final,
    AsyncIterator,
    Iterable, List, Sequence,
    Mapping, MutableMapping,
)

from aiohttp import web
import aiohttp_cors
import aiojobs.aiohttp
import aiotools
from aiopg.sa import create_engine
import click
from pathlib import Path
from setproctitle import setproctitle

from ai.backend.common import redis
from ai.backend.common.cli import LazyGroup
from ai.backend.common.utils import env_info
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.plugin import (
    discover_entrypoints, install_plugins
)
from ..manager import __version__
from ..manager.registry import AgentRegistry
from ..manager.scheduler.dispatcher import SchedulerDispatcher
from .config import load as load_config, load_shared as load_shared_config, redis_config_iv
from .defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB
from .etcd import ConfigServer
from .events import EventDispatcher
from .exceptions import (BackendError, MethodNotAllowed, GenericNotFound,
                         GenericBadRequest, InternalServerError)
from .typing import (
    AppCreator, PluginAppCreator,
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
    'v5.20191215'
])
LATEST_REV_DATES: Final = {
    1: '20160915',
    2: '20170915',
    3: '20181215',
    4: '20190615',
    5: '20191215',
}
LATEST_API_VERSION: Final = 'v5.20191215'

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))

PUBLIC_INTERFACES: Final = [
    'pidx',
    'config',
    'config_server',
    'dbpool',
    'registry',
    'redis_live',
    'redis_stat',
    'redis_image',
    'event_dispatcher',
    'stats_monitor',
    'error_monitor',
    'plugins',
]

public_interface_objs: MutableMapping[str, Any] = {}


async def hello(request: web.Request) -> web.Response:
    '''
    Returns the API version number.
    '''
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
        else:
            assert api_version in VALID_VERSIONS
            hdr_major_version, revision_date = api_version.split('.', maxsplit=1)
            request['api_version'] = (int(hdr_major_version[1:]), revision_date)
    except (AssertionError, ValueError, KeyError):
        raise GenericBadRequest('Unsupported API major version.')
    resp = (await _handler(request))
    return resp


@web.middleware
async def exception_middleware(request: web.Request,
                               handler: WebRequestHandler) -> web.StreamResponse:
    app = request.app
    try:
        if (stats_monitor := app.get('stats_monitor', None)) is not None:
            stats_monitor.report_stats('increment', 'ai.backend.gateway.api.requests')
        resp = (await handler(request))
    except BackendError as ex:
        if ex.status_code == 500:
            log.exception('Internal server error raised inside handlers')
            raise
        if (error_monitor := app.get('error_monitor', None)) is not None:
            error_monitor.capture_exception()
        if (stats_monitor := app.get('stats_monitor', None)) is not None:
            stats_monitor.report_stats('increment', 'ai.backend.gateway.api.failures')
            stats_monitor.report_stats('increment', f'ai.backend.gateway.api.status.{ex.status_code}')
        raise
    except web.HTTPException as ex:
        if (stats_monitor := app.get('stats_monitor', None)) is not None:
            stats_monitor.report_stats('increment', 'ai.backend.gateway.api.failures')
            stats_monitor.report_stats('increment', f'ai.backend.gateway.api.status.{ex.status_code}')
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
        if (error_monitor := app.get('error_monitor', None)) is not None:
            error_monitor.capture_exception()
        log.exception('Uncaught exception in HTTP request handlers {0!r}', e)
        if app['config']['debug']['enabled']:
            raise InternalServerError(traceback.format_exc())
        else:
            raise InternalServerError()
    else:
        if (stats_monitor := app.get('stats_monitor', None)) is not None:
            stats_monitor.report_stats('increment', f'ai.backend.gateway.api.status.{resp.status}')
        return resp


async def config_server_ctx(app: web.Application) -> AsyncIterator[None]:
    # populate public interfaces
    app['config_server'] = ConfigServer(
        app, app['config']['etcd']['addr'],
        app['config']['etcd']['user'], app['config']['etcd']['password'],
        app['config']['etcd']['namespace'])

    shared_config = await load_shared_config(app['config_server'].etcd)
    app['config'].update(shared_config)
    redis_config = await app['config_server'].etcd.get_prefix('config/redis')
    app['config']['redis'] = redis_config_iv.check(redis_config)
    _update_public_interface_objs(app)
    yield
    # await app['config_server'].close()


async def manager_status_ctx(app: web.Application) -> AsyncIterator[None]:
    if app['pidx'] == 0:
        mgr_status = await app['config_server'].get_manager_status()
        if mgr_status is None or mgr_status not in (ManagerStatus.RUNNING, ManagerStatus.FROZEN):
            # legacy transition: we now have only RUNNING or FROZEN for HA setup.
            await app['config_server'].update_manager_status(ManagerStatus.RUNNING)
            mgr_status = ManagerStatus.RUNNING
        log.info('Manager status: {}', mgr_status)
        tz = app['config']['system']['timezone']
        log.info('Configured timezone: {}', tz.tzname(datetime.now()))
    yield


async def redis_ctx(app: web.Application) -> AsyncIterator[None]:
    app['redis_live'] = await redis.connect_with_retries(
        app['config']['redis']['addr'].as_sockaddr(),
        password=(app['config']['redis']['password']
                  if app['config']['redis']['password'] else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_LIVE_DB)
    app['redis_stat'] = await redis.connect_with_retries(
        app['config']['redis']['addr'].as_sockaddr(),
        password=(app['config']['redis']['password']
                  if app['config']['redis']['password'] else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_STAT_DB)
    app['redis_image'] = await redis.connect_with_retries(
        app['config']['redis']['addr'].as_sockaddr(),
        password=(app['config']['redis']['password']
                  if app['config']['redis']['password'] else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB)
    _update_public_interface_objs(app)
    yield
    app['redis_image'].close()
    await app['redis_image'].wait_closed()
    app['redis_stat'].close()
    await app['redis_stat'].wait_closed()
    app['redis_live'].close()
    await app['redis_live'].wait_closed()


async def database_ctx(app: web.Application) -> AsyncIterator[None]:
    app['dbpool'] = await create_engine(
        host=app['config']['db']['addr'].host, port=app['config']['db']['addr'].port,
        user=app['config']['db']['user'], password=app['config']['db']['password'],
        dbname=app['config']['db']['name'],
        echo=bool(app['config']['logging']['level'] == 'DEBUG'),
        minsize=8, maxsize=256,
        timeout=60, pool_recycle=120,
    )
    _update_public_interface_objs(app)
    yield
    app['dbpool'].close()
    await app['dbpool'].wait_closed()


async def event_dispatcher_ctx(app: web.Application) -> AsyncIterator[None]:
    app['event_dispatcher'] = await EventDispatcher.new(app)
    _update_public_interface_objs(app)
    yield
    await app['event_dispatcher'].close()


async def agent_registry_ctx(app: web.Application) -> AsyncIterator[None]:
    app['registry'] = AgentRegistry(
        app['config_server'],
        app['dbpool'],
        app['redis_stat'],
        app['redis_live'],
        app['redis_image'],
        app['event_dispatcher'])
    await app['registry'].init()
    _update_public_interface_objs(app)
    yield
    await app['registry'].shutdown()


async def sched_dispatcher_ctx(app: web.Application) -> AsyncIterator[None]:
    sched_dispatcher = await SchedulerDispatcher.new(
        app['config'], app['config_server'], app['registry'], app['pidx'])
    yield
    await sched_dispatcher.close()


async def monitoring_ctx(app: web.Application) -> AsyncIterator[None]:
    # Install stats hook plugins.
    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    install_plugins(plugins, app, 'dict', app['config'])
    _update_public_interface_objs(app)
    yield


async def hook_plugins_ctx(app: web.Application) -> AsyncIterator[None]:
    # Install other hook plugins inside app['plugins'].
    plugins = [
        'hanati_hook',
        'cloud_beta_hook',
    ]
    app['plugins'] = {}
    install_plugins(plugins, app['plugins'], 'dict', app['config'])
    for plugin_name, plugin_registry in app['plugins'].items():
        if app['pidx'] == 0:
            log.info('Loading hook plugin: {0}', plugin_name)
        await plugin_registry.init()
    _update_public_interface_objs(app)
    yield


async def webapp_plugins_ctx(app: web.Application) -> AsyncIterator[None]:

    def init_extapp(pkg_name: str, root_app: web.Application, create_subapp: PluginAppCreator) -> None:
        subapp, global_middlewares = create_subapp(app['config']['plugins'], app['cors_opts'])
        _init_subapp(pkg_name, root_app, subapp, global_middlewares)

    plugins = [
        'hanati_webapp',
    ]
    for plugin_info in discover_entrypoints(
        plugins, disable_plugins=app['config']['manager']['disabled-plugins']
    ):
        plugin_group, plugin_name, entrypoint = plugin_info
        if app['pidx'] == 0:
            log.info('Loading app plugin: {0}', entrypoint.module_name)
        plugin = entrypoint.load()
        init_extapp(entrypoint.module_name, app, getattr(plugin, 'create_app'))
    yield


def handle_loop_error(
    app: web.Application,
    loop: asyncio.AbstractEventLoop,
    context: Mapping[str, Any],
) -> None:
    exception = context.get('exception')
    msg = context.get('message', '(empty message)')
    if exception is not None:
        if (error_monitor := app.get('error_monitor', None)) is not None:
            error_monitor.set_context(context)
        if sys.exc_info()[0] is not None:
            log.exception('Error inside event loop: {0}', msg)
            if error_monitor is not None:
                error_monitor.capture_exception(True)
        else:
            exc_info = (type(exception), exception, exception.__traceback__)
            log.error('Error inside event loop: {0}', msg, exc_info=exc_info)
            if error_monitor is not None:
                error_monitor.capture_exception(exc_info)


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


def build_root_app(pidx: int,
                   config: Mapping[str, Any], *,
                   cleanup_contexts: Sequence[CleanupContext] = None,
                   subapp_pkgs: Sequence[str] = None,
                   scheduler_opts: Mapping[str, Any] = None) -> web.Application:
    public_interface_objs.clear()
    app = web.Application(middlewares=[
        exception_middleware,
        api_middleware,
    ])
    global_exception_handler = functools.partial(handle_loop_error, app)
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(global_exception_handler)
    app['config'] = config
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
        app.cleanup_ctx.append(config_server_ctx)
        app.cleanup_ctx.append(monitoring_ctx)
        app.cleanup_ctx.append(manager_status_ctx)
        app.cleanup_ctx.append(redis_ctx)
        app.cleanup_ctx.append(database_ctx)
        app.cleanup_ctx.append(event_dispatcher_ctx)
        app.cleanup_ctx.append(agent_registry_ctx)
        app.cleanup_ctx.append(sched_dispatcher_ctx)
        app.cleanup_ctx.append(webapp_plugins_ctx)
        app.cleanup_ctx.append(hook_plugins_ctx)
    else:
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
        '.session_template',
        '.image',
        '.userconfig',
    ]
    app = build_root_app(pidx, _args[0], subapp_pkgs=subapp_pkgs)

    ssl_ctx = None
    if app['config']['manager']['ssl-enabled']:
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            str(app['config']['manager']['ssl-cert']),
            str(app['config']['manager']['ssl-privkey']),
        )
    runner = web.AppRunner(app)
    await runner.setup()
    service_addr = app['config']['manager']['service-addr']
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
        uid = app['config']['manager']['user']
        gid = app['config']['manager']['group']
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
    multiprocessing.set_start_method('spawn')

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
                    aiotools.start_server(server_main_logwrapper,
                                          num_workers=cfg['manager']['num-proc'],
                                          args=(cfg, log_endpoint))
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
