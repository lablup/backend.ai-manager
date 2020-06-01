'''
The main web / websocket server
'''

import asyncio
from datetime import datetime
import functools
import importlib
import logging
import os
import pwd, grp
import ssl
import sys
import traceback

from aiohttp import web
import aiohttp_cors
import aiojobs.aiohttp
import aiotools
from aiopg.sa import create_engine
import click
from pathlib import Path
from setproctitle import setproctitle
import uvloop

from ai.backend.common import redis
from ai.backend.common.cli import LazyGroup
from ai.backend.common.utils import env_info
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.plugin import (
    discover_entrypoints, install_plugins, add_plugin_args)
from ..manager import __version__
from ..manager.registry import AgentRegistry
from ..manager.scheduler import SessionScheduler
from .config import load as load_config, load_shared as load_shared_config, redis_config_iv
from .defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB
from .etcd import ConfigServer
from .events import EventDispatcher
from .exceptions import (BackendError, MethodNotAllowed, GenericNotFound,
                         GenericBadRequest, InternalServerError)
from . import ManagerStatus

VALID_VERSIONS = frozenset([
    'v1.20160915',  # deprecated
    'v2.20170315',  # deprecated
    'v3.20170615',

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
])
LATEST_REV_DATES = {
    1: '20160915',
    2: '20170915',
    3: '20181215',
    4: '20190615',
}
LATEST_API_VERSION = 'v4.20190615'

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))

PUBLIC_INTERFACES = [
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


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({
        'version': LATEST_API_VERSION,
        'manager': __version__,
    })


async def on_prepare(request, response):
    response.headers['Server'] = 'BackendAI'


@web.middleware
async def api_middleware(request, handler):
    _handler = handler
    method_override = request.headers.get('X-Method-Override', None)
    if method_override:
        request = request.clone(method=method_override)
        new_match_info = await request.app.router.resolve(request)
        _handler = new_match_info.handler
        request._match_info = new_match_info
    ex = request.match_info.http_exception
    if ex is not None:
        # handled by exception_middleware
        raise ex
    new_api_version = request.headers.get('X-BackendAI-Version')
    legacy_api_version = request.headers.get('X-Sorna-Version')
    api_version = new_api_version or legacy_api_version
    try:
        if api_version is None:
            major_version = int(request.match_info.get('version', 4))
            revision_date = LATEST_REV_DATES[major_version]
            request['api_version'] = (major_version, revision_date)
        else:
            assert api_version in VALID_VERSIONS
            major_version, revision_date = api_version.split('.', maxsplit=1)
            request['api_version'] = (int(major_version[1:]), revision_date)
    except (AssertionError, ValueError, KeyError):
        raise GenericBadRequest('Unsupported API major version.')
    resp = (await _handler(request))
    return resp


@web.middleware
async def exception_middleware(request, handler):
    app = request.app
    try:
        app['stats_monitor'].report_stats(
            'increment', 'ai.backend.gateway.api.requests')
        resp = (await handler(request))
    except BackendError as ex:
        if ex.status_code == 500:
            log.exception('Internal server error raised inside handlers')
            raise
        app['error_monitor'].capture_exception()
        app['stats_monitor'].report_stats(
            'increment', 'ai.backend.gateway.api.failures')
        app['stats_monitor'].report_stats(
            'increment', f'ai.backend.gateway.api.status.{ex.status_code}')
        raise
    except web.HTTPException as ex:
        app['stats_monitor'].report_stats(
            'increment', 'ai.backend.gateway.api.failures')
        app['stats_monitor'].report_stats(
            'increment', f'ai.backend.gateway.api.status.{ex.status_code}')
        if ex.status_code == 404:
            raise GenericNotFound
        if ex.status_code == 405:
            raise MethodNotAllowed(ex.method, ex.allowed_methods)
        log.warning('Bad request: {0!r}', ex)
        raise GenericBadRequest
    except asyncio.CancelledError as e:
        # The server is closing or the client has disconnected in the middle of
        # request.  Atomic requests are still executed to their ends.
        log.debug('Request cancelled ({0} {1})', request.method, request.rel_url)
        raise e
    except Exception as e:
        app['error_monitor'].capture_exception()
        log.exception('Uncaught exception in HTTP request handlers {0!r}', e)
        if app['config']['debug']['enabled']:
            raise InternalServerError(traceback.format_exc())
        else:
            raise InternalServerError()
    else:
        app['stats_monitor'].report_stats(
            'increment', f'ai.backend.gateway.api.status.{resp.status}')
        return resp


async def legacy_auth_test_redirect(request):
    raise web.HTTPFound('/v3/auth/test')


async def gw_init(app, default_cors_options):
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    # should be done in create_app() in other modules.
    cors.add(app.router.add_route('GET', r'', hello))
    cors.add(app.router.add_route('GET', r'/', hello))

    # legacy redirects
    cors.add(app.router.add_route('GET', r'/v{version:\d+}/authorize',
                                  legacy_auth_test_redirect))

    # populate public interfaces
    app['config_server'] = ConfigServer(
        app, app['config']['etcd']['addr'],
        app['config']['etcd']['user'], app['config']['etcd']['password'],
        app['config']['etcd']['namespace'])

    shared_config = await load_shared_config(app['config_server'].etcd)
    app['config'].update(shared_config)

    if app['pidx'] == 0:
        mgr_status = await app['config_server'].get_manager_status()
        if mgr_status is None or mgr_status not in (ManagerStatus.RUNNING, ManagerStatus.FROZEN):
            # legacy transition: we now have only RUNNING or FROZEN for HA setup.
            await app['config_server'].update_manager_status(ManagerStatus.RUNNING)
            mgr_status = ManagerStatus.RUNNING
        log.info('Manager status: {}', mgr_status)
        tz = app['config']['system']['timezone']
        log.info('Configured timezone: {}', tz.tzname(datetime.now()))

    app['dbpool'] = await create_engine(
        host=app['config']['db']['addr'].host, port=app['config']['db']['addr'].port,
        user=app['config']['db']['user'], password=app['config']['db']['password'],
        dbname=app['config']['db']['name'],
        echo=bool(app['config']['logging']['level'] == 'DEBUG'),
        minsize=8, maxsize=256,
        timeout=60, pool_recycle=120,
    )

    redis_config = await app['config_server'].etcd.get_prefix('config/redis')
    app['config']['redis'] = redis_config_iv.check(redis_config)

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

    app['event_dispatcher'] = await EventDispatcher.new(app)

    app['registry'] = AgentRegistry(
        app['config_server'],
        app['dbpool'],
        app['redis_stat'],
        app['redis_live'],
        app['redis_image'],
        app['event_dispatcher'])
    await app['registry'].init()

    # Detect and install monitoring plugins.
    app['stats_monitor'] = DummyStatsMonitor()
    app['error_monitor'] = DummyErrorMonitor()
    app['stats_monitor.enabled'] = False
    app['error_monitor.enabled'] = False

    # Install stats hook plugins.
    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    install_plugins(plugins, app, 'dict', app['config'])

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


async def gw_shutdown(app):
    await app['event_dispatcher'].close()


async def gw_cleanup(app):
    await app['registry'].shutdown()
    app['redis_image'].close()
    await app['redis_image'].wait_closed()
    app['redis_stat'].close()
    await app['redis_stat'].wait_closed()
    app['redis_live'].close()
    await app['redis_live'].wait_closed()
    app['dbpool'].close()
    await app['dbpool'].wait_closed()


def handle_loop_error(app, loop, context):
    exception = context.get('exception')
    msg = context.get('message', '(empty message)')
    if exception is not None:
        app['error_monitor'].set_context(context)
        if sys.exc_info()[0] is not None:
            log.exception('Error inside event loop: {0}', msg)
            app['error_monitor'].capture_exception(True)
        else:
            exc_info = (type(exception), exception, exception.__traceback__)
            log.error('Error inside event loop: {0}', msg, exc_info=exc_info)
            app['error_monitor'].capture_exception(exc_info)


def _get_legacy_handler(handler, app, major_api_version):

    @functools.wraps(handler)
    async def _wrapped_handler(request):
        request['api_version'] = (
            major_api_version,
            LATEST_REV_DATES[major_api_version],
        )
        # This is a hack to support legacy routes without altering aiohttp core.
        m = web.UrlMappingMatchInfo(request._match_info,
                                    request._match_info.route)
        m['version'] = major_api_version
        m.add_app(app)
        m.freeze()
        request._match_info = m
        return await handler(request)

    return _wrapped_handler


@aiotools.actxmgr
async def server_main(loop, pidx, _args):

    app = web.Application(middlewares=[
        exception_middleware,
        api_middleware,
    ])
    cors_options = {
        '*': aiohttp_cors.ResourceOptions(
            allow_credentials=False,
            expose_headers="*", allow_headers="*"),
    }
    app.on_response_prepare.append(on_prepare)
    app['config'] = _args[0]
    ssl_ctx = None
    if app['config']['manager']['ssl-enabled']:
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            str(app['config']['manager']['ssl-cert']),
            str(app['config']['manager']['ssl-privkey']),
        )
    app['pidx'] = pidx

    subapp_pkgs = [
        '.etcd', '.events',
        '.auth', '.ratelimit',
        '.vfolder', '.admin',
        '.kernel',
        '.stream',
        '.manager',
        '.resource',
        '.scaling_group',
        '.image',
        '.userconfig',
    ]

    global_exception_handler = functools.partial(handle_loop_error, app)
    scheduler_opts = {
        'limit': 2048,
        'close_timeout': 30,
        'exception_handler': global_exception_handler,
    }
    loop.set_exception_handler(global_exception_handler)
    aiojobs.aiohttp.setup(app, **scheduler_opts)
    await gw_init(app, cors_options)

    def _init_subapp(subapp, global_middlewares):
        assert isinstance(subapp, web.Application)
        subapp.on_response_prepare.append(on_prepare)
        # Allow subapp's access to the root app properties.
        # These are the public APIs exposed to extensions as well.
        for key in PUBLIC_INTERFACES:
            if key in app:
                subapp[key] = app[key]
        prefix = subapp.get('prefix', pkgname.split('.')[-1].replace('_', '-'))
        aiojobs.aiohttp.setup(subapp, **scheduler_opts)
        app.add_subapp('/' + prefix, subapp)
        app.middlewares.extend(global_middlewares)

        # Add legacy version-prefixed routes to the root app with some hacks
        # (NOTE: they do not support CORS!)
        for r in subapp.router.routes():
            for version in subapp['api_versions']:
                subpath = r.resource.canonical
                if subpath == f'/{prefix}':
                    subpath += '/'
                legacy_path = f'/v{version}{subpath}'
                handler = _get_legacy_handler(r.handler, subapp, version)
                app.router.add_route(r.method, legacy_path, handler)

    def init_subapp(create_subapp):
        subapp, global_middlewares = create_subapp(cors_options)
        _init_subapp(subapp, global_middlewares)

    def init_extapp(create_subapp):
        subapp, global_middlewares = create_subapp(app['config']['plugins'], cors_options)
        _init_subapp(subapp, global_middlewares)

    for pkgname in subapp_pkgs:
        if pidx == 0:
            log.info('Loading module: {0}', pkgname[1:])
        subapp_mod = importlib.import_module(pkgname, 'ai.backend.gateway')
        init_subapp(getattr(subapp_mod, 'create_app'))

    plugins = [
        'hanati_webapp',
        'cloud_beta_webapp',
    ]
    for plugin_info in discover_entrypoints(
            plugins, disable_plugins=app['config']['manager']['disabled-plugins']):
        plugin_group, plugin_name, entrypoint = plugin_info
        if pidx == 0:
            log.info('Loading app plugin: {0}', entrypoint.module_name)
        plugin = entrypoint.load()
        init_extapp(getattr(plugin, 'create_app'))

    app.on_shutdown.append(gw_shutdown)
    app.on_cleanup.append(gw_cleanup)

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
    session_scheduler = await SessionScheduler.new(
        app['config'], app['config_server'], app['registry'])

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
        await session_scheduler.close()
        await runner.cleanup()


def gw_args(parser):

    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    add_plugin_args(parser, plugins)


@click.group(invoke_without_command=True)
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./manager.toml and /etc/backend.ai/manager.toml)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(ctx, config_path, debug):

    cfg = load_config(config_path, debug)

    if ctx.invoked_subcommand is None:
        cfg['manager']['pid-file'].write_text(str(os.getpid()))
        try:
            logger = Logger(cfg['logging'])
            with logger:
                ns = cfg['etcd']['namespace']
                setproctitle(f"backend.ai: manager {ns}")
                log.info('Backend.AI Gateway {0}', __version__)
                log.info('runtime: {0}', env_info())
                log_config = logging.getLogger('ai.backend.gateway.config')
                log_config.debug('debug mode enabled.')

                if cfg['manager']['event-loop'] == 'uvloop':
                    uvloop.install()
                    log.info('Using uvloop as the event loop backend')
                try:
                    aiotools.start_server(server_main,
                                          num_workers=cfg['manager']['num-proc'],
                                          args=(cfg,))
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
def auth():
    pass


if __name__ == '__main__':
    sys.exit(main())
