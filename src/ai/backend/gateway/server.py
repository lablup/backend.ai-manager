'''
The main web / websocket server
'''

import asyncio
import functools
import importlib
from ipaddress import ip_address
import logging
import os
import ssl
import sys
import traceback

import aiohttp
from aiohttp import web
import aiohttp_cors
import aiojobs.aiohttp
import aioredis
import aiotools
from aiopg.sa import create_engine
from setproctitle import setproctitle
import uvloop

from ai.backend.common.argparse import (
    ipaddr, path, port_no,
)
from ai.backend.common.utils import env_info
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.plugin import (
    discover_entrypoints, install_plugins, add_plugin_args)
from ..manager import __version__
from ..manager.registry import AgentRegistry
from .defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB
from .etcd import ConfigServer
from .events import EventDispatcher, event_subscriber
from .exceptions import (BackendError, MethodNotAllowed, GenericNotFound,
                         GenericBadRequest, InternalServerError)
from .config import load_config
from .events import event_router
from . import ManagerStatus

VALID_VERSIONS = frozenset([
    'v1.20160915',  # deprecated
    'v2.20170315',  # deprecated
    'v3.20170615',
    'v4.20181215',  # authentication changed not to use request bodies
    'v4.20190115',  # added & enabled streaming-execute API
    'v4.20190315',  # resource/image changes
])
LATEST_REV_DATES = {
    1: '20160915',
    2: '20170915',
    3: '20181215',
    4: '20190315',
}
LATEST_API_VERSION = 'v4.20190315'

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
]


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({'version': LATEST_API_VERSION})


async def on_prepare(request, response):
    response.headers['Server'] = 'BackendAI-API/' + LATEST_API_VERSION


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
            raise MethodNotAllowed
        log.warning('Bad request: {0!r}', ex)
        raise GenericBadRequest
    except asyncio.CancelledError as e:
        # The server is closing or the client has disconnected in the middle of
        # request.  Atomic requests are still executed to their ends.
        log.warning('Request cancelled ({0} {1})', request.method, request.rel_url)
        raise e
    except Exception as e:
        app['error_monitor'].capture_exception()
        log.exception('Uncaught exception in HTTP request handlers {0!r}', e)
        if app['config'].debug:
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

    # legacy redirects
    cors.add(app.router.add_route('GET', r'/v{version:\d+}/authorize',
                                  legacy_auth_test_redirect))

    # populate public interfaces
    app['config_server'] = ConfigServer(
        app, app['config'].etcd_addr,
        app['config'].etcd_user, app['config'].etcd_password,
        app['config'].namespace)
    if app['pidx'] == 0:
        await app['config_server'].update_manager_status(ManagerStatus.PREPARING)
    app['dbpool'] = await create_engine(
        host=app['config'].db_addr[0], port=app['config'].db_addr[1],
        user=app['config'].db_user, password=app['config'].db_password,
        dbname=app['config'].db_name,
        echo=bool(app['config'].verbose),
        # TODO: check the throughput impacts of DB/redis pool sizes
        minsize=4, maxsize=256,
        timeout=30, pool_recycle=30,
    )
    app['redis_live'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
        password=(app['config'].redis_password
                  if app['config'].redis_password else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_LIVE_DB)
    app['redis_stat'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
        password=(app['config'].redis_password
                  if app['config'].redis_password else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_STAT_DB)
    app['redis_image'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
        password=(app['config'].redis_password
                  if app['config'].redis_password else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB)

    loop = asyncio.get_event_loop()
    dispatcher = EventDispatcher(app)
    app['event_dispatcher'] = dispatcher
    app['event_subscriber'] = loop.create_task(event_subscriber(dispatcher))

    app['registry'] = AgentRegistry(
        app['config_server'],
        app['dbpool'],
        app['redis_stat'],
        app['redis_live'],
        app['redis_image'])
    await app['registry'].init()

    # Detect and install monitoring plugins.
    app['stats_monitor'] = DummyStatsMonitor()
    app['error_monitor'] = DummyErrorMonitor()
    app['stats_monitor.enabled'] = False
    app['error_monitor.enabled'] = False

    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    install_plugins(plugins, app, 'dict', app['config'])


async def gw_shutdown(app):
    app['event_subscriber'].cancel()
    await app['event_subscriber']


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
    app['config'].app_name = 'backend.ai-manager'
    if app['config'].disable_plugins:
        app['config'].disable_plugins = app['config'].disable_plugins.split(',')
    else:
        app['config'].disable_plugins = []
    app['sslctx'] = None
    if app['config'].ssl_cert and app['config'].ssl_key:
        app['sslctx'] = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        app['sslctx'].load_cert_chain(str(app['config'].ssl_cert),
                                      str(app['config'].ssl_key))
    if app['config'].service_port == 0:
        app['config'].service_port = 8443 if app['sslctx'] else 8080
    app['pidx'] = pidx

    subapp_pkgs = [
        '.etcd', '.events',
        '.auth', '.ratelimit',
        '.vfolder', '.admin',
        '.kernel', '.stream',
        '.manager',
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

    def init_subapp(create_subapp):
        subapp, global_middlewares = create_subapp(cors_options)
        assert isinstance(subapp, web.Application)
        subapp.on_response_prepare.append(on_prepare)
        # Allow subapp's access to the root app properties.
        # These are the public APIs exposed to extensions as well.
        for key in PUBLIC_INTERFACES:
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

    for pkgname in subapp_pkgs:
        if pidx == 0:
            log.info('Loading module: {0}', pkgname[1:])
        subapp_mod = importlib.import_module(pkgname, 'ai.backend.gateway')
        init_subapp(getattr(subapp_mod, 'create_app'))

    plugins = [
        'webapp',
    ]
    for plugin_info in discover_entrypoints(
            plugins, disable_plugins=app['config'].disable_plugins):
        plugin_group, plugin_name, entrypoint = plugin_info
        if pidx == 0:
            log.info('Loading app plugin: {0}', entrypoint.module_name)
        plugin = entrypoint.load()
        init_subapp(getattr(plugin, 'create_app'))

    app.on_shutdown.append(gw_shutdown)
    app.on_cleanup.append(gw_cleanup)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(
        runner,
        str(app['config'].service_ip),
        app['config'].service_port,
        backlog=1024,
        reuse_port=True,
        ssl_context=app['sslctx'],
    )
    await site.start()
    log.info('started.')

    try:
        yield
    finally:
        log.info('shutting down...')
        await runner.cleanup()


def gw_args(parser):
    parser.add('-n', '--num-proc', env_var='BACKEND_GATEWAY_NPROC',
               type=int, default=min(os.cpu_count(), 4),
               help='The number of worker processes to handle API requests.')
    parser.add('--heartbeat-timeout', env_var='BACKEND_HEARTBEAT_TIMEOUT',
               type=float, default=5.0,
               help='The timeout for agent heartbeats.')
    parser.add('--advertised-manager-host',
               env_var='BACKEND_ADVERTISED_MANAGER_HOST',
               type=str, default=None,
               help='Manually set the manager hostname or IP address advertised '
                    'to the agents in this cluster via etcd.  '
                    'If not set, the manager tries the followings in order: '
                    '1) get the private IP address from the instance metadata in '
                    'supported cloud environments, '
                    '2) resolve the current hostname, or '
                    '3) return "127.0.0.1".')
    parser.add('--events-port', env_var='BACKEND_EVENTS_PORT',
               type=port_no, default=5002,
               help='The TCP port number where the event server listens on.')
    parser.add('--service-ip', env_var='BACKEND_SERVICE_IP',
               type=ipaddr, default=ip_address('0.0.0.0'),
               help='The IP where the API gateway server listens on.')
    parser.add('--service-port', env_var='BACKEND_SERVICE_PORT',
               type=port_no, default=0,
               help='The TCP port number where the API gateway server listens on. '
                    '(if unpsecified, it becomes 8080 and 8443 when SSL is enabled) '
                    'To run in production, you need the root privilege '
                    'to use the standard 80/443 ports or use a reverse-proxy '
                    'such as nginx.')
    parser.add('--ssl-cert', env_var='BACKEND_SSL_CERT',
               type=path, default=None,
               help='The path to an SSL certificate file. '
                    'It may contain inter/root CA certificates as well.')
    parser.add('--ssl-key', env_var='BACKEND_SSL_KEY',
               type=path, default=None,
               help='The path to the private key used to make requests '
                    'for the SSL certificate.')

    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    add_plugin_args(parser, plugins)


def main():

    config = load_config(extra_args_funcs=(gw_args, Logger.update_log_args))
    setproctitle(f'backend.ai: manager {config.namespace} '
                 f'{config.service_ip}:{config.service_port}')
    logger = Logger(config)
    logger.add_pkg('aiotools')
    logger.add_pkg('aiopg')
    logger.add_pkg('ai.backend')
    with logger:
        log.info('Backend.AI Gateway {0}', __version__)
        log.info('runtime: {0}', env_info())
        log_config = logging.getLogger('ai.backend.gateway.config')
        log_config.debug('debug mode enabled.')
        if config.debug:
            aiohttp.log.server_logger.setLevel('DEBUG')
            aiohttp.log.access_logger.setLevel('DEBUG')
        else:
            aiohttp.log.server_logger.setLevel('WARNING')
            aiohttp.log.access_logger.setLevel('WARNING')

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        try:
            aiotools.start_server(server_main, num_workers=config.num_proc,
                                  extra_procs=[event_router],
                                  args=(config,))
        finally:
            log.info('terminated.')


if __name__ == '__main__':
    main()
