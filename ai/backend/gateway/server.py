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

import aiohttp
from aiohttp import web
import aiojobs.aiohttp
import aioredis
import aiotools
from aiopg.sa import create_engine
import uvloop
try:
    import datadog
    datadog_available = True
except ImportError:
    datadog_available = False
try:
    import raven
    raven_available = True
except ImportError:
    raven_available = False

from ai.backend.common.argparse import (
    ipaddr, path, port_no,
)
from ai.backend.common.utils import env_info
from ai.backend.common.monitor import DummyDatadog, DummySentry
from ai.backend.common.logging import Logger
from ..manager import __version__
from ..manager.registry import AgentRegistry
from . import GatewayStatus
from .defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB
from .etcd import ConfigServer
from .events import EventDispatcher, event_subscriber
from .exceptions import (BackendError, MethodNotAllowed, GenericNotFound,
                         GenericBadRequest, InternalServerError)
from .config import load_config
from .events import event_router

VALID_VERSIONS = frozenset([
    'v1.20160915',
    'v2.20170315',
    'v3.20170615',
])
LATEST_API_VERSION = 'v3.20170615'

log = logging.getLogger('ai.backend.gateway.server')

PUBLIC_INTERFACES = [
    'pidx',
    'status',
    'config',
    'config_server',
    'dbpool',
    'registry',
    'redis_live',
    'redis_stat',
    'redis_image',
    'event_dispatcher',
    'datadog',
    'sentry',
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
    version = int(request.match_info.get('version', 3))
    if version < 1 or version > 3:
        raise GenericBadRequest('Unsupported API major version.')
    request['api_version'] = version
    resp = (await _handler(request))
    return resp


@web.middleware
async def exception_middleware(request, handler):
    app = request.app
    try:
        app['datadog'].statsd.increment('ai.backend.gateway.api.requests')
        resp = (await handler(request))
    except BackendError as ex:
        app['sentry'].captureException()
        statsd = app['datadog'].statsd
        statsd.increment('ai.backend.gateway.api.failures')
        statsd.increment(f'ai.backend.gateway.api.status.{ex.status_code}')
        raise
    except web.HTTPException as ex:
        statsd = app['datadog'].statsd
        statsd.increment('ai.backend.gateway.api.failures')
        statsd.increment(f'ai.backend.gateway.api.status.{ex.status_code}')
        if ex.status_code == 404:
            raise GenericNotFound
        if ex.status_code == 405:
            raise MethodNotAllowed
        log.warning(f'Bad request: {ex!r}')
        raise GenericBadRequest
    except asyncio.CancelledError:
        # The server is closing or the client has disconnected in the middle of
        # request.  Atomic requests are still executed to their ends.
        log.warning(f'Request cancelled ({request.method} {request.rel_url})')
        return web.Response(text='Request cancelled.', status=408)
    except Exception as ex:
        app['sentry'].captureException()
        log.exception('Uncaught exception in HTTP request handlers')
        raise InternalServerError
    else:
        app['datadog'].statsd.increment(
            f'ai.backend.gateway.api.status.{resp.status}')
        return resp


async def gw_init(app):
    # should be done in create_app() in other modules.
    app.router.add_route('GET', r'', hello)
    app.on_response_prepare.append(on_prepare)

    # legacy redirects
    app.router.add_route('GET', '/v{version:\d+}/authorize',
                         lambda request: web.HTTPFound('/v3/auth/test'))

    # populate public interfaces
    app['config_server'] = ConfigServer(
        app['config'].etcd_addr, app['config'].namespace)

    app['status'] = GatewayStatus.STARTING
    app['datadog'] = DummyDatadog()
    app['sentry'] = DummySentry()
    app['datadog.enabled'] = False
    app['sentry.enabled'] = False
    if datadog_available:
        if app['config'].datadog_api_key is None:
            log.warning('Datadog logging is disabled (missing API key).')
        else:
            datadog.initialize(
                api_key=app['config'].datadog_api_key,
                app_key=app['config'].datadog_app_key)
            app['datadog'] = datadog
            app['datadog.enabled'] = True
            log.info('Datadog logging is enabled.')
    if raven_available:
        if app['config'].raven_uri is None:
            log.warning('Sentry error reporting is disabled (missing DSN URI).')
        else:
            app['sentry'] = raven.Client(
                app['config'].raven_uri,
                release=raven.fetch_package_version('backend.ai-manager'))
            app['sentry.enabled'] = True
            log.info('Sentry error reporting is enabled.')

    app['dbpool'] = await create_engine(
        host=app['config'].db_addr[0], port=app['config'].db_addr[1],
        user=app['config'].db_user, password=app['config'].db_password,
        dbname=app['config'].db_name,
        echo=bool(app['config'].verbose),
        # TODO: check the throughput impacts of DB/redis pool sizes
        minsize=4, maxsize=16,
        timeout=30, pool_recycle=30,
    )
    app['redis_live'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_LIVE_DB)
    app['redis_stat'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_STAT_DB)
    app['redis_image'] = await aioredis.create_redis(
        app['config'].redis_addr.as_sockaddr(),
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
        app['sentry'].user_context(context)
        if sys.exc_info()[0] is not None:
            log.exception(f"Error inside event loop: {msg}")
            app['sentry'].captureException(True)
        else:
            exc_info = (type(exception), exception, exception.__traceback__)
            log.error(f"Error inside event loop: {msg}", exc_info=exc_info)
            app['sentry'].captureException(exc_info)


@aiotools.actxmgr
async def server_main(loop, pidx, _args):

    app = web.Application(middlewares=[
        exception_middleware,
        api_middleware,
    ])
    app['config'] = _args[0]
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
    ] + [
        ext_name for ext_name in app['config'].extensions
    ]

    global_exception_handler = functools.partial(handle_loop_error, app)
    scheduler_opts = {
        'limit': 2048,
        'close_timeout': 30,
        'exception_handler': global_exception_handler,
    }
    loop.set_exception_handler(global_exception_handler)
    aiojobs.aiohttp.setup(app, **scheduler_opts)
    await gw_init(app)
    for pkgname in subapp_pkgs:
        subapp_mod = importlib.import_module(pkgname, 'ai.backend.gateway')
        subapp, global_middlewares = getattr(subapp_mod, 'create_app')()
        assert isinstance(subapp, web.Application)
        # Allow subapp's access to the root app properties.
        # These are the public APIs exposed to extensions as well.
        for key in PUBLIC_INTERFACES:
            subapp[key] = app[key]
        prefix = subapp.get('prefix', pkgname.split('.')[-1].replace('_', '-'))
        aiojobs.aiohttp.setup(subapp, **scheduler_opts)
        app.add_subapp(r'/v{version:\d+}/' + prefix, subapp)
        app.middlewares.extend(global_middlewares)

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
    if datadog_available:
        parser.add('--datadog-api-key', env_var='DATADOG_API_KEY',
                   type=str, default=None,
                   help='The API key for Datadog monitoring agent.')
        parser.add('--datadog-app-key', env_var='DATADOG_APP_KEY',
                   type=str, default=None,
                   help='The application key for Datadog monitoring agent.')
    if raven_available:
        parser.add('--raven-uri', env_var='RAVEN_URI',
                   type=str, default=None,
                   help='The sentry.io event report URL with DSN.')


def main():

    config = load_config(extra_args_funcs=(gw_args, Logger.update_log_args))
    logger = Logger(config)
    logger.add_pkg('aiotools')
    logger.add_pkg('aiopg')
    logger.add_pkg('ai.backend')

    with logger:
        log.info(f'Backend.AI Gateway {__version__}')
        log.info(f'runtime: {env_info()}')
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
