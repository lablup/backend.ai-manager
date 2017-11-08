'''
The main web / websocket server
'''

import asyncio
from ipaddress import ip_address
import logging
from multiprocessing.managers import SyncManager
import os
import signal
import ssl

import aiohttp
from aiohttp import web
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
    host_port_pair, HostPortPair
)
from ai.backend.common.utils import env_info
from ai.backend.common.monitor import DummyDatadog, DummySentry
from ..manager import __version__
from . import GatewayStatus
from .defs import REDIS_STAT_DB
from .exceptions import (BackendError, GenericNotFound,
                         GenericBadRequest, InternalServerError)
from .admin import init as admin_init, shutdown as admin_shutdown
from .auth import init as auth_init, shutdown as auth_shutdown
from .config import load_config, init_logger
from .etcd import init as etcd_init, shutdown as etcd_shutdown
from .events import init as event_init, shutdown as event_shutdown
from .events import event_router
from .kernel import init as kernel_init, shutdown as kernel_shutdown
from .ratelimit import init as rlim_init, shutdown as rlim_shutdown

VALID_VERSIONS = frozenset([
    'v1.20160915',
    'v2.20170315',
    'v3.20170615',
])
LATEST_API_VERSION = 'v3.20170615'

log = logging.getLogger('ai.backend.gateway.server')


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({'version': LATEST_API_VERSION})


async def on_prepare(request, response):
    response.headers['Server'] = 'BackendAI-API/' + LATEST_API_VERSION


async def api_middleware_factory(app, handler):
    async def api_middleware_handler(request):
        _handler = handler
        method_override = request.headers.get('X-Method-Override', None)
        if method_override:
            request = request.clone(method=method_override)
            new_match_info = await app.router.resolve(request)
            _handler = new_match_info.handler
            request._match_info = new_match_info
        if request.match_info.http_exception is not None:
            return request.match_info.http_exception
        version = int(request.match_info['version'])
        if version < 1 or version > 3:
            raise GenericBadRequest('Unsupported API major version.')
        request['api_version'] = version
        resp = (await _handler(request))
        return resp
    return api_middleware_handler


async def exception_middleware_factory(app, handler):
    async def exception_middleware_handler(request):
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
            log.warning(f'Bad request: {ex!r}')
            raise GenericBadRequest
        except Exception as ex:
            app['sentry'].captureException()
            log.exception('Uncaught exception in HTTP request handlers')
            raise InternalServerError
        else:
            app['datadog'].statsd.increment(
                f'ai.backend.gateway.api.status.{resp.status}')
            return resp
    return exception_middleware_handler


async def gw_init(app):
    app.on_response_prepare.append(on_prepare)
    app.router.add_route('GET', r'/v{version:\d+}', hello)
    app['status'] = GatewayStatus.STARTING
    app['datadog'] = DummyDatadog()
    app['sentry'] = DummySentry()
    if datadog_available:
        if app.config.datadog_api_key is None:
            log.warning('datadog logging disabled (missing API key)')
        else:
            datadog.initialize(
                api_key=app.config.datadog_api_key,
                app_key=app.config.datadog_app_key)
            app['datadog'] = datadog
            log.info('datadog logging enabled')
    if raven_available:
        if app.config.raven_uri is None:
            log.info('skipping Sentry initialization due to missing DSN URI...')
        else:
            app['sentry'] = raven.Client(
                app.config.raven_uri,
                release=raven.fetch_package_version('backend.ai-manager'))
            log.info('sentry logging enabled')

    app['dbpool'] = await create_engine(
        host=app.config.db_addr[0], port=app.config.db_addr[1],
        user=app.config.db_user, password=app.config.db_password,
        dbname=app.config.db_name, minsize=4, maxsize=16,
        echo=bool(app.config.verbose),
    )
    app['redis_stat_pool'] = await aioredis.create_pool(
        app.config.redis_addr.as_sockaddr(),
        create_connection_timeout=3.0,
        encoding='utf8',
        db=REDIS_STAT_DB)
    app.middlewares.append(exception_middleware_factory)
    app.middlewares.append(api_middleware_factory)


async def gw_shutdown(app):
    app['redis_stat_pool'].close()
    await app['redis_stat_pool'].wait_closed()
    app['dbpool'].close()
    await app['dbpool'].wait_closed()


@aiotools.actxmgr
async def server_main(loop, pidx, _args):

    app = web.Application()
    app.config = _args[0]
    app.sslctx = None
    if app.config.ssl_cert and app.config.ssl_key:
        app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        app.sslctx.load_cert_chain(str(app.config.ssl_cert),
                                   str(app.config.ssl_key))
    if app.config.service_port == 0:
        app.config.service_port = 8443 if app.sslctx else 8080

    app['shared_states'] = _args[1]
    app['pidx'] = pidx

    await etcd_init(app)
    await event_init(app)
    await gw_init(app)
    await auth_init(app)
    await admin_init(app)
    await rlim_init(app)
    await kernel_init(app)

    web_handler = app.make_handler()
    server = await loop.create_server(
        web_handler,
        host=str(app.config.service_ip),
        port=app.config.service_port,
        reuse_port=True,
        ssl=app.sslctx,
    )
    log.info('started.')

    try:
        yield
    finally:

        log.info('shutting down...')
        server.close()
        await server.wait_closed()

        await kernel_shutdown(app)
        await rlim_shutdown(app)
        await admin_shutdown(app)
        await auth_shutdown(app)
        await gw_shutdown(app)
        await event_shutdown(app)
        await etcd_shutdown(app)

        await app.shutdown()
        await app.cleanup()


def gw_args(parser):
    parser.add('--namespace', env_var='BACKEND_NAMESPACE',
               type=str, default='local',
               help='The namespace of this Backend.AI cluster. (default: local)')
    parser.add('--etcd-addr', env_var='BACKEND_ETCD_ADDR',
               type=host_port_pair, metavar='HOST:PORT',
               default=HostPortPair(ip_address('127.0.0.1'), 2379),
               help='The host:port pair of the etcd cluster or its proxy.')
    parser.add('--events-port', env_var='BACKEND_EVENTS_PORT',
               type=port_no, default=5002,
               help='The TCP port number where the event server listens on.')
    parser.add('--docker-registry', env_var='BACKEND_DOCKER_REGISTRY',
               type=str, metavar='URL', default=None,
               help='The host:port pair of the private docker registry '
                    'that caches the kernel images')
    parser.add('--heartbeat-timeout', env_var='BACKEND_HEARTBEAT_TIMEOUT',
               type=float, default=5.0,
               help='The timeout for agent heartbeats.')
    parser.add('--service-ip', env_var='BACKEND_SERVICE_IP',
               type=ipaddr, default=ip_address('0.0.0.0'),
               help='The IP where the API gateway server listens on. '
                    '(default: 0.0.0.0)')
    parser.add('--service-port', env_var='BACKEND_SERVICE_PORT',
               type=port_no, default=0,
               help='The TCP port number where the API gateway server listens on. '
                    '(default: 8080, 8443 when SSL is enabled) '
                    'To run in production, you need the root privilege '
                    'to use the standard 80/443 ports.')
    parser.add('--ssl-cert', env_var='BACKEND_SSL_CERT',
               type=path, default=None,
               help='The path to an SSL certificate file. '
                    'It may contain inter/root CA certificates as well. '
                    '(default: None)')
    parser.add('--ssl-key', env_var='BACKEND_SSL_KEY',
               type=path, default=None,
               help='The path to the private key used to make requests '
                    'for the SSL certificate. '
                    '(default: None)')
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

    config = load_config(extra_args_func=gw_args)
    init_logger(config)

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

    num_workers = os.cpu_count()
    manager = SyncManager()
    manager.start(lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
    shared_states = manager.Namespace()
    shared_states.lock = manager.Lock()
    shared_states.barrier = manager.Barrier(num_workers)
    shared_states.agent_last_seen = manager.dict()

    try:
        aiotools.start_server(server_main, num_workers=num_workers,
                              extra_procs=[event_router],
                              args=(config, shared_states))
    finally:
        manager.shutdown()
        log.info('terminated.')


if __name__ == '__main__':
    main()
