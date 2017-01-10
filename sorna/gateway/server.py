'''
The main web / websocket server
'''

import asyncio
from ipaddress import ip_address
import logging
import signal
import ssl
import sys

import aiohttp
from aiohttp import web
import asyncpgsa
import uvloop

from sorna.argparse import ipaddr, path, port_no
from sorna.exceptions import (SornaError, GenericNotFound,
                              GenericBadRequest, InternalServerError)
from sorna.utils import env_info
from ..manager import __version__
from . import GatewayStatus
from .auth import init as auth_init, shutdown as auth_shutdown
from .config import load_config, init_logger
from .events import init as event_init, shutdown as event_shutdown
from .kernel import init as kernel_init, shutdown as kernel_shutdown
from .ratelimit import init as rlim_init, shutdown as rlim_shutdown

LATEST_API_VERSION = 'v1.20160915'

log = logging.getLogger('sorna.gateway.server')


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({'version': LATEST_API_VERSION})


async def on_prepare(request, response):
    response.headers['Server'] = 'Sorna-API/' + LATEST_API_VERSION


async def exception_middleware_factory(app, handler):
    async def exception_middleware_handler(request):
        try:
            return (await handler(request))
        except SornaError:
            raise
        except web.HTTPException as ex:
            if ex.status_code == 404:
                raise GenericNotFound
            log.warning(f'Bad request: {ex!r}')
            raise GenericBadRequest
        except Exception as ex:
            log.exception('Uncaught exception in HTTP request handlers')
            raise InternalServerError
    return exception_middleware_handler


async def gw_init(app):
    app.on_response_prepare.append(on_prepare)
    app.router.add_route('GET', '/v1', hello)
    app['status'] = GatewayStatus.STARTING

    app.dbpool = await asyncpgsa.create_pool(
        host=str(app.config.db_addr[0]),
        port=app.config.db_addr[1],
        database=app.config.db_name,
        user=app.config.db_user,
        password=app.config.db_password,
        min_size=4, max_size=16,
    )
    app.middlewares.append(exception_middleware_factory)


async def gw_shutdown(app):
    await app.dbpool.close()


def gw_args(parser):
    parser.add('--service-ip', env_var='SORNA_SERVICE_IP', type=ipaddr, default=ip_address('0.0.0.0'),
               help='The IP where the API gateway server listens on. (default: 0.0.0.0)')
    parser.add('--service-port', env_var='SORNA_SERVICE_PORT', type=port_no, default=0,
               help='The TCP port number where the API gateway server listens on. '
                    '(default: 8080, 8443 when SSL is enabled) '
                    'To run in production, you need the root privilege to use the standard 80/443 ports.')
    parser.add('--events-port', env_var='SORNA_EVENTS_PORT', type=port_no, default=5002,
               help='The TCP port number where the event server listens on.')
    parser.add('--ssl-cert', env_var='SORNA_SSL_CERT', type=path, default=None,
               help='The path to an SSL certificate file. '
                    'It may contain inter/root CA certificates as well. '
                    '(default: None)')
    parser.add('--ssl-key', env_var='SORNA_SSL_KEY', type=path, default=None,
               help='The path to the private key used to make requests for the SSL certificate. '
                    '(default: None)')


def main():

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    app = web.Application()
    app.config = load_config(extra_args_func=gw_args)
    init_logger(app.config)

    log.info(f'Sorna Gateway {__version__}')
    log.info(f'runtime: {env_info()}')

    log_config = logging.getLogger('sorna.gateway.config')
    log_config.debug('debug mode enabled.')

    if app.config.debug:
        aiohttp.log.server_logger.setLevel('DEBUG')
        aiohttp.log.access_logger.setLevel('DEBUG')
    else:
        aiohttp.log.server_logger.setLevel('WARNING')
        aiohttp.log.access_logger.setLevel('WARNING')

    term_ev = asyncio.Event(loop=loop)

    def handle_signal(loop, term_ev):
        if term_ev.is_set():
            log.warning('Forced shutdown!')
            sys.exit(1)
        else:
            term_ev.set()
            loop.stop()

    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)

    server = None
    web_handler = None

    async def initialize():
        nonlocal server, web_handler

        app.sslctx = None
        if app.config.ssl_cert and app.config.ssl_key:
            app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            app.sslctx.load_cert_chain(str(app.config.ssl_cert),
                                       str(app.config.ssl_key))
        if app.config.service_port == 0:
            app.config.service_port = 8443 if app.sslctx else 8080

        await event_init(app)
        await gw_init(app)
        await auth_init(app)
        await rlim_init(app)
        await kernel_init(app)

        web_handler = app.make_handler()
        server = await loop.create_server(
            web_handler,
            host=str(app.config.service_ip),
            port=app.config.service_port,
            ssl=app.sslctx,
        )
        log.info('started.')

    async def shutdown():
        log.info('shutting down...')
        server.close()
        await server.wait_closed()

        await kernel_shutdown(app)
        await rlim_shutdown(app)
        await auth_shutdown(app)
        await gw_shutdown(app)
        await event_shutdown(app)

        await app.shutdown()
        await web_handler.finish_connections(60.0)
        await app.cleanup()

        await loop.shutdown_asyncgens()

    try:
        loop.run_until_complete(initialize())
        loop.run_forever()
        # interrupted
        loop.run_until_complete(shutdown())
    finally:
        loop.close()
        log.info('terminated.')


if __name__ == '__main__':
    main()
