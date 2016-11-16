'''
The main web / websocket server
'''

import asyncio
from ipaddress import ip_address
import logging
import signal
import ssl

from aiohttp import web
import aiopg, aiopg.sa
import uvloop

from sorna.argparse import ipaddr, path, port_no
from .auth import init as auth_init, shutdown as auth_shutdown
from .config import load_config, init_logger
from .kernel import init as kernel_init, shutdown as kernel_shutdown

LATEST_API_VERSION = 'v1.20160915'

log = logging.getLogger('sorna.gateway.server')


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({'version': LATEST_API_VERSION})


async def on_prepare(request, response):
    response.headers['Server'] = 'Sorna-API/' + LATEST_API_VERSION


async def gw_init(app):
    app.sslctx = None
    if app.config.ssl_cert and app.config.ssl_key:
        app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        app.sslctx.load_cert_chain(str(app.config.ssl_cert),
                                   str(app.config.ssl_key))
    if app.config.service_port == 0:
        app.config.service_port = 8443 if app.sslctx else 8080

    app.on_response_prepare.append(on_prepare)
    app.router.add_route('GET', '/v1', hello)

    app.dbpool = await aiopg.sa.create_engine(
        host=app.config.db_addr[0],
        port=app.config.db_addr[1],
        database=app.config.db_name,
        user=app.config.db_user,
        password=app.config.db_password,
    )


async def gw_shutdown(app):
    app.dbpool.close()
    await app.dbpool.wait_closed()


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    def server_args(parser):
        parser.add('--service-ip', env_var='SORNA_SERVICE_IP', type=ipaddr, default=ip_address('0.0.0.0'),
                   help='The IP where the API gateway server listens on. (default: 0.0.0.0)')
        parser.add('--service-port', env_var='SORNA_SERVICE_PORT', type=port_no, default=0,
                   help='The TCP port number where the API gateway server listens on. '
                        '(default: 8080, 8443 when SSL is enabled) '
                        'To run in production, you need the root privilege to use the standard 80/443 ports.')
        parser.add('--ssl-cert', env_var='SORNA_SSL_CERT', type=path, default=None,
                   help='The path to an SSL certificate file. '
                        'It may contain inter/root CA certificates as well. '
                        '(default: None)')
        parser.add('--ssl-key', env_var='SORNA_SSL_KEY', type=path, default=None,
                   help='The path to the private key used to make requests for the SSL certificate. '
                        '(default: None)')

    app = web.Application()
    app.config = load_config(extra_args_func=server_args)
    init_logger(app.config)

    term_ev = asyncio.Event(loop=loop)

    async def init_apps():
        await gw_init(app)
        await auth_init(app)
        await kernel_init(app)

    loop.run_until_complete(init_apps())

    def handle_signal(loop, term_ev):
        if not term_ev.is_set():
            term_ev.set()
            loop.stop()

    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)

    try:
        web_handler = app.make_handler()
        server = loop.run_until_complete(loop.create_server(
            web_handler,
            host=str(app.config.service_ip),
            port=app.config.service_port,
            ssl=app.sslctx,
        ))
        log.info('started.')
        loop.run_forever()

        # interrupted
        async def shutdown_all():
            server.close()
            await server.wait_closed()

            await kernel_shutdown(app)
            await auth_shutdown(app)
            await gw_shutdown(app)

            await app.shutdown()
            await web_handler.finish_connections(60.0)
            await app.cleanup()

        log.info('shutting down...')
        loop.run_until_complete(shutdown_all())
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
