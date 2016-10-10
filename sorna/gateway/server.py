'''
The main web / websocket server
'''

import asyncio
import ssl

from aiohttp import web
import asyncpg
import uvloop

from .auth import init as auth_init
from .config import load_config
from .kernel import init as kernel_init

LATEST_API_VERSION = 'v1.20160915'


async def hello(request) -> web.Response:
    '''
    Returns the API version number.
    '''
    return web.json_response({'version': LATEST_API_VERSION})

async def on_prepare(request, response):
    response.headers['Server'] = 'Sorna-API/' + LATEST_API_VERSION

async def init(app):
    app.sslctx = None
    if app.config.ssl_cert and app.config.ssl_key:
        app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        app.sslctx.load_cert_chain(str(app.config.ssl_cert),
                                   str(app.config.ssl_key))
    if app.config.service_port == 0:
        app.config.service_port = 8443 if app.sslctx else 8080

    app.on_response_prepare.append(on_prepare)
    app.router.add_route('GET', '/v1', hello)

    app['dbpool'] = await asyncpg.create_pool(
        host=str(app.config.db_host),
        database=app.config.db_name,
        user=app.config.db_user,
        password=app.config.db_password)


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    app = web.Application()
    app.config = load_config()

    loop.run_until_complete(init(app))
    loop.run_until_complete(auth_init(app))
    loop.run_until_complete(kernel_init(app))

    web.run_app(app,
                host=str(app.config.service_ip),
                port=app.config.service_port,
                ssl_context=app.sslctx)


if __name__ == '__main__':
    main()
