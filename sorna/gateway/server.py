'''
The main web / websocket server
'''

import asyncio
import logging, logging.config
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
        host=app.config.db_addr[0],
        port=app.config.db_addr[1],
        database=app.config.db_name,
        user=app.config.db_user,
        password=app.config.db_password)


def main():

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
                'field_styles': {'levelname': {'color':'black', 'bold':True},
                                 'name': {'color':'black', 'bold':True},
                                 'asctime': {'color':'black'}},
                'level_styles': {'info': {'color':'cyan'},
                                 'debug': {'color':'green'},
                                 'warning': {'color':'yellow'},
                                 'error': {'color':'red'},
                                 'critical': {'color':'red', 'bold':True}},
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'colored',
                'stream': 'ext://sys.stdout',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            'sorna': {
                'handlers': ['console'],
                'level': 'INFO',
            },
        },
    })

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
