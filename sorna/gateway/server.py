'''
The main web / websocket server
'''

import asyncio

from aiohttp import web
import asyncpg
import uvloop

from .auth import init as auth_init
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
    app.on_response_prepare.append(on_prepare)
    app.router.add_route('GET', '/v1', hello)
    app['dbpool'] = await asyncpg.create_pool(
        host='localhost',
        database='sorna',
        user='postgres',
        password='develove')


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = web.Application()
    loop.run_until_complete(init(app))
    loop.run_until_complete(auth_init(app))
    loop.run_until_complete(kernel_init(app))
    web.run_app(app, port=7000)


if __name__ == '__main__':
    main()
