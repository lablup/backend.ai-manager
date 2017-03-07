from aiohttp import web

from sorna.gateway.server import hello, LATEST_API_VERSION


async def test_hello(test_client, loop):
    app = web.Application(loop=loop)
    app.router.add_get('/', hello)
    client = await test_client(app)
    resp = await client.get('/')

    assert resp.status == 200
    result = await resp.json()
    assert {'version': LATEST_API_VERSION} == result
