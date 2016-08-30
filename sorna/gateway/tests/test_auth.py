from ..auth import init as auth_init
from ..server import init as server_init


async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    await server_init(app)
    resp = await client.get('/1.0')
    body = await resp.read()
    assert body == b'OK'

async def test_auth(create_app_and_client):
    app, client = await create_app_and_client()
    await auth_init(app)
    # TODO: implement
