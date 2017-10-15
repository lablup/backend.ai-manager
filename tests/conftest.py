import pathlib
import socket
import ssl

import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
import sqlalchemy as sa
import pytest

from ai.backend.gateway.config import load_config
from ai.backend.gateway.server import gw_init, gw_args
from ai.backend.manager.models import keypairs

here = pathlib.Path(__file__).parent


@pytest.fixture
def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Client:
    def __init__(self, session, url):
        self._session = session
        if not url.endswith('/'):
            url += '/'
        self._url = url

    def close(self):
        self._session.close()

    def request(self, method, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.request(method, url, **kwargs)

    def get(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.get(url, **kwargs)

    def post(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.post(url, **kwargs)

    def delete(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.delete(url)

    def ws_connect(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.ws_connect(url, **kwargs)


@pytest.fixture
async def default_keypair(event_loop):
    access_key = 'AKIAIOSFODNN7EXAMPLE'
    config = load_config(argv=[], extra_args_func=gw_args)
    pool = await create_engine(
        dsn=f'host={config.db_addr[0]} port={config.db_addr[1]} '
            f'user={config.db_user} password={config.db_password} '
            f'dbname={config.db_name}',
        minsize=1, maxsize=4,
    )
    async with pool.acquire() as conn:
        query = sa.select([keypairs.c.access_key, keypairs.c.secret_key]) \
                  .where(keypairs.c.access_key == access_key)
        result = await conn.execute(query)
        row = await result.first()
        keypair = {
            'access_key': access_key,
            'secret_key': row.secret_key,
        }
    pool.close()
    await pool.wait_closed()
    return keypair


async def _create_server(loop, unused_port, extra_inits=None, debug=False):
    app = web.Application(loop=loop)
    app.config = load_config(argv=[], extra_args_func=gw_args)

    # Override default configs for testing setup.
    app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_port
    app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    app.sslctx.load_cert_chain(str(app.config.ssl_cert),
                               str(app.config.ssl_key))

    await gw_init(app)
    if extra_inits:
        for init in extra_inits:
            await init(app)
    handler = app.make_handler(debug=debug, keep_alive_on=False)
    server = await loop.create_server(
        handler,
        app.config.service_ip,
        app.config.service_port,
        ssl=app.sslctx)
    return app, app.config.service_port, handler, server


@pytest.fixture
async def create_app_and_client(event_loop, unused_port):
    client = None
    app = handler = server = None

    async def maker(extra_inits=None):
        nonlocal client, app, handler, server
        server_params = {}
        client_params = {}
        app, port, handler, server = await _create_server(
            event_loop, unused_port, extra_inits=extra_inits, **server_params)
        if app.sslctx:
            url = 'https://localhost:{}'.format(port)
            client_params['connector'] = aiohttp.TCPConnector(verify_ssl=False)
        else:
            url = 'http://localhost:{}'.format(port)
        client = Client(aiohttp.ClientSession(loop=event_loop, **client_params), url)
        return app, client

    yield maker

    if client:
        client.close()
    server.close()
    await server.wait_closed()
    await app.shutdown()
    await handler.finish_connections()
    await app.cleanup()
