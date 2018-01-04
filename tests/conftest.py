import pathlib
import socket
# import ssl

import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
import sqlalchemy as sa
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.config import load_config
from ai.backend.gateway.server import gw_init, gw_shutdown, gw_args
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
async def pre_app(event_loop, unused_tcp_port):
    """ For tests not require actual server running.
    """
    app = web.Application(loop=event_loop)
    app.config = load_config(argv=[], extra_args_func=gw_args)

    # Override basic settings.
    # Change these configs if local servers have different port numbers.
    app.config.redis_addr = host_port_pair('127.0.0.1:6379')
    app.config.db_addr = host_port_pair('127.0.0.1:5432')
    app.config.db_name = 'testing'

    # Override extra settings
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_tcp_port
    # app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.sslctx = None
    # app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app.sslctx.load_cert_chain(str(app.config.ssl_cert),
    #                            str(app.config.ssl_key))

    return app


@pytest.fixture
async def default_keypair(event_loop, pre_app):
    access_key = 'AKIAIOSFODNN7EXAMPLE'
    config = pre_app.config
    pool = await create_engine(
        host=config.db_addr[0], port=config.db_addr[1],
        user=config.db_user, password=config.db_password,
        dbname=config.db_name, minsize=1, maxsize=4
    )
    async with pool.acquire() as conn:
        query = (sa.select([keypairs.c.access_key, keypairs.c.secret_key])
                   .select_from(keypairs)
                   .where(keypairs.c.access_key == access_key))
        result = await conn.execute(query)
        row = await result.first()
        keypair = {
            'access_key': access_key,
            'secret_key': row.secret_key,
        }
    pool.close()
    await pool.wait_closed()
    return keypair


async def _create_server(loop, pre_app, extra_inits=None, debug=False):
    await gw_init(pre_app)
    if extra_inits:
        for init in extra_inits:
            await init(pre_app)
    handler = pre_app.make_handler(debug=debug, keep_alive_on=False)
    server = await loop.create_server(
        handler,
        pre_app.config.service_ip,
        pre_app.config.service_port,
        # ssl=app.sslctx)
    )
    return pre_app, pre_app.config.service_port, handler, server


@pytest.fixture
async def create_app_and_client(event_loop, pre_app):
    client = None
    app = handler = server = None
    extra_shutdown_list = None

    async def maker(extra_inits=None, extra_shutdowns=None):
        nonlocal client, app, handler, server, extra_shutdown_list
        server_params = {}
        client_params = {}
        extra_shutdown_list = extra_shutdowns
        app, port, handler, server = await _create_server(
            event_loop, pre_app, extra_inits=extra_inits, **server_params)
        if app.sslctx:
            url = f'https://localhost:{port}'
            client_params['connector'] = aiohttp.TCPConnector(verify_ssl=False)
        else:
            url = f'http://localhost:{port}'
        client = Client(aiohttp.ClientSession(loop=event_loop, **client_params), url)
        return app, client

    yield maker

    if client:
        client.close()
    await gw_shutdown(pre_app)
    if extra_shutdown_list:
        for shutdown in extra_shutdown_list:
            await shutdown(pre_app)
    if server:
        server.close()
        await server.wait_closed()
    if app:
        await app.shutdown()
        await app.cleanup()
