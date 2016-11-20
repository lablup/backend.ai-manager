import asyncio
import contextlib
import gc
import pathlib
import socket

import aiohttp
from aiohttp import web
import asyncpgsa
import sqlalchemy as sa
import pytest
import uvloop

from ..config import load_config
from ..server import gw_init, gw_args
from ..models import KeyPair


@contextlib.contextmanager
def loop_context(loop=None):
    current_scope = False
    if not loop:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        current_scope = True

    yield loop

    if current_scope:
        if not loop.is_closed():
            loop.call_soon(loop.stop)
            loop.run_forever()
            loop.close()
        gc.collect()
        asyncio.set_event_loop(None)


def pytest_pycollect_makeitem(collector, name, obj):
    # Patch pytest for coroutines
    if collector.funcnamefilter(name) and asyncio.iscoroutinefunction(obj):
        return list(collector._genfunctions(name, obj))


def pytest_pyfunc_call(pyfuncitem):
    # Patch pytest for coroutines.
    if asyncio.iscoroutinefunction(pyfuncitem.function):
        existing_loop = pyfuncitem.funcargs.get('loop', None)
        with loop_context(existing_loop) as loop:
            testargs = {arg: pyfuncitem.funcargs[arg]
                        for arg in pyfuncitem._fixtureinfo.argnames}
            task = loop.create_task(pyfuncitem.obj(**testargs))
            loop.run_until_complete(task)
        return True


@pytest.fixture
def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.yield_fixture
def loop():
    with loop_context() as loop:
        yield loop


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
def default_keypair(loop):

    async def _fetch():
        access_key = 'AKIAIOSFODNN7EXAMPLE'
        config = load_config(argv=[], extra_args_func=gw_args)
        pool = await asyncpgsa.create_pool(
            host=config.db_addr[0],
            port=config.db_addr[1],
            database=config.db_name,
            user=config.db_user,
            password=config.db_password,
            min_size=1, max_size=2,
        )
        async with pool.acquire() as conn:
            query = sa.select([KeyPair.c.access_key, KeyPair.c.secret_key]) \
                      .where(KeyPair.c.access_key == access_key)
            row = await conn.fetchrow(query)
            keypair = {
                'access_key': access_key,
                'secret_key': row.secret_key,
            }
        await pool.close()
        return keypair

    return loop.run_until_complete(_fetch())


@pytest.yield_fixture
def create_server(loop, unused_port):
    app = handler = server = None
    here = pathlib.Path(__file__).parent

    async def create(debug=False):
        nonlocal app, handler, server
        app = web.Application(loop=loop)
        app.config = load_config(argv=[], extra_args_func=gw_args)

        # Override default configs for testing setup.
        app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
        app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
        app.config.service_ip = '127.0.0.1'
        app.config.service_port = unused_port

        await gw_init(app)
        handler = app.make_handler(debug=debug, keep_alive_on=False)
        server = await loop.create_server(handler,
                                          app.config.service_ip,
                                          app.config.service_port,
                                          ssl=app.sslctx)
        return app, app.config.service_port

    yield create

    async def finish():
        nonlocal app, handler, server
        server.close()
        await server.wait_closed()
        await app.shutdown()
        await handler.finish_connections()
        await app.cleanup()
    loop.run_until_complete(finish())


@pytest.yield_fixture
def create_app_and_client(loop, create_server):
    client = None

    async def maker():
        nonlocal client
        server_params = {}
        client_params = {}
        app, port = await create_server(**server_params)
        if app.sslctx:
            url = 'https://localhost:{}'.format(port)
            client_params['connector'] = aiohttp.TCPConnector(verify_ssl=False)
        else:
            url = 'http://localhost:{}'.format(port)
        client = Client(aiohttp.ClientSession(loop=loop, **client_params), url)
        return app, client

    yield maker

    if client:
        client.close()
