from datetime import datetime
import hashlib, hmac
from importlib import import_module
import pathlib
# import ssl

import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
from dateutil.tz import tzutc
import sqlalchemy as sa
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.config import load_config
from ai.backend.gateway.server import gw_init, gw_shutdown, gw_args
from ai.backend.manager.models import keypairs

here = pathlib.Path(__file__).parent


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
    app.config.namespace = 'local'
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_tcp_port
    # app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.sslctx = None
    # app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app.sslctx.load_cert_chain(str(app.config.ssl_cert),
    #                            str(app.config.ssl_key))

    app['pidx'] = 0

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


@pytest.fixture
def get_headers(pre_app, default_keypair):
    def create_header(url, req_bytes, hash_type='sha256',
                      api_version='v3.20170615'):
        now = datetime.now(tzutc())
        hostname = f'localhost:{pre_app.config.service_port}'
        headers = {
            'Date': now.isoformat(),
            'Content-Type': 'application/json',
            'X-BackendAI-Version': api_version,
        }
        req_hash = hashlib.new(hash_type, req_bytes).hexdigest()
        sign_bytes = b'GET\n' \
                     + url.encode() + b'\n' \
                     + now.isoformat().encode() + b'\n' \
                     + b'host:' + hostname.encode() + b'\n' \
                     + b'content-type:application/json\n' \
                     + b'x-backendai-version:' + api_version.encode() + b'\n' \
                     + req_hash.encode()
        sign_key = hmac.new(default_keypair['secret_key'].encode(),
                            now.strftime('%Y%m%d').encode(), hash_type).digest()
        sign_key = hmac.new(sign_key, hostname.encode(), hash_type).digest()
        signature = hmac.new(sign_key, sign_bytes, hash_type).hexdigest()
        headers['Authorization'] = \
            f'BackendAI signMethod=HMAC-{hash_type.upper()}, ' \
            + f'credential={default_keypair["access_key"]}:{signature}'
        return headers
    return create_header


async def _create_server(loop, pre_app, extra_inits=None, debug=False):
    await gw_init(pre_app)
    if extra_inits:
        for init in extra_inits:
            await init(pre_app)
    handler = pre_app.make_handler(debug=debug, keep_alive_on=False, loop=loop)
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
    extra_shutdowns = []

    async def maker(extras=None):
        nonlocal client, app, handler, server, extra_shutdowns

        # Store extra inits and shutowns
        extra_inits = []
        for extra in extras:
            assert extra in ['etcd', 'event', 'auth', 'vfolder', 'admin',
                             'rlim', 'kernel']
            target_module = import_module(f'ai.backend.gateway.{extra}')
            fn_init = getattr(target_module, 'init', None)
            if fn_init:
                extra_inits.append(fn_init)
            fn_shutdown = getattr(target_module, 'shutdown', None)
            if fn_shutdown:
                extra_shutdowns.append(fn_shutdown)

        server_params = {}
        client_params = {}
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
    for shutdown in extra_shutdowns:
        await shutdown(pre_app)
    if server:
        server.close()
        await server.wait_closed()
    if app:
        await app.shutdown()
        await app.cleanup()
