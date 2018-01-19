from datetime import datetime
import hashlib, hmac
from importlib import import_module
import multiprocessing as mp
import os
import pathlib
import signal
# import ssl

import aiodocker
import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
import asyncio
from dateutil.tz import tzutc
import sqlalchemy as sa
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.config import load_config
from ai.backend.gateway.events import event_router
from ai.backend.gateway.server import gw_init, gw_shutdown, gw_args
from ai.backend.manager.models import kernels, keypairs, vfolders

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

    def patch(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.patch(url, **kwargs)

    def delete(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.delete(url, **kwargs)

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
    app.config.heartbeat_timeout = 10.0
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_tcp_port
    # app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.sslctx = None
    # app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app.sslctx.load_cert_chain(str(app.config.ssl_cert),
    #                            str(app.config.ssl_key))

    # num_workers = 1
    manager = mp.managers.SyncManager()
    manager.start(lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
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
async def user_keypair(event_loop, pre_app):
    access_key = 'AKIANOTADMIN7EXAMPLE'
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
def get_headers(pre_app, default_keypair, prepare_docker_images):
    def create_header(method, url, req_bytes, ctype='application/json',
                      hash_type='sha256', api_version='v3.20170615',
                      keypair=default_keypair):
        now = datetime.now(tzutc())
        hostname = f'localhost:{pre_app.config.service_port}'
        headers = {
            'Date': now.isoformat(),
            'Content-Type': ctype,
            'Content-Length': str(len(req_bytes)),
            'X-BackendAI-Version': api_version,
        }
        if ctype.startswith('multipart'):
            req_bytes = b''
            del headers['Content-Type']
            del headers['Content-Length']
        req_hash = hashlib.new(hash_type, req_bytes).hexdigest()
        sign_bytes = method.upper().encode() + b'\n' \
                     + url.encode() + b'\n' \
                     + now.isoformat().encode() + b'\n' \
                     + b'host:' + hostname.encode() + b'\n' \
                     + b'content-type:' + ctype.encode() + b'\n' \
                     + b'x-backendai-version:' + api_version.encode() + b'\n' \
                     + req_hash.encode()
        sign_key = hmac.new(keypair['secret_key'].encode(),
                            now.strftime('%Y%m%d').encode(), hash_type).digest()
        sign_key = hmac.new(sign_key, hostname.encode(), hash_type).digest()
        signature = hmac.new(sign_key, sign_bytes, hash_type).hexdigest()
        headers['Authorization'] = \
            f'BackendAI signMethod=HMAC-{hash_type.upper()}, ' \
            + f'credential={keypair["access_key"]}:{signature}'
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
        # ssl=pre_app.sslctx)
    )
    return pre_app, pre_app.config.service_port, handler, server


@pytest.fixture
async def create_app_and_client(event_loop, pre_app, default_keypair, user_keypair):
    client = None
    app = handler = server = None
    extra_proc = None
    extra_shutdowns = []

    async def maker(extras=None, ev_router=False):
        nonlocal client, app, handler, server, extra_proc, extra_shutdowns

        # Store extra inits and shutowns
        extra_inits = []
        if extras is None:
            extras = []
        for extra in extras:
            assert extra in ['etcd', 'events', 'auth', 'vfolder', 'admin',
                             'ratelimit', 'kernel']
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
        if ev_router:
            # Run event_router proc. Is it enough? No way to get return values
            # (app, client, etc) by using aiotools.start_server.
            args = (app.config,)
            extra_proc = mp.Process(target=event_router,
                                    args=('', 0, args,),
                                    daemon=True)
            extra_proc.start()
        if app.sslctx:
            url = f'https://localhost:{port}'
            client_params['connector'] = aiohttp.TCPConnector(verify_ssl=False)
        else:
            url = f'http://localhost:{port}'
        client = Client(aiohttp.ClientSession(loop=event_loop, **client_params), url)
        return app, client

    yield maker

    # Clean DB
    # TODO: load DB server dedicated only for testing, and exploit transaction
    #       rollback to provide clean DB table for each test.
    if app and 'dbpool' in app:
        async with app['dbpool'].acquire() as conn, conn.begin():
            await conn.execute((vfolders.delete()))
            await conn.execute((kernels.delete()))
            # from ai.backend.manager.models import agents
            # await conn.execute((agents.delete()))
            access_key = default_keypair['access_key']
            query = (sa.update(keypairs)
                       .values({
                           'concurrency_used': 0,
                           'num_queries': 0,
                       })
                       .where(keypairs.c.access_key == access_key))
            await conn.execute(query)
            access_key = user_keypair['access_key']
            query = (sa.update(keypairs)
                       .values({
                           'concurrency_used': 0,
                           'num_queries': 0,
                       })
                       .where(keypairs.c.access_key == access_key))
            await conn.execute(query)

    # Terminate client and servers
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
    if extra_proc:
        os.kill(extra_proc.pid, signal.SIGINT)
        extra_proc.join()


@pytest.fixture(scope='session')
def prepare_docker_images():
    event_loop = asyncio.get_event_loop()

    async def pull():
        docker = aiodocker.Docker()
        images_to_pull = [
            'lablup/kernel-lua:latest',
        ]
        for img in images_to_pull:
            try:
                await docker.images.get(img)
            except aiodocker.exceptions.DockerError as e:
                assert e.status == 404
                print(f'Pulling image "{img}" for testing...')
                await docker.pull(img)
        await docker.close()
    event_loop.run_until_complete(pull())
