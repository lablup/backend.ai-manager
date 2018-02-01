from argparse import Namespace
from datetime import datetime
import hashlib, hmac
from importlib import import_module
import multiprocessing as mp
import os
import pathlib
import re
import secrets
import signal
import subprocess
import tempfile
# import ssl

import aiodocker
import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
import asyncio
from dateutil.tz import tzutc
import sqlalchemy as sa
import psycopg2 as pg
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.config import load_config
from ai.backend.gateway.events import event_router
from ai.backend.gateway.server import gw_init, gw_shutdown, gw_args
from ai.backend.manager import models
from ai.backend.manager.models import fixtures, agents, kernels, keypairs, vfolders
from ai.backend.manager.cli.etcd import delete, put, update_images, update_aliases
from ai.backend.manager.cli.dbschema import oneshot

here = pathlib.Path(__file__).parent


@pytest.fixture(scope='session')
def test_id():
    return secrets.token_hex(12)


@pytest.fixture(scope='session')
def test_ns(test_id):
    return f'testing-ns-{test_id}'


@pytest.fixture(scope='session')
def test_db(test_id):
    return f'test_db_{test_id}'


@pytest.fixture(scope='session')
def test_vfroot(test_id):
    return f'test-vf-{test_id}'


@pytest.fixture(scope='session', autouse=True)
def prepare_and_cleanup_databases(request, test_ns, test_db, test_vfroot):
    os.environ['BACKEND_NAMESPACE'] = test_ns
    os.environ['BACKEND_DB_NAME'] = test_db

    # Clear and reset etcd namespace using CLI functions.
    etcd_addr = host_port_pair(os.environ['BACKEND_ETCD_ADDR'])
    samples = here / '..' / 'sample-configs'
    args = Namespace(file=samples / 'image-metadata.yml',
                     etcd_addr=etcd_addr, namespace=test_ns)
    update_images(args)
    args = Namespace(file=samples / 'image-aliases.yml',
                     etcd_addr=etcd_addr, namespace=test_ns)
    update_aliases(args)
    args = Namespace(key='volumes/_mount', value='/tmp',
                     etcd_addr=etcd_addr, namespace=test_ns)
    put(args)
    args = Namespace(key='volumes/_vfroot', value=test_vfroot,
                     etcd_addr=etcd_addr, namespace=test_ns)
    put(args)

    def finalize_etcd():
        args = Namespace(key='', prefix=True,
                         etcd_addr=etcd_addr, namespace=test_ns)
        delete(args)

    request.addfinalizer(finalize_etcd)

    # Create database using low-level psycopg2 API.
    db_addr = host_port_pair(os.environ['BACKEND_DB_ADDR'])
    db_user = os.environ['BACKEND_DB_USER']
    db_pass = os.environ['BACKEND_DB_PASSWORD']
    if db_pass:
        # TODO: escape/urlquote db_pass
        db_url = f'postgresql://{db_user}:{db_pass}@{db_addr}'
    else:
        db_url = f'postgresql://{db_user}@{db_addr}'
    conn = pg.connect(db_url)
    conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE "{test_db}";')
    cur.close()
    conn.close()

    def finalize_db():
        conn = pg.connect(db_url)
        conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE "{test_db}";')
        cur.close()
        conn.close()

    request.addfinalizer(finalize_db)

    # Load the database schema using CLI function.
    alembic_url = db_url + '/' + test_db
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf8') as alembic_cfg:
        alembic_sample_cfg = here / '..' / 'alembic.ini.sample'
        alembic_cfg_data = alembic_sample_cfg.read_text()
        alembic_cfg_data = re.sub(
            r'^sqlalchemy.url = .*$',
            f'sqlalchemy.url = {alembic_url}',
            alembic_cfg_data, flags=re.M)
        alembic_cfg.write(alembic_cfg_data)
        alembic_cfg.flush()
        args = Namespace(
            config=pathlib.Path(alembic_cfg.name),
            schema_version='head')
        oneshot(args)

    # Populate example_keypair fixture
    fixture = getattr(fixtures, 'example_keypair')
    engine = sa.create_engine(alembic_url)
    conn = engine.connect()
    for rowset in fixture:
        table = getattr(models, rowset[0])
        conn.execute(table.insert(), rowset[1])
    conn.close()
    engine.dispose()

    # Launch an agent daemon
    agent_proc = subprocess.Popen([
        'python', '-m', 'ai.backend.agent.server',
        '--etcd-addr', str(etcd_addr),
        '--namespace', test_ns,
        '--scratch-root', '/tmp/scratches',
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def finalize_agent():
        agent_proc.terminate()
        try:
            agent_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            agent_proc.kill()

    request.addfinalizer(finalize_agent)


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
async def pre_app(event_loop, test_ns, test_db, unused_tcp_port):
    """ For tests that do not require actual server running.
    """
    app = web.Application(loop=event_loop)
    app.config = load_config(argv=[], extra_args_funcs=(gw_args,))

    # Override basic settings.
    # Change these configs if local servers have different port numbers.
    app.config.redis_addr = host_port_pair(os.environ['BACKEND_REDIS_ADDR'])
    app.config.db_addr = host_port_pair(os.environ['BACKEND_DB_ADDR'])
    app.config.db_name = test_db

    # Override extra settings
    app.config.namespace = test_ns
    app.config.heartbeat_timeout = 10.0
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_tcp_port
    app.config.verbose = False
    # app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.sslctx = None
    # app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app.sslctx.load_cert_chain(str(app.config.ssl_cert),
    #                            str(app.config.ssl_key))

    # num_workers = 1
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

    # Clean DB tables.
    # (the database itself is dropped when all tests finishes.)
    if app and 'dbpool' in app:
        async with app['dbpool'].acquire() as conn, conn.begin():
            await conn.execute((vfolders.delete()))
            await conn.execute((kernels.delete()))
            await conn.execute((agents.delete()))
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
