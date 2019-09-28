import asyncio
from datetime import datetime
import hashlib, hmac
from importlib import import_module
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import tempfile

import aiodocker
import aiohttp
from aiohttp import web
import aiohttp_cors
import aiojobs.aiohttp
from async_timeout import timeout
from dateutil.tz import tzutc
import sqlalchemy as sa
import psycopg2 as pg
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.common.types import HostPortPair
from ai.backend.gateway.config import load as load_config
from ai.backend.gateway.server import (
    gw_init, gw_shutdown,
    exception_middleware, api_middleware,
    _get_legacy_handler,
    PUBLIC_INTERFACES)
from ai.backend.manager.models.base import populate_fixture
from ai.backend.manager.models import agents, kernels, keypairs, vfolders

here = Path(__file__).parent


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
def folder_mount(test_id):
    return Path(f'/tmp/backend.ai/vfolders-{test_id}')


@pytest.fixture(scope='session')
def folder_fsprefix(test_id):
    # NOTE: the prefix must NOT start with "/"
    return Path('fsprefix/inner/')


@pytest.fixture(scope='session')
def folder_host():
    return 'local'


@pytest.fixture(scope='session', autouse=True)
def prepare_and_cleanup_databases(request, test_ns, test_db,
                                  folder_mount, folder_host, folder_fsprefix):
    os.environ['BACKEND_NAMESPACE'] = test_ns
    os.environ['BACKEND_DB_NAME'] = test_db

    # Clear and reset etcd namespace using CLI functions.
    cfg = load_config()
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'volumes/_mount', str(folder_mount)])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'volumes/_fsprefix', str(folder_fsprefix)])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'volumes/_default_host', str(folder_host)])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'config/docker/registry/index.docker.io',
                     'https://registry-1.docker.io'])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'config/redis/addr', '127.0.0.1:8110'])
    # Add fake plugin settings.
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'config/plugins/cloudia/base_url', '127.0.0.1:8090'])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'config/plugins/cloudia/user', 'fake-cloudia-user@lablup.com'])
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                     'config/plugins/cloudia/password', 'fake-password'])

    def finalize_etcd():
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'delete',
                         '', '--prefix'])
    request.addfinalizer(finalize_etcd)

    # Create database using low-level psycopg2 API.
    db_addr = cfg['db']['addr']
    db_user = cfg['db']['user']
    db_pass = cfg['db']['password']
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
        cur.execute(f'REVOKE CONNECT ON DATABASE "{test_db}" FROM public;')
        cur.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                    'WHERE pid <> pg_backend_pid();')
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
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'schema', 'oneshot',
                         '-f', alembic_cfg.name])

    # Populate example_keypair fixture
    fixtures = {}
    fixtures.update(json.loads(
        (Path(__file__).parent.parent /
         'sample-configs' / 'example-keypairs.json').read_text()))
    fixtures.update(json.loads(
        (Path(__file__).parent.parent /
         'sample-configs' / 'example-resource-presets.json').read_text()))
    engine = sa.create_engine(alembic_url)
    conn = engine.connect()
    populate_fixture(conn, fixtures)
    conn.close()
    engine.dispose()


class Client:
    def __init__(self, session, url):
        self._session = session
        if not url.endswith('/'):
            url += '/'
        self._url = url

    async def close(self):
        await self._session.close()

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

    def put(self, path, **kwargs):
        while path.startswith('/'):
            path = path[1:]
        url = self._url + path
        return self._session.put(url, **kwargs)

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
async def app(event_loop, test_ns, test_db, unused_tcp_port_factory):
    """
    For tests that do not require actual server running.
    """
    app = web.Application(middlewares=[
        exception_middleware,
        api_middleware,
    ])
    app['config'] = load_config()

    # Override settings for testing.
    app['config']['db']['name'] = test_db
    app['config']['etcd']['namespace'] = test_ns
    app['config']['manager']['num-proc'] = 2
    app['config']['manager']['heartbeat-timeout'] = 10.0
    app['config']['manager']['service-addr'] = HostPortPair(
        '127.0.0.1', unused_tcp_port_factory())
    # import ssl
    # app['config'].ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app['config'].ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    # app['sslctx'] = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app['sslctx'].load_cert_chain(str(app['config'].ssl_cert),
    #                            str(app['config'].ssl_key))

    app['pidx'] = 0
    return app


@pytest.fixture
async def default_keypair(event_loop, app):
    """Global admin keypair"""
    # access_key = 'AKIAIOSFODNN7EXAMPLE'
    # config = app['config']
    # pool = await create_engine(
    #     host=config.db_addr[0], port=config.db_addr[1],
    #     user=config.db_user, password=config.db_password,
    #     dbname=config.db_name, minsize=1, maxsize=4
    # )
    # async with pool.acquire() as conn:
    #     query = (sa.select([keypairs.c.access_key, keypairs.c.secret_key])
    #                .select_from(keypairs)
    #                .where(keypairs.c.access_key == access_key))
    #     result = await conn.execute(query)
    #     row = await result.first()
    #     keypair = {
    #         'access_key': access_key,
    #         'secret_key': row.secret_key,
    #     }
    # pool.close()
    # await pool.wait_closed()
    # return keypair
    return {
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    }


@pytest.fixture
async def default_domain_keypair(event_loop, app):
    """Default domain admin keypair"""
    return {
        'access_key': 'AKIAHUKCHDEZGEXAMPLE',
        'secret_key': 'cWbsM_vBB4CzTW7JdORRMx8SjGI3-wEXAMPLEKEY',
    }


@pytest.fixture
async def user_keypair(event_loop, app):
    return {
        'access_key': 'AKIANABBDUSEREXAMPLE',
        'secret_key': 'C8qnIo29EZvXkPK_MXcuAakYTy4NYrxwmCEyNPlf',
    }


@pytest.fixture
async def monitor_keypair(event_loop, app):
    return {
        'access_key': 'AKIANAMONITOREXAMPLE',
        'secret_key': '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',
    }


@pytest.fixture
def get_headers(app, default_keypair, prepare_docker_images):
    def create_header(method, url, req_bytes, ctype='application/json',
                      hash_type='sha256', api_version='v4.20181215',
                      keypair=default_keypair):
        now = datetime.now(tzutc())
        hostname = f"localhost:{app['config']['manager']['service-addr'].port}"
        headers = {
            'Date': now.isoformat(),
            'Content-Type': ctype,
            'Content-Length': str(len(req_bytes)),
            'X-BackendAI-Version': api_version,
        }
        if api_version >= 'v4.20181215':
            req_bytes = b''
        else:
            if ctype.startswith('multipart'):
                req_bytes = b''
        if ctype.startswith('multipart'):
            # Let aiohttp to create appropriate header values
            # (e.g., multipart content-type header with message boundaries)
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


@pytest.fixture
async def create_app_and_client(request, test_id, test_ns,
                                event_loop, app,
                                default_keypair, user_keypair):
    client = None
    runner = None
    extra_proc = None

    async def maker(modules=None, spawn_agent=False):
        nonlocal client, runner, extra_proc

        if modules is None:
            modules = []
        scheduler_opts = {
            'close_timeout': 10,
        }
        cors_opts = {
            '*': aiohttp_cors.ResourceOptions(
                allow_credentials=False,
                expose_headers="*", allow_headers="*"),
        }
        aiojobs.aiohttp.setup(app, **scheduler_opts)
        await gw_init(app, cors_opts)
        for mod in modules:
            target_module = import_module(f'.{mod}', 'ai.backend.gateway')
            subapp, mw = getattr(target_module, 'create_app', None)(cors_opts)
            assert isinstance(subapp, web.Application)
            for key in PUBLIC_INTERFACES:
                subapp[key] = app[key]
            prefix = subapp.get('prefix', mod.replace('_', '-'))
            aiojobs.aiohttp.setup(subapp, **scheduler_opts)
            app.add_subapp('/' + prefix, subapp)
            app.middlewares.extend(mw)

            # TODO: refactor to avoid duplicates with gateway.server

            # Add legacy version-prefixed routes to the root app with some hacks
            for r in subapp.router.routes():
                for version in subapp['api_versions']:
                    subpath = r.resource.canonical
                    if subpath == f'/{prefix}':
                        subpath += '/'
                    legacy_path = f'/v{version}{subpath}'
                    handler = _get_legacy_handler(r.handler, subapp, version)
                    app.router.add_route(r.method, legacy_path, handler)

        server_params = {}
        client_params = {}

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            app['config']['manager']['service-addr'].host,
            app['config']['manager']['service-addr'].port,
            ssl_context=app.get('sslctx'),
            **server_params,
        )
        await site.start()

        # Launch an agent daemon
        if spawn_agent:
            etcd_addr = host_port_pair(os.environ['BACKEND_ETCD_ADDR'])
            os.makedirs(f'/tmp/backend.ai/scratches-{test_id}', exist_ok=True)
            agent_proc = subprocess.Popen([
                'python', '-m', 'ai.backend.agent.server',
                '--etcd-addr', str(etcd_addr),
                '--namespace', test_ns,
                '--scratch-root', f'/tmp/backend.ai/scratches-{test_id}',
                '--idle-timeout', '30',
            ])

            def finalize_agent():
                agent_proc.terminate()
                try:
                    agent_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    agent_proc.kill()
                shutil.rmtree(f'/tmp/backend.ai/scratches-{test_id}')

            request.addfinalizer(finalize_agent)

            async def wait_for_agent():
                while True:
                    all_ids = [inst_id async for inst_id in
                               app['registry'].enumerate_instances()]
                    if len(all_ids) > 0:
                        break
                    await asyncio.sleep(0.2)
            task = event_loop.create_task(wait_for_agent())
            with timeout(10.0):
                await task

        port = app['config']['manager']['service-addr'].port
        if app.get('sslctx'):
            url = f'https://localhost:{port}'
            client_params['connector'] = aiohttp.TCPConnector(verify_ssl=False)
        else:
            url = f'http://localhost:{port}'
        http_session = aiohttp.ClientSession(
            loop=event_loop, **client_params)
        client = Client(http_session, url)
        return app, client

    yield maker

    dbpool = app.get('dbpool')
    if dbpool is not None:
        # Clean DB tables for subsequent tests.
        async with dbpool.acquire() as conn, conn.begin():
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
        await client.close()
    if runner:
        await runner.cleanup()
    await gw_shutdown(app)

    if extra_proc:
        os.kill(extra_proc.pid, signal.SIGINT)
        extra_proc.join()


@pytest.fixture(scope='session')
def prepare_docker_images():
    _loop = asyncio.new_event_loop()

    async def pull():
        docker = aiodocker.Docker()
        images_to_pull = [
            'lablup/kernel-lua:5.3-alpine',
        ]
        for img in images_to_pull:
            try:
                await docker.images.inspect(img)
            except aiodocker.exceptions.DockerError as e:
                assert e.status == 404
                print(f'Pulling image "{img}" for testing...')
                await docker.pull(img)
        await docker.close()

    try:
        _loop.run_until_complete(pull())
    finally:
        _loop.close()


@pytest.fixture
async def prepare_kernel(request, create_app_and_client,
                         get_headers, default_keypair):
    sess_id = f'test-kernel-session-{secrets.token_hex(8)}'
    app, client = await create_app_and_client(
        modules=['etcd', 'events', 'auth', 'vfolder',
                 'admin', 'ratelimit', 'kernel', 'stream', 'manager'],
        spawn_agent=True)

    async def create_kernel(image='lua:5.3-alpine', tag=None):
        url = '/v3/kernel/'
        req_bytes = json.dumps({
            'image': image,
            'tag': tag,
            'clientSessionToken': sess_id,
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        response = await client.post(url, data=req_bytes, headers=headers)
        return await response.json()

    yield app, client, create_kernel

    access_key = default_keypair['access_key']
    try:
        await app['registry'].destroy_session(sess_id, access_key)
    except Exception:
        pass
