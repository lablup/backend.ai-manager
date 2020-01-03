from datetime import datetime
import hashlib, hmac
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
from typing import (
    Any, Optional,
    AsyncGenerator,
    Sequence,
    Mapping,
    Tuple,
)

import aiohttp
from aiohttp import web
from dateutil.tz import tzutc
import sqlalchemy as sa
import psycopg2 as pg
import pytest

from ai.backend.gateway.config import load as load_config, redis_config_iv
from ai.backend.gateway.etcd import ConfigServer
from ai.backend.gateway.server import (
    build_root_app,
)
from ai.backend.manager.models.base import populate_fixture
from ai.backend.manager.models import (
    scaling_groups,
    agents,
    kernels, keypairs, vfolders,
)
from ai.backend.common.types import HostPortPair

here = Path(__file__).parent


@pytest.fixture(scope='session', autouse=True)
def test_id():
    return secrets.token_hex(12)


@pytest.fixture(scope='session', autouse=True)
def test_ns(test_id):
    ret = f'testing-ns-{test_id}'
    os.environ['BACKEND_NAMESPACE'] = ret
    return ret


@pytest.fixture(scope='session')
def test_db(test_id):
    return f'test_db_{test_id}'


@pytest.fixture(scope='session')
def vfolder_mount(test_id):
    ret = Path(f'/tmp/backend.ai-testing/vfolders-{test_id}')
    ret.mkdir(parents=True, exist_ok=True)
    yield ret
    shutil.rmtree(ret.parent)


@pytest.fixture(scope='session')
def vfolder_fsprefix(test_id):
    # NOTE: the prefix must NOT start with "/"
    return Path('fsprefix/inner/')


@pytest.fixture(scope='session')
def vfolder_host():
    return 'local'


@pytest.fixture(scope='session')
def test_config(test_id, test_db):
    cfg = load_config()
    cfg['db']['name'] = test_db
    cfg['manager']['num-proc'] = 1
    cfg['manager']['service-addr'] = HostPortPair('localhost', 29100)
    # In normal setups, this is read from etcd.
    cfg['redis'] = redis_config_iv.check({
        'addr': {'host': '127.0.0.1', 'port': '6379'},
    })
    return cfg


@pytest.fixture(scope='session')
def etcd_fixture(test_id, test_config, vfolder_mount, vfolder_fsprefix, vfolder_host):
    # Clear and reset etcd namespace using CLI functions.
    with tempfile.NamedTemporaryFile(mode='w', suffix='.etcd.json') as f:
        etcd_fixture = {
            'volumes': {
                '_mount': str(vfolder_mount),
                '_fsprefix': str(vfolder_fsprefix),
                '_default_host': str(vfolder_host),
            },
            'nodes': {
            },
            'config': {
                'docker': {
                    'registry': {
                        'index.docker.io': 'https://registry-1.docker.io',
                    },
                },
                'redis': {
                    'addr': '127.0.0.1:6379',
                },
                'plugins': {
                    'cloudia': {
                        'base_url': '127.0.0.1:8090',
                        'user': 'fake-cloudia-user@lablup.com',
                        'password': 'fake-password',
                    }
                }
            },
        }
        json.dump(etcd_fixture, f)
        f.flush()
        subprocess.call([
            'python', '-m', 'ai.backend.manager.cli',
            'etcd', 'put-json', '', f.name,
        ])
    yield
    subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'delete',
                     '', '--prefix'])


@pytest.fixture
async def config_server(app, etcd_fixture):
    server = ConfigServer(
        app,
        app['config']['etcd']['addr'],
        app['config']['etcd']['user'],
        app['config']['etcd']['password'],
        app['config']['etcd']['namespace'],
    )
    yield server


@pytest.fixture(scope='session')
def database(request, test_config, test_db):
    '''
    Create a new database for the current test session
    and install the table schema using alembic.
    '''
    db_addr = test_config['db']['addr']
    db_user = test_config['db']['user']
    db_pass = test_config['db']['password']

    # Create database using low-level psycopg2 API.
    # Temporarily use "testing" dbname until we create our own db.
    if db_pass:
        # TODO: escape/urlquote db_pass
        db_url = f'postgresql://{db_user}:{db_pass}@{db_addr}/testing'
    else:
        db_url = f'postgresql://{db_user}@{db_addr}/testing'
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
    alembic_url = f'postgresql://{db_user}:{db_pass}@{db_addr}/{test_db}'
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


@pytest.fixture()
def database_fixture(test_config, test_db, database):
    '''
    Populate the example data as fixtures to the database
    and delete them after use.
    '''
    db_addr = test_config['db']['addr']
    db_user = test_config['db']['user']
    db_pass = test_config['db']['password']
    alembic_url = f'postgresql://{db_user}:{db_pass}@{db_addr}/{test_db}'

    fixtures = {}
    fixtures.update(json.loads(
        (Path(__file__).parent.parent /
         'sample-configs' / 'example-keypairs.json').read_text()))
    fixtures.update(json.loads(
        (Path(__file__).parent.parent /
         'sample-configs' / 'example-resource-presets.json').read_text()))
    engine = sa.create_engine(alembic_url)
    conn = engine.connect()
    try:
        populate_fixture(conn, fixtures, ignore_unique_violation=True)
    finally:
        conn.close()
        engine.dispose()

    yield

    engine = sa.create_engine(alembic_url)
    conn = engine.connect()
    try:
        conn.execute((vfolders.delete()))
        conn.execute((kernels.delete()))
        conn.execute((agents.delete()))
        conn.execute((keypairs.delete()))
        conn.execute((scaling_groups.delete()))
    finally:
        conn.close()
        engine.dispose()


class Client:
    def __init__(self, session, url):
        self._session = session
        if not url.endswith('/'):
            url += '/'
        self._url = url

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
async def app(test_config):
    '''
    Create an empty application with the test configuration.
    '''
    return build_root_app(0, test_config,
                          cleanup_contexts=[],
                          subapp_pkgs=[])


@pytest.fixture
async def create_app_and_client(test_config):
    client: Optional[Client] = None
    client_session: Optional[aiohttp.ClientSession] = None
    runner: Optional[web.BaseRunner] = None

    async def app_builder(
        cleanup_contexts: Sequence[AsyncGenerator[None, None]] = None,
        subapp_pkgs: Sequence[str] = None,
        scheduler_opts: Mapping[str, Any] = None,
    ) -> Tuple[web.Application, Client]:
        nonlocal client, client_session, runner

        if scheduler_opts is None:
            scheduler_opts = {}
        app = build_root_app(0, test_config,
                             cleanup_contexts=cleanup_contexts,
                             subapp_pkgs=subapp_pkgs,
                             scheduler_opts={'close_timeout': 10, **scheduler_opts})
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            str(app['config']['manager']['service-addr'].host),
            app['config']['manager']['service-addr'].port,
        )
        await site.start()
        port = app['config']['manager']['service-addr'].port
        client_session = aiohttp.ClientSession()
        client = Client(client_session, f'http://localhost:{port}')
        return app, client

    yield app_builder

    if client_session:
        await client_session.close()
    if runner:
        await runner.cleanup()


@pytest.fixture
def default_keypair():
    return {
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    }


@pytest.fixture
def default_domain_keypair():
    """Default domain admin keypair"""
    return {
        'access_key': 'AKIAHUKCHDEZGEXAMPLE',
        'secret_key': 'cWbsM_vBB4CzTW7JdORRMx8SjGI3-wEXAMPLEKEY',
    }


@pytest.fixture
def user_keypair():
    return {
        'access_key': 'AKIANABBDUSEREXAMPLE',
        'secret_key': 'C8qnIo29EZvXkPK_MXcuAakYTy4NYrxwmCEyNPlf',
    }


@pytest.fixture
def monitor_keypair():
    return {
        'access_key': 'AKIANAMONITOREXAMPLE',
        'secret_key': '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',
    }


@pytest.fixture
def get_headers(app, default_keypair):
    def create_header(method, url, req_bytes, ctype='application/json',
                      hash_type='sha256', api_version='v5.20191215',
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
