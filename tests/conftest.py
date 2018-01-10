from collections import defaultdict
from datetime import datetime
import functools
import hashlib, hmac
from importlib import import_module
import multiprocessing as mp
import os
import pathlib
import signal
# import ssl

import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
import aiotools
from dateutil.tz import tzutc
import sqlalchemy as sa
import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway import GatewayStatus
from ai.backend.gateway.config import load_config
from ai.backend.gateway.events import event_router
from ai.backend.gateway.kernel import (
    create, get_info, get_logs, restart, destroy, execute, interrupt, complete,
    stream_pty, not_impl_stub, upload_files, kernel_terminated, instance_started,
    instance_terminated, instance_heartbeat, instance_stats, check_agent_lost
)
from ai.backend.gateway.server import gw_init, gw_shutdown, gw_args
from ai.backend.gateway.utils import method_placeholder
from ai.backend.manager.models import keypairs
from ai.backend.manager.registry import InstanceRegistry

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
    app.config.heartbeat_timeout = 10.0
    app.config.service_ip = '127.0.0.1'
    app.config.service_port = unused_tcp_port
    # app.config.ssl_cert = here / 'sample-ssl-cert' / 'sample.crt'
    # app.config.ssl_key = here / 'sample-ssl-cert' / 'sample.key'
    app.sslctx = None
    # app.sslctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    # app.sslctx.load_cert_chain(str(app.config.ssl_cert),
    #                            str(app.config.ssl_key))

    num_workers = 1
    manager = mp.managers.SyncManager()
    manager.start(lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
    shared_states = manager.Namespace()
    shared_states.lock = manager.Lock()
    shared_states.barrier = manager.Barrier(num_workers)
    shared_states.agent_last_seen = manager.dict()
    app['shared_states'] = shared_states
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
    def create_header(method, url, req_bytes, ctype='application/json',
                      hash_type='sha256', api_version='v3.20170615'):
        now = datetime.now(tzutc())
        hostname = f'localhost:{pre_app.config.service_port}'
        headers = {
            'Date': now.isoformat(),
            'Content-Type': ctype,
            'Content-Length': str(len(req_bytes)),
            'X-BackendAI-Version': api_version,
        }
        req_hash = hashlib.new(hash_type, req_bytes).hexdigest()
        sign_bytes = method.upper().encode() + b'\n' \
                     + url.encode() + b'\n' \
                     + now.isoformat().encode() + b'\n' \
                     + b'host:' + hostname.encode() + b'\n' \
                     + b'content-type:' + ctype.encode() + b'\n' \
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
        # ssl=pre_app.sslctx)
    )
    return pre_app, pre_app.config.service_port, handler, server


@pytest.fixture
async def create_app_and_client(event_loop, pre_app, default_keypair):
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
            if extra == 'kernel':
                fn_init = temp_kernel_init
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
        from ai.backend.manager.models import vfolders, kernels
        access_key = default_keypair['access_key']
        async with app['dbpool'].acquire() as conn, conn.begin():
            await conn.execute((vfolders.delete()))
            await conn.execute((kernels.delete()))
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


async def temp_kernel_init(app):
    """ Temporary kernel initializer. Will be removed when shared agent heartbeat
    information is handled by redis."""
    rt = app.router.add_route
    rt('POST',   r'/v{version:\d+}/kernel/create', create)  # legacy
    rt('POST',   r'/v{version:\d+}/kernel/', create)
    rt('GET',    r'/v{version:\d+}/kernel/{sess_id}', get_info)
    rt('GET',    r'/v{version:\d+}/kernel/{sess_id}/logs', get_logs)

    rt('PATCH',  r'/v{version:\d+}/kernel/{sess_id}', restart)
    rt('DELETE', r'/v{version:\d+}/kernel/{sess_id}', destroy)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}', execute)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/interrupt', interrupt)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/complete', complete)
    rt('GET',    r'/v{version:\d+}/stream/kernel/{sess_id}/pty', stream_pty)
    rt('GET',    r'/v{version:\d+}/stream/kernel/{sess_id}/events', not_impl_stub)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/upload', upload_files)
    rt('POST',   r'/v{version:\d+}/folder/create', not_impl_stub)
    rt('GET',    r'/v{version:\d+}/folder/{folder_id}', not_impl_stub)
    rt('POST',   r'/v{version:\d+}/folder/{folder_id}', method_placeholder('DELETE'))
    rt('DELETE', r'/v{version:\d+}/folder/{folder_id}', not_impl_stub)

    app['event_dispatcher'].add_handler('kernel_terminated', kernel_terminated)
    app['event_dispatcher'].add_handler('instance_started', instance_started)
    app['event_dispatcher'].add_handler('instance_terminated', instance_terminated)
    app['event_dispatcher'].add_handler('instance_heartbeat', instance_heartbeat)
    app['event_dispatcher'].add_handler('instance_stats', instance_stats)

    app['stream_pty_handlers'] = defaultdict(set)
    app['stream_stdin_socks'] = defaultdict(set)

    app['registry'] = InstanceRegistry(
        app['config_server'],
        app['dbpool'],
        app['redis_stat_pool'])
    await app['registry'].init()

    # Scan ALIVE agents
    if app['pidx'] == 0:
        # log.debug(f'initializing agent status checker at proc:{app["pidx"]}')
        # now = time.monotonic()
        # async for inst in app['registry'].enumerate_instances():
        #     app['shared_states'].agent_last_seen[inst.id] = now
        app['agent_lost_checker'] = aiotools.create_timer(
            functools.partial(check_agent_lost, app), 1.0)

    # app['shared_states'].barrier.wait()
    app['status'] = GatewayStatus.RUNNING
