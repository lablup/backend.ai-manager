'''
WebSocket-based streaming kernel interaction APIs.

NOTE: For nginx-based setups, we need to gather all websocket-based API handlers
      under this "/stream/"-prefixed app.
'''

import asyncio
import base64
from collections import defaultdict
import json
import logging
import secrets
from typing import (
    Any,
    Mapping,
    List, Tuple,
    Set,
)
import uuid
from urllib.parse import urlparse
import weakref

import aiohttp
from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response
from aiotools import apartial, adefer
import aiozmq
from aiozmq import create_zmq_stream as aiozmq_sock
import sqlalchemy as sa
import trafaret as t
import zmq

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId,
    KernelId,
)

from .auth import auth_required
from .exceptions import (
    AppNotFound, GroupNotFound, KernelNotFound,
    BackendError,
    InvalidAPIParameters, GenericForbidden,
    InternalServerError,
)
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params, call_non_bursty
from .wsproxy import TCPProxy
from ..manager.models import kernels, groups, UserRole

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))

sentinel = object()


@server_status_required(READ_ALLOWED)
@auth_required
@adefer
async def stream_pty(defer, request: web.Request) -> web.StreamResponse:
    app = request.app
    registry = app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)
    extra_fields = (kernels.c.stdin_port, kernels.c.stdout_port)
    api_version = request['api_version']
    try:
        kernel = await asyncio.shield(
            registry.get_session(sess_id, access_key, field=extra_fields))
    except KernelNotFound:
        raise
    log.info('STREAM_PTY(ak:{0}, s:{1})', access_key, sess_id)

    await asyncio.shield(registry.increment_session_usage(sess_id, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_pty_handlers'][stream_key].add(myself)
    defer(lambda: app['stream_pty_handlers'][stream_key].discard(myself))

    async def connect_streams(kernel):
        # TODO: refactor as custom row/table method
        if kernel.kernel_host is None:
            kernel_host = urlparse(kernel.agent_addr).hostname
        else:
            kernel_host = kernel.kernel_host
        stdin_addr = f'tcp://{kernel_host}:{kernel.stdin_port}'
        log.debug('stream_pty({0}): stdin: {1}', stream_key, stdin_addr)
        stdin_sock = await aiozmq_sock(zmq.PUB, connect=stdin_addr)
        stdin_sock.transport.setsockopt(zmq.LINGER, 100)
        stdout_addr = f'tcp://{kernel_host}:{kernel.stdout_port}'
        log.debug('stream_pty({0}): stdout: {1}', stream_key, stdout_addr)
        stdout_sock = await aiozmq_sock(zmq.SUB, connect=stdout_addr)
        stdout_sock.transport.setsockopt(zmq.LINGER, 100)
        stdout_sock.transport.subscribe(b'')
        return stdin_sock, stdout_sock

    # Wrap sockets in a list so that below coroutines can share reference changes.
    socks = list(await connect_streams(kernel))
    app['stream_stdin_socks'][stream_key].add(socks[0])
    defer(lambda: app['stream_stdin_socks'][stream_key].discard(socks[0]))
    stream_sync = asyncio.Event()

    async def stream_stdin():
        nonlocal socks
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data['type'] == 'stdin':
                        raw_data = base64.b64decode(data['chars'].encode('ascii'))
                        try:
                            socks[0].write([raw_data])
                        except (AttributeError, aiozmq.ZmqStreamClosed):
                            # AttributeError occurs when stdin_sock._transport
                            # is None because it's already closed somewhere
                            # else.
                            app['stream_stdin_socks'][stream_key].discard(socks[0])
                            socks[1].close()
                            kernel = await asyncio.shield(registry.get_session(
                                sess_id, access_key, field=extra_fields))
                            stdin_sock, stdout_sock = await connect_streams(kernel)
                            socks[0] = stdin_sock
                            socks[1] = stdout_sock
                            app['stream_stdin_socks'][stream_key].add(socks[0])
                            socks[0].write([raw_data])
                            log.debug('stream_stdin({0}): zmq stream reset',
                                      stream_key)
                            stream_sync.set()
                            continue
                    else:
                        await asyncio.shield(
                            registry.increment_session_usage(sess_id, access_key))
                        run_id = secrets.token_hex(8)
                        if data['type'] == 'resize':
                            code = f"%resize {data['rows']} {data['cols']}"
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', code, {},
                                flush_timeout=None)
                        elif data['type'] == 'ping':
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', '%ping', {},
                                flush_timeout=None)
                        elif data['type'] == 'restart':
                            # Close existing zmq sockets and let stream
                            # handlers get a new one with changed stdin/stdout
                            # ports.
                            log.debug('stream_stdin: restart requested')
                            if not socks[0].at_closing():
                                await asyncio.shield(
                                    registry.restart_session(sess_id, access_key))
                                socks[0].close()
                            else:
                                log.warning('stream_stdin({0}): '
                                            'duplicate kernel restart request; '
                                            'ignoring it.',
                                            stream_key)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.warning('stream_stdin({0}): connection closed ({1})',
                                stream_key, ws.exception())
        except asyncio.CancelledError:
            # Agent or kernel is terminated.
            raise
        except Exception:
            app['error_monitor'].capture_exception()
            log.exception('stream_stdin({0}): unexpected error', stream_key)
        finally:
            log.debug('stream_stdin({0}): terminated', stream_key)
            if not socks[0].at_closing():
                socks[0].close()

    async def stream_stdout():
        nonlocal socks
        log.debug('stream_stdout({0}): started', stream_key)
        try:
            while True:
                try:
                    data = await socks[1].read()
                except aiozmq.ZmqStreamClosed:
                    await stream_sync.wait()
                    stream_sync.clear()
                    log.debug('stream_stdout({0}): zmq stream reset', stream_key)
                    continue
                if ws.closed:
                    break
                await ws.send_str(json.dumps({
                    'type': 'out',
                    'data': base64.b64encode(data[0]).decode('ascii'),
                }, ensure_ascii=False))
        except asyncio.CancelledError:
            raise
        except:
            app['error_monitor'].capture_exception()
            log.exception('stream_stdout({0}): unexpected error', stream_key)
        finally:
            log.debug('stream_stdout({0}): terminated', stream_key)
            socks[1].close()

    # According to aiohttp docs, reading ws must be done inside this task.
    # We execute the stdout handler as another task.
    try:
        stdout_task = asyncio.ensure_future(stream_stdout())
        await stream_stdin()
    except:
        app['error_monitor'].capture_exception()
        log.exception('stream_pty({0}): unexpected error', stream_key)
    finally:
        stdout_task.cancel()
        await stdout_task
    return ws


@server_status_required(READ_ALLOWED)
@auth_required
@adefer
async def stream_execute(defer, request: web.Request) -> web.StreamResponse:
    '''
    WebSocket-version of gateway.kernel.execute().
    '''
    app = request.app
    registry = app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)
    api_version = request['api_version']
    log.info('STREAM_EXECUTE(ak:{0}, s:{1})', access_key, sess_id)
    try:
        _ = await asyncio.shield(registry.get_session(sess_id, access_key))  # noqa
    except KernelNotFound:
        raise

    await asyncio.shield(registry.increment_session_usage(sess_id, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_execute_handlers'][stream_key].add(myself)
    defer(lambda: app['stream_execute_handlers'][stream_key].discard(myself))

    # This websocket connection itself is a "run".
    run_id = secrets.token_hex(8)

    try:
        if ws.closed:
            log.debug('STREAM_EXECUTE: client disconnected (cancelled)')
            return ws
        params = await ws.receive_json()
        assert params.get('mode'), 'mode is missing or empty!'
        mode = params['mode']
        assert mode in {'query', 'batch'}, 'mode has an invalid value.'
        code = params.get('code', '')
        opts = params.get('options', None) or {}

        while True:
            # TODO: rewrite agent and kernel-runner for unbuffered streaming.
            raw_result = await registry.execute(
                sess_id, access_key,
                api_version, run_id, mode, code, opts,
                flush_timeout=0.2)
            if ws.closed:
                log.debug('STREAM_EXECUTE: client disconnected (interrupted)')
                await asyncio.shield(registry.interrupt_session(sess_id, access_key))
                break
            if raw_result is None:
                # repeat until we get finished
                log.debug('STREAM_EXECUTE: none returned, continuing...')
                mode = 'continue'
                code = ''
                opts.clear()
                continue
            await ws.send_json({
                'status': raw_result['status'],
                'console': raw_result.get('console'),
                'exitCode': raw_result.get('exitCode'),
                'options': raw_result.get('options'),
                'files': raw_result.get('files'),
            })
            if raw_result['status'] == 'waiting-input':
                mode = 'input'
                code = await ws.receive_str()
            elif raw_result['status'] == 'finished':
                break
            else:
                # repeat until we get finished
                mode = 'continue'
                code = ''
                opts.clear()
    except (json.decoder.JSONDecodeError, AssertionError) as e:
        log.warning('STREAM_EXECUTE: invalid/missing parameters: {0!r}', e)
        if not ws.closed:
            await ws.send_json({
                'status': 'error',
                'msg': f'Invalid API parameters: {e!r}',
            })
    except BackendError as e:
        log.exception('STREAM_EXECUTE: exception')
        if not ws.closed:
            await ws.send_json({
                'status': 'error',
                'msg': f'BackendError: {e!r}',
            })
        raise
    except asyncio.CancelledError:
        if not ws.closed:
            await ws.send_json({
                'status': 'server-restarting',
                'msg': 'The API server is going to restart for maintenance. '
                       'Please connect again with the same run ID.',
            })
        raise
    finally:
        return ws


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['app', 'service']): t.String,
        tx.AliasedKey(['port'], default=None): t.Null | t.Int[1024:65535],
    }))
@adefer
async def stream_proxy(defer, request: web.Request, params: Mapping[str, Any]) -> web.StreamResponse:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    service = request.query.get('app', None)  # noqa

    stream_key = (sess_id, access_key)
    myself = asyncio.Task.current_task()
    request.app['stream_proxy_handlers'][stream_key].add(myself)
    defer(lambda: request.app['stream_proxy_handlers'][stream_key].discard(myself))

    try:
        kernel = await asyncio.shield(registry.get_session(sess_id, access_key))
    except KernelNotFound:
        raise
    if kernel.kernel_host is None:
        kernel_host = urlparse(kernel.agent_addr).hostname
    else:
        kernel_host = kernel.kernel_host
    for sport in kernel.service_ports:
        if sport['name'] == service:
            if params['port']:
                try:
                    hport_idx = sport['container_ports'].index(params['port'])
                except ValueError:
                    raise InvalidAPIParameters(
                        f"Service {service} does not open the port number {params['port']}.")
                port = sport['host_ports'][hport_idx]
            else:
                if 'host_ports' not in sport:
                    port = sport['host_port']  # legacy kernels
                else:
                    port = sport['host_ports'][0]
            dest = (kernel_host, port)
            break
    else:
        raise AppNotFound(f'{sess_id}:{service}')

    log.info('STREAM_WSPROXY (ak:{}, s:{}): tunneling {}:{} to {}',
             access_key, sess_id, service, sport['protocol'], '{}:{}'.format(*dest))
    if sport['protocol'] == 'tcp':
        proxy_cls = TCPProxy
    elif sport['protocol'] == 'pty':
        raise NotImplementedError
        # proxy_cls = TermProxy
    elif sport['protocol'] == 'http':
        proxy_cls = TCPProxy
        # proxy_cls = HTTPProxy
    else:
        raise InvalidAPIParameters(
            f"Unsupported service protocol: {sport['protocol']}")
    # TODO: apply a (distributed) semaphore to limit concurrency per user.
    await asyncio.shield(registry.increment_session_usage(sess_id, access_key))

    async def refresh_cb(kernel_id: str, data: bytes):
        await asyncio.shield(call_non_bursty(
            kernel_id,
            apartial(registry.refresh_session, sess_id, access_key),
            max_bursts=64, max_idle=2000))

    down_cb = apartial(refresh_cb, kernel.id)
    up_cb = apartial(refresh_cb, kernel.id)
    ping_cb = apartial(refresh_cb, kernel.id)

    try:
        opts: Mapping[str, Any] = {}
        result = await asyncio.shield(
            registry.start_service(sess_id, access_key, service, opts))
        if result['status'] == 'failed':
            raise InternalServerError(
                "Failed to launch the app service",
                extra_data=result['error'])

        # TODO: weakref to proxies for graceful shutdown?
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)
        proxy = proxy_cls(ws, dest[0], dest[1],
                          downstream_callback=down_cb,
                          upstream_callback=up_cb,
                          ping_callback=ping_cb)
        return await proxy.proxy()
    except asyncio.CancelledError:
        log.debug('stream_proxy({}, {}) cancelled', stream_key, service)
        raise


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        t.Key('sessionId', default='*') >> 'session_id': t.String,
        t.Key('ownerAccessKey', default=None) >> 'owner_access_key': t.Null | t.String,
        tx.AliasedKey(['group', 'groupName'], default='*') >> 'group_name': t.String,
    }))
@adefer
async def stream_events(defer, request: web.Request, params: Mapping[str, Any]) -> web.StreamResponse:
    app = request.app
    session_id = params['session_id']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']
    access_key = params['owner_access_key']
    if access_key is None:
        access_key = request['keypair']['access_key']
    if user_role == UserRole.USER:
        if access_key != request['keypair']['access_key']:
            raise GenericForbidden
    group_name = params['group_name']
    event_queues = app['event_queues']  # type: Set[asyncio.Queue]
    my_queue = asyncio.Queue()          # type: asyncio.Queue[Tuple[str, dict, str]]
    log.info('STREAM_EVENTS (ak:{}, s:{}, g:{})', access_key, session_id, group_name)
    if group_name == '*':
        group_id = '*'
    else:
        async with app['dbpool'].acquire() as conn, conn.begin():
            query = (
                sa.select([groups.c.id]).select_from(groups)
                .where(groups.c.name == group_name)
            )
            row = await conn.first(query)
            if row is None:
                raise GroupNotFound
            group_id = row['id']
    event_queues.add(my_queue)
    defer(lambda: event_queues.remove(my_queue))
    try:
        async with sse_response(request) as resp:
            while True:
                evdata = await my_queue.get()
                if evdata is sentinel:
                    break
                event_name, row, reason = evdata
                if user_role in (UserRole.USER, UserRole.ADMIN):
                    if row['domain_name'] != request['user']['domain_name']:
                        continue
                if user_role == UserRole.USER:
                    if row['user_uuid'] != user_uuid:
                        continue
                if group_id != '*' and row['group_id'] != group_id:
                    continue
                if session_id != '*' and not (
                        (row['sess_id'] == session_id) and
                        (row['access_key'] == access_key)):
                    continue
                await resp.send(json.dumps({
                    'sessionId': str(row['sess_id']),
                    'ownerAccessKey': row['access_key'],
                    'reason': reason,
                }), event=event_name)
    finally:
        return resp


async def kernel_terminated(app: web.Application, agent_id: AgentId, event_name: str,
                            kernel_id: KernelId, reason: str,
                            exit_code: int = None) -> None:
    try:
        kernel = await app['registry'].get_kernel(
            kernel_id, (kernels.c.role, kernels.c.status), allow_stale=True)
    except KernelNotFound:
        return
    if kernel.role == 'master':
        sess_id = kernel['sess_id']
        stream_key = (sess_id, kernel['access_key'])
        cancelled_tasks = []
        for sock in app['stream_stdin_socks'][sess_id]:
            sock.close()
        for handler in list(app['stream_pty_handlers'].get(stream_key, [])):
            handler.cancel()
            cancelled_tasks.append(handler)
        for handler in list(app['stream_execute_handlers'].get(stream_key, [])):
            handler.cancel()
            cancelled_tasks.append(handler)
        for handler in list(app['stream_proxy_handlers'].get(stream_key, [])):
            handler.cancel()
            cancelled_tasks.append(handler)
        await asyncio.gather(*cancelled_tasks, return_exceptions=True)
        # TODO: reconnect if restarting?


async def enqueue_status_update(app: web.Application, agent_id: AgentId, event_name: str,
                                raw_kernel_id: str,
                                reason: str = None,
                                exit_code: int = None) -> None:
    if raw_kernel_id is None:
        return
    kernel_id = uuid.UUID(raw_kernel_id)
    # TODO: when event_name == 'kernel_started', read the service port data.
    async with app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([
                kernels.c.role,
                kernels.c.sess_id,
                kernels.c.access_key,
                kernels.c.domain_name,
                kernels.c.group_id,
                kernels.c.user_uuid,
            ])
            .select_from(kernels)
            .where(
                (kernels.c.id == kernel_id)
            )
        )
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            return
        if row['role'] != 'master':
            return
    for q in app['event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_result_update(app: web.Application, agent_id: AgentId, event_name: str,
                                raw_kernel_id: str,
                                exit_code: int = None) -> None:
    kernel_id = uuid.UUID(raw_kernel_id)
    # TODO: when event_name == 'kernel_started', read the service port data.
    async with app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([
                kernels.c.role,
                kernels.c.sess_id,
                kernels.c.access_key,
                kernels.c.domain_name,
                kernels.c.group_id,
                kernels.c.user_uuid,
            ])
            .select_from(kernels)
            .where(
                (kernels.c.id == kernel_id)
            )
        )
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            return
        if row['role'] != 'master':
            return
    if event_name == 'kernel_success':
        reason = 'task-success'
    else:
        reason = 'task-failure'
    for q in app['event_queues']:
        q.put_nowait((event_name, row, reason))


async def init(app: web.Application) -> None:
    app['stream_pty_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_execute_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_proxy_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_stdin_socks'] = defaultdict(weakref.WeakSet)
    app['event_queues'] = set()
    event_dispatcher = app['event_dispatcher']
    event_dispatcher.subscribe('kernel_terminated', app, kernel_terminated)
    event_dispatcher.subscribe('kernel_enqueued', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_preparing', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_pulling', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_creating', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_started', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_terminating', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_terminated', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_cancelled', app, enqueue_status_update)
    event_dispatcher.subscribe('kernel_success', app, enqueue_result_update)
    event_dispatcher.subscribe('kernel_failure', app, enqueue_result_update)


async def shutdown(app: web.Application) -> None:
    cancelled_tasks: List[asyncio.Task] = []
    for per_kernel_handlers in app['stream_pty_handlers'].values():
        for handler in list(per_kernel_handlers):
            if not handler.done():
                handler.cancel()
                cancelled_tasks.append(handler)
    for per_kernel_handlers in app['stream_execute_handlers'].values():
        for handler in list(per_kernel_handlers):
            if not handler.done():
                handler.cancel()
                cancelled_tasks.append(handler)
    for per_kernel_handlers in app['stream_proxy_handlers'].values():
        for handler in list(per_kernel_handlers):
            if not handler.done():
                handler.cancel()
                cancelled_tasks.append(handler)
    for q in app['event_queues']:
        q.put_nowait(sentinel)
    await asyncio.gather(*cancelled_tasks, return_exceptions=True)


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'stream'
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', r'/kernel/_/events', stream_events))
    cors.add(add_route('GET', r'/kernel/{sess_id}/pty', stream_pty))
    cors.add(add_route('GET', r'/kernel/{sess_id}/execute', stream_execute))
    # internally both tcp/http proxies use websockets as API/agent-level transports,
    # and thus they have the same implementation here.
    cors.add(add_route('GET', r'/kernel/{sess_id}/httpproxy', stream_proxy))
    cors.add(add_route('GET', r'/kernel/{sess_id}/tcpproxy', stream_proxy))
    return app, []
