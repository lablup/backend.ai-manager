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
    Any, Iterable, AsyncIterator,
    Mapping,
    MutableMapping,
    List, Tuple,
    Set, Union,
)
import uuid
from urllib.parse import urlparse
import weakref

import aiohttp
from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response
from aiotools import apartial
import sqlalchemy as sa
import trafaret as t
import zmq, zmq.asyncio

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId,
    KernelId,
)

from .auth import auth_required
from .exceptions import (
    AppNotFound, GroupNotFound, SessionNotFound,
    BackendError,
    InvalidAPIParameters, GenericForbidden,
    InternalServerError,
)
from .manager import READ_ALLOWED, server_status_required
from .typing import CORSOptions, WebMiddleware
from .utils import check_api_params, call_non_bursty
from .wsproxy import TCPProxy
from ..manager.models import kernels, groups, UserRole

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))

sentinel = object()


@server_status_required(READ_ALLOWED)
@auth_required
async def stream_pty(request: web.Request) -> web.StreamResponse:
    app = request.app
    registry = app['registry']
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    stream_key = (session_name, access_key)
    extra_fields = (kernels.c.stdin_port, kernels.c.stdout_port)
    api_version = request['api_version']
    try:
        compute_session = await asyncio.shield(
            registry.get_session(session_name, access_key, field=extra_fields))
    except SessionNotFound:
        raise
    log.info('STREAM_PTY(ak:{0}, s:{1})', access_key, session_name)

    await asyncio.shield(registry.increment_session_usage(session_name, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_pty_handlers'][stream_key].add(myself)

    async def connect_streams(compute_session) -> Tuple[zmq.asyncio.Socket, zmq.asyncio.Socket]:
        # TODO: refactor as custom row/table method
        if compute_session.kernel_host is None:
            kernel_host = urlparse(compute_session.agent_addr).hostname
        else:
            kernel_host = compute_session.kernel_host
        stdin_addr = f'tcp://{kernel_host}:{compute_session.stdin_port}'
        log.debug('stream_pty({0}): stdin: {1}', stream_key, stdin_addr)
        stdin_sock = await app['zctx'].socket(zmq.PUB)
        stdin_sock.connect(stdin_addr)
        stdin_sock.setsockopt(zmq.LINGER, 100)
        stdout_addr = f'tcp://{kernel_host}:{compute_session.stdout_port}'
        log.debug('stream_pty({0}): stdout: {1}', stream_key, stdout_addr)
        stdout_sock = await app['zctx'].socket(zmq.SUB)
        stdout_sock.connect(stdout_addr)
        stdout_sock.setsockopt(zmq.LINGER, 100)
        stdout_sock.subscribe(b'')
        return stdin_sock, stdout_sock

    # Wrap sockets in a list so that below coroutines can share reference changes.
    socks = list(await connect_streams(compute_session))
    app['stream_stdin_socks'][stream_key].add(socks[0])
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
                            await socks[0].send_mlutipart([raw_data])
                        except (RuntimeError, zmq.error.ZMQError):
                            # when socks[0] is closed, re-initiate the connection.
                            app['stream_stdin_socks'][stream_key].discard(socks[0])
                            socks[1].close()
                            kernel = await asyncio.shield(registry.get_session(
                                session_name, access_key, field=extra_fields))
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
                            registry.increment_session_usage(session_name, access_key))
                        run_id = secrets.token_hex(8)
                        if data['type'] == 'resize':
                            code = f"%resize {data['rows']} {data['cols']}"
                            await registry.execute(
                                session_name, access_key,
                                api_version, run_id, 'query', code, {},
                                flush_timeout=None)
                        elif data['type'] == 'ping':
                            await registry.execute(
                                session_name, access_key,
                                api_version, run_id, 'query', '%ping', {},
                                flush_timeout=None)
                        elif data['type'] == 'restart':
                            # Close existing zmq sockets and let stream
                            # handlers get a new one with changed stdin/stdout
                            # ports.
                            log.debug('stream_stdin: restart requested')
                            if not socks[0].closed:
                                await asyncio.shield(
                                    registry.restart_session(session_name, access_key))
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
            if not socks[0].closed:
                socks[0].close()

    async def stream_stdout():
        nonlocal socks
        log.debug('stream_stdout({0}): started', stream_key)
        try:
            while True:
                try:
                    data = await socks[1].recv_multipart()
                except (asyncio.CancelledError, zmq.error.ZMQError):
                    if socks[0] not in app['stream_stdin_socks']:
                        # we are terminating
                        return
                    # connection is closed, so wait until stream_stdin() recovers it.
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
            pass
        except:
            app['error_monitor'].capture_exception()
            log.exception('stream_stdout({0}): unexpected error', stream_key)
        finally:
            log.debug('stream_stdout({0}): terminated', stream_key)
            socks[1].close()

    # According to aiohttp docs, reading ws must be done inside this task.
    # We execute the stdout handler as another task.
    try:
        stdout_task = asyncio.create_task(stream_stdout())
        await stream_stdin()
    except Exception:
        app['error_monitor'].capture_exception()
        log.exception('stream_pty({0}): unexpected error', stream_key)
    finally:
        app['stream_pty_handlers'][stream_key].discard(myself)
        app['stream_stdin_socks'][stream_key].discard(socks[0])
        stdout_task.cancel()
        await stdout_task
    return ws


@server_status_required(READ_ALLOWED)
@auth_required
async def stream_execute(request: web.Request) -> web.StreamResponse:
    '''
    WebSocket-version of gateway.kernel.execute().
    '''
    app = request.app
    registry = app['registry']
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    stream_key = (session_name, access_key)
    api_version = request['api_version']
    log.info('STREAM_EXECUTE(ak:{0}, s:{1})', access_key, session_name)
    try:
        _ = await asyncio.shield(registry.get_session(session_name, access_key))  # noqa
    except SessionNotFound:
        raise

    await asyncio.shield(registry.increment_session_usage(session_name, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_execute_handlers'][stream_key].add(myself)

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
                session_name, access_key,
                api_version, run_id, mode, code, opts,
                flush_timeout=0.2)
            if ws.closed:
                log.debug('STREAM_EXECUTE: client disconnected (interrupted)')
                await asyncio.shield(registry.interrupt_session(session_name, access_key))
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
        app['stream_execute_handlers'][stream_key].discard(myself)
        return ws


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['app', 'service']): t.String,
        # The port argument is only required to use secondary ports
        # when the target app listens multiple TCP ports.
        # Otherwise it should be omitted or set to the same value of
        # the actual port number used by the app.
        tx.AliasedKey(['port'], default=None): t.Null | t.Int[1024:65535],
        tx.AliasedKey(['envs'], default=None): t.Null | t.String,  # stringified JSON
                                                                   # e.g., '{"PASSWORD": "12345"}'
        tx.AliasedKey(['arguments'], default=None): t.Null | t.String,  # stringified JSON
                                                                        # e.g., '{"-P": "12345"}'
                                                                        # The value can be one of:
                                                                        # None, str, List[str]
    }))
async def stream_proxy(request: web.Request, params: Mapping[str, Any]) -> web.StreamResponse:
    registry = request.app['registry']
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    service = params['app']

    stream_key = (session_name, access_key)
    myself = asyncio.Task.current_task()
    request.app['stream_proxy_handlers'][stream_key].add(myself)

    try:
        kernel = await asyncio.shield(registry.get_session(session_name, access_key))
    except SessionNotFound:
        raise
    if kernel.kernel_host is None:
        kernel_host = urlparse(kernel.agent_addr).hostname
    else:
        kernel_host = kernel.kernel_host
    for sport in kernel.service_ports:
        if sport['name'] == service:
            if params['port']:
                # using one of the primary/secondary ports of the app
                try:
                    hport_idx = sport['container_ports'].index(params['port'])
                except ValueError:
                    raise InvalidAPIParameters(
                        f"Service {service} does not open the port number {params['port']}.")
                host_port = sport['host_ports'][hport_idx]
            else:
                # using the default (primary) port of the app
                if 'host_ports' not in sport:
                    host_port = sport['host_port']  # legacy kernels
                else:
                    host_port = sport['host_ports'][0]
            dest = (kernel_host, host_port)
            break
    else:
        raise AppNotFound(f'{session_name}:{service}')

    log.info('STREAM_WSPROXY (ak:{}, s:{}): tunneling {}:{} to {}',
             access_key, session_name, service, sport['protocol'], '{}:{}'.format(*dest))
    if sport['protocol'] == 'tcp':
        proxy_cls = TCPProxy
    elif sport['protocol'] == 'pty':
        raise NotImplementedError
    elif sport['protocol'] == 'http':
        proxy_cls = TCPProxy
    elif sport['protocol'] == 'preopen':
        proxy_cls = TCPProxy
    else:
        raise InvalidAPIParameters(
            f"Unsupported service protocol: {sport['protocol']}")
    # TODO: apply a (distributed) semaphore to limit concurrency per user.
    await asyncio.shield(registry.increment_session_usage(session_name, access_key))

    async def refresh_cb(kernel_id: str, data: bytes):
        await asyncio.shield(call_non_bursty(
            kernel_id,
            apartial(registry.refresh_session, session_name, access_key),
            max_bursts=64, max_idle=2000))

    down_cb = apartial(refresh_cb, kernel.id)
    up_cb = apartial(refresh_cb, kernel.id)
    ping_cb = apartial(refresh_cb, kernel.id)

    try:
        opts: MutableMapping[str, Union[None, str, List[str]]] = {}
        if params['arguments'] is not None:
            opts['arguments'] = json.loads(params['arguments'])
        if params['envs'] is not None:
            opts['envs'] = json.loads(params['envs'])

        result = await asyncio.shield(
            registry.start_service(session_name, access_key, service, opts))
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
    finally:
        request.app['stream_proxy_handlers'][stream_key].discard(myself)


@server_status_required(READ_ALLOWED)
@auth_required
async def get_stream_apps(request: web.Request) -> web.Response:
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    resp = []

    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([
                kernels.c.service_ports,
            ])
            .select_from(kernels)
            .where(
                (kernels.c.sess_id == session_name) &
                (kernels.c.access_key == access_key)
            )
        )
        result = await conn.execute(query)
        row = await result.first()
        for item in row['service_ports']:
            response_dict = {
                'name': item['name'],
                'protocol': item['protocol'],
                'ports': item['container_ports'],
            }
            if 'url_template' in item.keys():
                response_dict['url_template'] = item['url_template']
            if 'allowed_arguments' in item.keys():
                response_dict['allowed_arguments'] = item['allowed_arguments']
            if 'allowed_envs' in item.keys():
                response_dict['allowed_envs'] = item['allowed_envs']
            resp.append(response_dict)

    return web.json_response(resp)


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['name', 'sessionName'], default='*') >> 'session_name': t.String,
        t.Key('ownerAccessKey', default=None) >> 'owner_access_key': t.Null | t.String,
        tx.AliasedKey(['group', 'groupName'], default='*') >> 'group_name': t.String,
    }))
async def stream_events(request: web.Request, params: Mapping[str, Any]) -> web.StreamResponse:
    app = request.app
    session_name = params['session_name']
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
    log.info('STREAM_EVENTS (ak:{}, s:{}, g:{})', access_key, session_name, group_name)
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
                if session_name != '*' and not (
                        (row['sess_id'] == session_name) and
                        (row['access_key'] == access_key)):
                    continue
                await resp.send(json.dumps({
                    'sessionName': str(row['sess_id']),
                    'ownerAccessKey': row['access_key'],
                    'reason': reason,
                }), event=event_name)
    finally:
        event_queues.remove(my_queue)
        return resp


async def kernel_terminated(app: web.Application, agent_id: AgentId, event_name: str,
                            kernel_id: KernelId, reason: str,
                            exit_code: int = None) -> None:
    try:
        kernel = await app['registry'].get_kernel(
            kernel_id, (kernels.c.role, kernels.c.status), allow_stale=True)
    except SessionNotFound:
        return
    if kernel.role == 'master':
        session_name = kernel['sess_id']
        stream_key = (session_name, kernel['access_key'])
        cancelled_tasks = []
        for sock in app['stream_stdin_socks'][session_name]:
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


async def stream_app_ctx(app: web.Application) -> AsyncIterator[None]:
    app['stream_pty_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_execute_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_proxy_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_stdin_socks'] = defaultdict(weakref.WeakSet)
    app['zctx'] = zmq.asyncio.Context()
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

    yield

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
    app['zctx'].term()


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.cleanup_ctx.append(stream_app_ctx)
    app['prefix'] = 'stream'
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', r'/session/_/events', stream_events))
    cors.add(add_route('GET', r'/session/{session_name}/pty', stream_pty))
    cors.add(add_route('GET', r'/session/{session_name}/execute', stream_execute))
    cors.add(add_route('GET', r'/session/{session_name}/apps', get_stream_apps))
    # internally both tcp/http proxies use websockets as API/agent-level transports,
    # and thus they have the same implementation here.
    cors.add(add_route('GET', r'/session/{session_name}/httpproxy', stream_proxy))
    cors.add(add_route('GET', r'/session/{session_name}/tcpproxy', stream_proxy))
    return app, []
