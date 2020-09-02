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
    Union,
)
from urllib.parse import urlparse
import weakref

import aiohttp
from aiohttp import web
import aiohttp_cors
from aiotools import apartial, adefer
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
    AppNotFound, SessionNotFound,
    BackendError,
    InvalidAPIParameters,
    InternalServerError,
)
from .manager import READ_ALLOWED, server_status_required
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params, call_non_bursty
from .wsproxy import TCPProxy
from ..manager.models import kernels

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))


@server_status_required(READ_ALLOWED)
@auth_required
@adefer
async def stream_pty(defer, request: web.Request) -> web.StreamResponse:
    app = request.app
    config = app['config']
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
    ws = web.WebSocketResponse(max_msg_size=config['manager']['max-wsmsg-size'])
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_pty_handlers'][stream_key].add(myself)
    defer(lambda: app['stream_pty_handlers'][stream_key].discard(myself))

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
            await app['error_monitor'].capture_exception(context={'user': request['user']['uuid']})
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
            await app['error_monitor'].capture_exception(context={'user': request['user']['uuid']})
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
        await app['error_monitor'].capture_exception(context={'user': request['user']['uuid']})
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
    config = app['config']
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
    ws = web.WebSocketResponse(max_msg_size=config['manager']['max-wsmsg-size'])
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
@adefer
async def stream_proxy(defer, request: web.Request, params: Mapping[str, Any]) -> web.StreamResponse:
    registry = request.app['registry']
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    service = params['app']
    config = request.app['config']

    stream_key = (session_name, access_key)
    myself = asyncio.Task.current_task()
    request.app['stream_proxy_handlers'][stream_key].add(myself)
    defer(lambda: request.app['stream_proxy_handlers'][stream_key].discard(myself))

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
        ws = web.WebSocketResponse(autoping=False, max_msg_size=config['manager']['max-wsmsg-size'])
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
async def get_stream_apps(request: web.Request) -> web.Response:
    session_name = request.match_info['session_name']
    access_key = request['keypair']['access_key']
    compute_session = await request.app['registry'].get_session(session_name, access_key)
    if compute_session['service_ports'] is None:
        return web.json_response([])
    resp = []
    for item in compute_session['service_ports']:
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


async def stream_app_ctx(app: web.Application) -> AsyncIterator[None]:
    app['stream_pty_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_execute_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_proxy_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_stdin_socks'] = defaultdict(weakref.WeakSet)
    app['zctx'] = zmq.asyncio.Context()

    event_dispatcher = app['event_dispatcher']
    event_dispatcher.subscribe('kernel_terminated', app, kernel_terminated)

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
    await asyncio.gather(*cancelled_tasks, return_exceptions=True)
    app['zctx'].term()


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.cleanup_ctx.append(stream_app_ctx)
    app['prefix'] = 'stream'
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', r'/session/{session_name}/pty', stream_pty))
    cors.add(add_route('GET', r'/session/{session_name}/execute', stream_execute))
    cors.add(add_route('GET', r'/session/{session_name}/apps', get_stream_apps))
    # internally both tcp/http proxies use websockets as API/agent-level transports,
    # and thus they have the same implementation here.
    cors.add(add_route('GET', r'/session/{session_name}/httpproxy', stream_proxy))
    cors.add(add_route('GET', r'/session/{session_name}/tcpproxy', stream_proxy))
    return app, []
