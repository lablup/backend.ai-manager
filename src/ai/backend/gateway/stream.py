'''
WebSocket-based streaming kernel interaction APIs.

NOTE: For nginx-based setups, we need to gather all websocket-based API handlers
      under this "/stream/"-prefixed app.
'''

import asyncio
import base64
from collections import defaultdict
from functools import partial
import json
import logging
import secrets
from urllib.parse import urlparse
import weakref

import aiohttp
import aiohttp_cors
from aiohttp import web
import aiozmq
from aiozmq import create_zmq_stream as aiozmq_sock
import zmq

from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .exceptions import (
    BackendError, AppNotFound, KernelNotFound, InvalidAPIParameters,
    InternalServerError,
)
from .manager import READ_ALLOWED, server_status_required
from .utils import not_impl_stub, call_non_bursty
from .wsproxy import TCPProxy
from ..manager.models import kernels

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))


@server_status_required(READ_ALLOWED)
@auth_required
async def stream_pty(request) -> web.Response:
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

    await asyncio.shield(registry.increment_session_usage(sess_id, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_pty_handlers'][stream_key].add(myself)

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
        app['stream_pty_handlers'][stream_key].discard(myself)
        app['stream_stdin_socks'][stream_key].discard(socks[0])
        stdout_task.cancel()
        await stdout_task
    return ws


@server_status_required(READ_ALLOWED)
@auth_required
async def stream_execute(request) -> web.Response:
    '''
    WebSocket-version of gateway.kernel.execute().
    '''
    app = request.app
    registry = app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)
    api_version = request['api_version']
    log.info('STREAM_EXECUTE(u:{0}, k:{1})', access_key, sess_id)
    try:
        _ = await asyncio.shield(registry.get_session(sess_id, access_key))  # noqa
    except KernelNotFound:
        raise

    await asyncio.shield(registry.increment_session_usage(sess_id, access_key))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    myself = asyncio.Task.current_task()
    app['stream_execute_handlers'][stream_key].add(myself)

    # This websocket connection itself is a "run".
    run_id = secrets.token_hex(8)

    try:
        if ws.closed:
            log.warning('STREAM_EXECUTE: client disconnected (cancelled)')
            return
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
                log.warning('STREAM_EXECUTE: client disconnected (interrupted)')
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
        app['stream_execute_handlers'][stream_key].discard(myself)
        return ws


@server_status_required(READ_ALLOWED)
@auth_required
async def stream_proxy(request) -> web.Response:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    service = request.query.get('app', None)  # noqa

    stream_key = (sess_id, access_key)
    myself = asyncio.Task.current_task()
    request.app['stream_proxy_handlers'][stream_key].add(myself)

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
            dest = (kernel_host, sport['host_port'])
            break
    else:
        raise AppNotFound(f'{sess_id}:{service}')

    log.info('STREAM_WSPROXY: {0} ==[{1}:{2}]==> {3}',
             sess_id, service, sport['protocol'], dest)
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
        await asyncio.shield(
            call_non_bursty(registry.refresh_session(sess_id, access_key)))

    down_cb = partial(refresh_cb, kernel.id)
    up_cb = partial(refresh_cb, kernel.id)
    ping_cb = partial(refresh_cb, kernel.id)

    try:
        opts = {}
        result = await asyncio.shield(
            registry.start_service(sess_id, access_key, service, opts))
        if result['status'] == 'failed':
            msg = f"Failed to launch the app service: {result['error']}"
            raise InternalServerError(msg)

        # TODO: weakref to proxies for graceful shutdown?
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)
        proxy = proxy_cls(ws, dest[0], dest[1],
                          downstream_callback=down_cb,
                          upstream_callback=up_cb,
                          ping_callback=ping_cb)
        return await proxy.proxy()
    except asyncio.CancelledError:
        log.warning('stream_proxy({}, {}) cancelled', stream_key, service)
        raise
    finally:
        request.app['stream_proxy_handlers'][stream_key].discard(myself)


async def kernel_terminated(app, agent_id, kernel_id, reason, kern_stat):
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
        await asyncio.gather(*cancelled_tasks)
        # TODO: reconnect if restarting?


async def init(app):
    app['stream_pty_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_execute_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_proxy_handlers'] = defaultdict(weakref.WeakSet)
    app['stream_stdin_socks'] = defaultdict(weakref.WeakSet)
    app['event_dispatcher'].add_handler('kernel_terminated', app, kernel_terminated)


async def shutdown(app):
    for per_kernel_handlers in app['stream_pty_handlers'].values():
        for handler in list(per_kernel_handlers):
            if not handler.done():
                handler.cancel()
                await handler


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'stream'
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', r'/kernel/{sess_id}/pty', stream_pty))
    cors.add(add_route('GET', r'/kernel/{sess_id}/execute', stream_execute))
    # internally both tcp/http proxies use websockets as API/agent-level transports,
    # and thus they have the same implementation here.
    cors.add(add_route('GET', r'/kernel/{sess_id}/httpproxy', stream_proxy))
    cors.add(add_route('GET', r'/kernel/{sess_id}/tcpproxy', stream_proxy))
    cors.add(add_route('GET', r'/kernel/{sess_id}/events', not_impl_stub))
    return app, []
