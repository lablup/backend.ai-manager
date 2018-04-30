'''
WebSocket-based streaming kernel interaction APIs.
'''

import asyncio
import base64
from collections import defaultdict
import json
import logging
import secrets
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
import aiozmq
from aiozmq import create_zmq_stream as aiozmq_sock
import zmq

from .auth import auth_required
from .exceptions import KernelNotFound
from .utils import not_impl_stub
from ..manager.models import kernels

log = logging.getLogger('ai.backend.gateway.stream')


@auth_required
async def stream_pty(request) -> web.Response:
    app = request.app
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)
    extra_fields = (kernels.c.stdin_port, kernels.c.stdout_port)
    try:
        kernel = await registry.get_session(
            sess_id, access_key, field=extra_fields)
    except KernelNotFound:
        raise

    await registry.increment_session_usage(sess_id, access_key)

    # Upgrade connection to WebSocket.
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    app['stream_pty_handlers'][stream_key].add(asyncio.Task.current_task())

    async def connect_streams(kernel):
        # TODO: refactor as custom row/table method
        if kernel.kernel_host is None:
            kernel_host = urlparse(kernel.agent_addr).hostname
        else:
            kernel_host = kernel.kernel_host
        stdin_addr = f'tcp://{kernel_host}:{kernel.stdin_port}'
        log.debug(f'stream_pty({stream_key}): stdin: {stdin_addr}')
        stdin_sock = await aiozmq_sock(zmq.PUB, connect=stdin_addr)
        stdin_sock.transport.setsockopt(zmq.LINGER, 100)
        stdout_addr = f'tcp://{kernel_host}:{kernel.stdout_port}'
        log.debug(f'stream_pty({stream_key}): stdout: {stdout_addr}')
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
                            app['stream_stdin_socks'][stream_key].remove(socks[0])
                            socks[1].close()
                            kernel = await registry.get_session(
                                sess_id, access_key, field=extra_fields)
                            stdin_sock, stdout_sock = await connect_streams(kernel)
                            socks[0] = stdin_sock
                            socks[1] = stdout_sock
                            app['stream_stdin_socks'][stream_key].add(socks[0])
                            socks[0].write([raw_data])
                            log.debug(f'stream_stdin({stream_key}): '
                                      'zmq stream reset')
                            stream_sync.set()
                            continue
                    else:
                        await registry.increment_session_usage(sess_id, access_key)
                        api_version = 2
                        run_id = secrets.token_hex(8)
                        if data['type'] == 'resize':
                            code = f"%resize {data['rows']} {data['cols']}"
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', code, {})
                        elif data['type'] == 'ping':
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', '%ping', {})
                        elif data['type'] == 'restart':
                            # Close existing zmq sockets and let stream
                            # handlers get a new one with changed stdin/stdout
                            # ports.
                            log.debug('stream_stdin: restart requested')
                            if not socks[0].at_closing():
                                await registry.restart_session(sess_id, access_key)
                                socks[0].close()
                            else:
                                log.warning(f'stream_stdin({stream_key}): '
                                            'duplicate kernel restart request; '
                                            'ignoring it.')
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.warning(f'stream_stdin({stream_key}): '
                                f'connection closed ({ws.exception()})')
        except asyncio.CancelledError:
            # Agent or kernel is terminated.
            pass
        except:
            app['sentry'].captureException()
            log.exception(f'stream_stdin({stream_key}): unexpected error')
        finally:
            log.debug(f'stream_stdin({stream_key}): terminated')
            if not socks[0].at_closing():
                socks[0].close()

    async def stream_stdout():
        nonlocal socks
        log.debug(f'stream_stdout({stream_key}): started')
        try:
            while True:
                try:
                    data = await socks[1].read()
                except aiozmq.ZmqStreamClosed:
                    await stream_sync.wait()
                    stream_sync.clear()
                    log.debug(f'stream_stdout({stream_key}): zmq stream reset')
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
            app['sentry'].captureException()
            log.exception(f'stream_stdout({stream_key}): unexpected error')
        finally:
            log.debug(f'stream_stdout({stream_key}): terminated')
            socks[1].close()

    # According to aiohttp docs, reading ws must be done inside this task.
    # We execute the stdout handler as another task.
    try:
        stdout_task = asyncio.ensure_future(stream_stdout())
        await stream_stdin()
    except:
        app['sentry'].captureException()
        log.exception(f'stream_pty({stream_key}): unexpected error')
    finally:
        app['stream_pty_handlers'][stream_key].remove(asyncio.Task.current_task())
        app['stream_stdin_socks'][stream_key].remove(socks[0])
        stdout_task.cancel()
        await stdout_task
    return ws


async def kernel_terminated(app, agent_id, kernel_id, reason, kern_stat):
    try:
        kernel = await app['registry'].get_kernel(
            kernel_id, (kernels.c.role, kernels.c.status), allow_stale=True)
    except KernelNotFound:
        return
    if kernel.role == 'master':
        sess_id = kernel['sess_id']
        stream_key = (sess_id, kernel['access_key'])
        for sock in app['stream_stdin_socks'][sess_id]:
            sock.close()
        for handler in app['stream_pty_handlers'][stream_key].copy():
            handler.cancel()
            await handler
        # TODO: reconnect if restarting?


async def init(app):
    app['stream_pty_handlers'] = defaultdict(set)
    app['stream_stdin_socks'] = defaultdict(set)
    app['event_dispatcher'].add_handler('kernel_terminated', app, kernel_terminated)


async def shutdown(app):
    for per_kernel_handlers in app['stream_pty_handlers'].values():
        for handler in per_kernel_handlers.copy():
            handler.cancel()
            await handler


def create_app():
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'stream'
    app['api_versions'] = (2, 3)
    app.router.add_route('GET', r'/kernel/{sess_id}/pty', stream_pty)
    app.router.add_route('GET', r'/kernel/{sess_id}/events', not_impl_stub)
    return app, []
