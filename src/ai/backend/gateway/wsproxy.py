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
import weakref

import aiohttp
from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .exceptions import KernelNotFound
from .utils import not_impl_stub
from ..manager.models import kernels

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.wsproxy'))


class WebSocketProxy():
    __slots__ = ['path', 'conn', 'down_conn', 'upstream_buffer', 'upstream_buffer_task']

    def __init__(self, path, ws: web.WebSocketResponse):
        super(WebSocketProxy, self).__init__()
        self.path = path
        self.upstream_buffer = asyncio.PriorityQueue()
        self.down_conn = ws
        self.conn = None
        self.upstream_buffer_task = None

    async def proxy(self):
        asyncio.ensure_future(self.downstream())
        await self.upstream()

    async def upstream(self):
        try:
            async for msg in self.down_conn:
                if msg.type in (web.WSMsgType.TEXT, web.WSMsgType.binary):
                    await self.write(msg.data, msg.type)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print_info('ws connection closed"\
                            " with exception %s' % self.conn.exception())
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break
        finally:
            await self.close()

    async def downstream(self):
        path = self.path
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(path) as ws:
                    self.conn = ws
                    self.upstream_buffer_task = \
                            asyncio.ensure_future(self.consume_upstream_buffer())
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self.down_conn.send_str(msg.data)
                        if msg.type == aiohttp.WSMsgType.binary:
                            await self.down_conn.send_bytes(msg.data)
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
        except Exception as e:
            print(e)
        finally:
            await self.close()

    async def consume_upstream_buffer(self):
        while True:
            msg, tp = await self.upstream_buffer.get()
            if self.conn:
                if tp == aiohttp.WSMsgType.TEXT:
                    await self.conn.send_str(msg)
                elif tp == aiohttp.WSMsgType.binary:
                    await self.conn.send_bytes(msg)
            else:
                await self.close()

    async def write(self, msg: str, tp):
        await self.upstream_buffer.put((msg, tp))

    async def close(self):
        if self.upstream_buffer_task:
            self.upstream_buffer_task.cancel()
        if self.conn:
            await self.conn.close()
        if not self.down_conn.closed:
            await self.down_conn.close()
        self.conn = None


@auth_required
async def ws_proxy(request) -> web.Response:

    app = request.app
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)

    extra_fields = (kernels.c.stdin_port, )
    log.info(f'{sess_id}')
    try:
        kernel = await registry.get_session(
            sess_id, access_key, field=extra_fields)
    except KernelNotFound:
        raise
    await registry.increment_session_usage(sess_id, access_key)

    if kernel.kernel_host is None:
        kernel_host = urlparse(kernel.agent_addr).hostname
    else:
        kernel_host = kernel.kernel_host
    path = f'ws://{kernel_host}:{kernel.stdin_port}/'

    log.info(f'WS: {sess_id} -> {path}')
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    web_socket_proxy = WebSocketProxy(path, ws)
    await web_socket_proxy.proxy()
    return ws


async def kernel_terminated(app, agent_id, kernel_id, reason, kern_stat):
    pass


async def init(app):
    pass


async def shutdown(app):
    pass


def create_app():
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'wsproxy'
    app['api_versions'] = (2, 3, 4)
    app.router.add_route('GET', r'/{sess_id}/stream', ws_proxy)
    return app, []
