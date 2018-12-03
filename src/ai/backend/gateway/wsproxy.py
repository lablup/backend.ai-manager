'''
WebSocket-based streaming kernel interaction APIs.
'''

import asyncio
import logging

import aiohttp
from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.wsproxy'))


class WebSocketProxy():
    __slots__ = (
        'path', 'conn', 'down_conn',
        'upstream_buffer', 'upstream_buffer_task',
    )

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
                    log.info("ws connection closed with exception {}",
                             self.conn.exception())
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
