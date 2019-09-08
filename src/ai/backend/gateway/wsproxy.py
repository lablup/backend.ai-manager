'''
WebSocket-based streaming kernel interaction APIs.
'''

from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import (
    Optional, Union,
    Awaitable,
)

import aiohttp
from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.wsproxy'))


class ServiceProxy(metaclass=ABCMeta):
    '''
    The abstract base class to implement service proxy handlers.
    '''

    __slots__ = (
        'ws', 'host', 'port',
        'downstream_cb', 'upstream_cb', 'ping_cb',
    )

    def __init__(self, down_ws, dest_host, dest_port, *,
                 downstream_callback: Awaitable = None,
                 upstream_callback: Awaitable = None,
                 ping_callback: Awaitable = None):
        self.ws = down_ws
        self.host = dest_host
        self.port = dest_port
        self.downstream_cb = downstream_callback
        self.upstream_cb = upstream_callback
        self.ping_cb = ping_callback

    @abstractmethod
    async def proxy(self):
        pass


class TCPProxy(ServiceProxy):

    __slots__ = ServiceProxy.__slots__ + ('down_task', )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.down_task = None

    async def proxy(self):
        try:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
            except ConnectionRefusedError:
                await self.ws.close(code=1014)
                return self.ws

            async def downstream():
                try:
                    while True:
                        try:
                            chunk = await reader.read(8192)
                            if not chunk:
                                break
                            await self.ws.send_bytes(chunk)
                        except (RuntimeError, ConnectionResetError,
                                asyncio.CancelledError):
                            # connection interrupted
                            break
                        else:
                            if self.downstream_cb is not None:
                                await self.downstream_cb(chunk)
                except asyncio.CancelledError:
                    pass
                finally:
                    await self.ws.close(code=1001)

            log.debug('TCPProxy connected {0}:{1}', self.host, self.port)
            self.down_task = asyncio.ensure_future(downstream())
            async for msg in self.ws:
                if msg.type == web.WSMsgType.BINARY:
                    try:
                        writer.write(msg.data)
                        await writer.drain()
                    except RuntimeError:
                        log.debug("Error on writing: Is it closed?")
                    if self.upstream_cb is not None:
                        await self.upstream_cb(msg.data)
                elif msg.type == web.WSMsgType.PING:
                    await self.ws.pong(msg.data)
                    if self.ping_cb is not None:
                        await self.ping_cb(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.debug('ws connection closed with exception %s' %
                              self.ws.exception())
                    writer.close()
                    await writer.wait_closed()

        except asyncio.CancelledError:
            pass
        finally:
            if self.down_task is not None and not self.down_task.done():
                self.down_task.cancel()
                await self.down_task
            log.debug('websocket connection closed')
        return self.ws


class WebSocketProxy:
    __slots__ = (
        'up_conn', 'down_conn',
        'upstream_buffer', 'upstream_buffer_task',
        'downstream_cb', 'upstream_cb', 'ping_cb',
    )

    up_conn: aiohttp.ClientWebSocketResponse
    down_conn: web.WebSocketResponse
    # FIXME: use __future__.annotations in Python 3.7+
    upstream_buffer: asyncio.Queue  # contains: Tuple[Union[bytes, str], web.WSMsgType]
    upstream_buffer_task: Optional[asyncio.Task]
    downstream_cb: Optional[Awaitable]
    upstream_cb: Optional[Awaitable]
    ping_cb: Optional[Awaitable]

    def __init__(self, up_conn: aiohttp.ClientWebSocketResponse,
                 down_conn: web.WebSocketResponse, *,
                 downstream_callback: Awaitable = None,
                 upstream_callback: Awaitable = None,
                 ping_callback: Awaitable = None):
        self.up_conn = up_conn
        self.down_conn = down_conn
        self.upstream_buffer = asyncio.Queue()
        self.upstream_buffer_task = None
        self.downstream_cb = downstream_callback
        self.upstream_cb = upstream_callback
        self.ping_cb = ping_callback

    async def proxy(self):
        asyncio.ensure_future(self.downstream())
        await self.upstream()

    async def upstream(self):
        try:
            async for msg in self.down_conn:
                if msg.type in (web.WSMsgType.TEXT, web.WSMsgType.binary):
                    await self.write(msg.data, msg.type)
                    if self.upstream_cb is not None:
                        await self.upstream_cb(msg.data)
                elif msg.type == web.WSMsgType.PING:
                    if self.ping_cb is not None:
                        await self.ping_cb(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error("ws connection closed with exception {}",
                              self.up_conn.exception())
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break
            # here, client gracefully disconnected
        except asyncio.CancelledError:
            # here, client forcibly disconnected
            raise
        finally:
            await self.close_downstream()

    async def downstream(self):
        try:
            self.upstream_buffer_task = \
                    asyncio.ensure_future(self.consume_upstream_buffer())
            async for msg in self.up_conn:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self.down_conn.send_str(msg.data)
                    if self.downstream_cb is not None:
                        await asyncio.shield(self.downstream_cb(msg.data))
                if msg.type == aiohttp.WSMsgType.BINARY:
                    await self.down_conn.send_bytes(msg.data)
                    if self.downstream_cb is not None:
                        await asyncio.shield(self.downstream_cb(msg.data))
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
            # here, server gracefully disconnected
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('unexpected error')
        finally:
            await self.close_upstream()

    async def consume_upstream_buffer(self):
        while True:
            msg, tp = await self.upstream_buffer.get()
            if self.up_conn:
                if tp == aiohttp.WSMsgType.TEXT:
                    await self.up_conn.send_str(msg)
                elif tp == aiohttp.WSMsgType.binary:
                    await self.up_conn.send_bytes(msg)
            else:
                await self.close()

    async def write(self, msg: Union[bytes, str], tp: web.WSMsgType):
        await self.upstream_buffer.put((msg, tp))

    async def close_downstream(self):
        if not self.down_conn.closed:
            await self.down_conn.close()

    async def close_upstream(self):
        if self.upstream_buffer_task:
            self.upstream_buffer_task.cancel()
            await self.upstream_buffer_task
        if self.up_conn:
            await self.up_conn.close()
