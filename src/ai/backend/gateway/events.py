import asyncio
from collections import defaultdict
from dataclasses import dataclass
import functools
import logging
from typing import (
    Any,
    List, Tuple,
    MutableMapping,
)
from typing_extensions import Protocol

from aiohttp import web
import aioredis
from aiojobs.aiohttp import get_scheduler_from_app

from ai.backend.common import msgpack
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    aobject,
    AgentId,
)
from .defs import REDIS_STREAM_DB
from .utils import current_loop

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.events'))


class EventCallback(Protocol):
    async def __call__(self,
                       app: web.Application,
                       agent_id: AgentId,
                       event_name: str,
                       *args):
        ...


@dataclass
class EventHandler:
    app: web.Application
    callback: EventCallback


class EventDispatcher(aobject):

    loop: asyncio.AbstractEventLoop
    root_app: web.Application
    subscriber_task: asyncio.Task
    handlers: MutableMapping[str, List[EventHandler]]
    redis: aioredis.Redis

    def __init__(self, app: web.Application) -> None:
        self.loop = current_loop()
        self.root_app = app
        self.handlers = defaultdict(list)

    async def __ainit__(self) -> None:
        config = self.root_app['config']
        self.redis = await aioredis.create_redis(
            config['redis']['addr'].as_sockaddr(),
            db=REDIS_STREAM_DB,
            password=config['redis']['password'] if config['redis']['password'] else None,
            encoding=None)
        self.subscriber_task = self.loop.create_task(self.subscribe())

    async def close(self) -> None:
        self.subscriber_task.cancel()
        await self.subscriber_task
        self.redis.close()
        await self.redis.wait_closed()

    def add_handler(self, event_name: str, app: web.Application, callback: EventCallback) -> None:
        self.handlers[event_name].append(EventHandler(app, callback))

    async def publish_event(self, event_name: str, args: Tuple[Any, ...] = tuple()) -> None:
        raw_msg = msgpack.packb({
            'event_name': event_name,
            'agent_id': 'manager',
            'args': args,
        })
        await self.redis.rpush('agent.events', raw_msg)

    async def dispatch(self, event_name: str, agent_id: AgentId,
                       args: Tuple[Any, ...] = tuple()) -> None:
        log.debug('DISPATCH({0}/{1})', event_name, agent_id)
        scheduler = get_scheduler_from_app(self.root_app)
        for handler in self.handlers[event_name]:
            cb = handler.callback
            try:
                if asyncio.iscoroutine(cb) or asyncio.iscoroutinefunction(cb):
                    await scheduler.spawn(cb(handler.app, agent_id, event_name, *args))
                else:
                    cb = functools.partial(cb, handler.app, agent_id, event_name, *args)
                    self.loop.call_soon(cb)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('EventDispatcher.dispatch(ev:{}, ag:{}): unexpected error',
                              event_name, agent_id)

    async def subscribe(self) -> None:
        try:
            while True:
                key, raw_msg = await self.redis.blpop('agent.events')
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch(msg['event_name'], msg['agent_id'], msg['args'])
        except asyncio.CancelledError:
            pass


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
