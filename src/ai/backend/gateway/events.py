import asyncio
from collections import defaultdict
from dataclasses import dataclass
import functools
import logging
from typing import (
    Any,
    Sequence, List, Tuple,
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
    context: Any
    callback: EventCallback


class EventDispatcher(aobject):
    '''
    We have two types of event handlers: consumer and subscriber.

    Consumers use the distribution pattern. Only one consumer among many manager worker processes
    receives the event.

    Consumer example: database updates upon specific events.

    Subscribers use the broadcast pattern. All subscribers in many manager worker processes
    receive the same event.

    Subscriber example: enqueuing events to the queues for event streaming API handlers
    '''

    loop: asyncio.AbstractEventLoop
    root_app: web.Application
    subscriber_task: asyncio.Task
    consumers: MutableMapping[str, List[EventHandler]]
    subscribers: MutableMapping[str, List[EventHandler]]
    redis_producer: aioredis.Redis
    redis_consumer: aioredis.Redis
    redis_subscriber: aioredis.Redis

    def __init__(self, app: web.Application) -> None:
        self.loop = current_loop()
        self.root_app = app
        self.consumers = defaultdict(list)
        self.subscribers = defaultdict(list)

    async def __ainit__(self) -> None:
        self.redis_producer = await self._create_redis()
        self.redis_consumer = await self._create_redis()
        self.redis_subscriber = await self._create_redis()
        self.consumer_task = self.loop.create_task(self._consume())
        self.subscriber_task = self.loop.create_task(self._subscribe())

    async def _create_redis(self):
        config = self.root_app['config']
        while True:
            try:
                return await aioredis.create_redis(
                    config['redis']['addr'].as_sockaddr(),
                    db=REDIS_STREAM_DB,
                    password=config['redis']['password'] if config['redis']['password'] else None,
                    encoding=None)
            except ConnectionRefusedError:
                await asyncio.sleep(0.5)
                continue

    async def close(self) -> None:
        self.consumer_task.cancel()
        await self.consumer_task
        self.subscriber_task.cancel()
        await self.subscriber_task
        self.redis_producer.close()
        self.redis_consumer.close()
        self.redis_subscriber.close()
        await self.redis_producer.wait_closed()
        await self.redis_consumer.wait_closed()
        await self.redis_subscriber.wait_closed()

    def consume(self, event_name: str, context: Any, callback: EventCallback) -> None:
        self.consumers[event_name].append(EventHandler(context, callback))

    def subscribe(self, event_name: str, context: Any, callback: EventCallback) -> None:
        self.subscribers[event_name].append(EventHandler(context, callback))

    async def produce_event(self, event_name: str,
                            args: Sequence[Any] = tuple(), *,
                            agent_id: str = 'manager') -> None:
        raw_msg = msgpack.packb({
            'event_name': event_name,
            'agent_id': agent_id,
            'args': args,
        })
        while True:
            try:
                commands = self.redis_producer.pipeline()
                commands.rpush('events.prodcons', raw_msg)
                commands.publish('events.pubsub', raw_msg)
                await commands.execute()
            except (ConnectionRefusedError, aioredis.errors.ConnectionClosedError,
                    aioredis.errors.PipelineError):
                await asyncio.sleep(0.2)
                self.redis_producer = await self._create_redis()
                continue
            else:
                break

    async def dispatch_consumers(self, event_name: str, agent_id: AgentId,
                                 args: Tuple[Any, ...] = tuple()) -> None:
        log_fmt = 'DISPATCH_CONSUMERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self.root_app['config']['debug']['log-events']:
            log.debug(log_fmt, *log_args)
        scheduler = get_scheduler_from_app(self.root_app)
        for consumer in self.consumers[event_name]:
            cb = consumer.callback
            try:
                if asyncio.iscoroutine(cb) or asyncio.iscoroutinefunction(cb):
                    await scheduler.spawn(cb(consumer.context, agent_id, event_name, *args))
                else:
                    cb = functools.partial(cb, consumer.context, agent_id, event_name, *args)
                    self.loop.call_soon(cb)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(log_fmt + ': unexpected-error', *log_args)

    async def dispatch_subscribers(self, event_name: str, agent_id: AgentId,
                                   args: Tuple[Any, ...] = tuple()) -> None:
        log_fmt = 'DISPATCH_SUBSCRIBERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self.root_app['config']['debug']['log-events']:
            log.debug(log_fmt, *log_args)
        scheduler = get_scheduler_from_app(self.root_app)
        for subscriber in self.subscribers[event_name]:
            cb = subscriber.callback
            try:
                if asyncio.iscoroutine(cb) or asyncio.iscoroutinefunction(cb):
                    await scheduler.spawn(cb(subscriber.context, agent_id, event_name, *args))
                else:
                    cb = functools.partial(cb, subscriber.context, agent_id, event_name, *args)
                    self.loop.call_soon(cb)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(log_fmt + ': unexpected-error', *log_args)

    async def _consume(self) -> None:
        try:
            while True:
                try:
                    key, raw_msg = await self.redis_consumer.blpop('events.prodcons')
                except (ConnectionRefusedError, aioredis.errors.ConnectionClosedError):
                    await asyncio.sleep(0.5)
                    self.redis_consumer = await self._create_redis()
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_consumers(msg['event_name'], msg['agent_id'], msg['args'])
        except asyncio.CancelledError:
            pass

    async def _subscribe(self) -> None:
        try:
            while True:
                try:
                    channels = await self.redis_subscriber.subscribe('events.pubsub')
                    async for raw_msg in channels[0].iter():
                        msg = msgpack.unpackb(raw_msg)
                        await self.dispatch_subscribers(msg['event_name'], msg['agent_id'], msg['args'])
                except (ConnectionRefusedError, aioredis.errors.ConnectionClosedError):
                    await asyncio.sleep(0.5)
                    self.redis_subscriber = await self._create_redis()
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
