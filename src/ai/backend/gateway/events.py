from __future__ import annotations

import asyncio
from collections import defaultdict
import functools
import logging
import json
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Final,
    Generic,
    Iterable,
    Mapping,
    MutableMapping,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Union,
    TypeVar,
    TYPE_CHECKING,
    cast,
)
import uuid
import weakref

from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response
import aioredis
from aiotools import adefer
import attr
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import msgpack, redis
from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    aobject,
    AgentId,
)
from .auth import auth_required
from .defs import REDIS_STREAM_DB
from .exceptions import GenericNotFound, GenericForbidden, GroupNotFound
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params
from ..manager.models import kernels, groups, UserRole
from ..manager.types import BackgroundTaskEventArgs, Sentinel
if TYPE_CHECKING:
    from .types import CORSOptions, WebMiddleware
    from ..gateway.config import LocalConfig, SharedConfig

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.events'))

sentinel: Final = Sentinel.token

C = TypeVar('C', contravariant=True)


class EventCallback(Protocol[C]):
    async def __call__(
        self,
        context: C,
        agent_id: AgentId,
        event_name: str,
        *args: Any,
    ) -> None:
        ...


@attr.s(auto_attribs=True, slots=True, frozen=True, eq=False, order=False)
class EventHandler(Generic[C]):
    context: C
    callback: EventCallback[C]


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

    consumers: MutableMapping[str, Set[EventHandler[Any]]]
    subscribers: MutableMapping[str, Set[EventHandler[Any]]]
    redis_producer: aioredis.Redis
    redis_consumer: aioredis.Redis
    redis_subscriber: aioredis.Redis
    consumer_loop_task: asyncio.Task
    subscriber_loop_task: asyncio.Task
    producer_lock: asyncio.Lock
    consumer_taskset: weakref.WeakSet[asyncio.Task]
    subscriber_taskset: weakref.WeakSet[asyncio.Task]

    def __init__(self, local_config: LocalConfig, shared_config: SharedConfig) -> None:
        self.local_config = local_config
        self.shared_config = shared_config
        self.consumers = defaultdict(set)
        self.subscribers = defaultdict(set)

    async def __ainit__(self) -> None:
        self.redis_producer = await self._create_redis()
        self.redis_consumer = await self._create_redis()
        self.redis_subscriber = await self._create_redis()
        self.consumer_loop_task = asyncio.create_task(self._consume_loop())
        self.subscriber_loop_task = asyncio.create_task(self._subscribe_loop())
        self.producer_lock = asyncio.Lock()
        self.consumer_taskset = weakref.WeakSet()
        self.subscriber_taskset = weakref.WeakSet()

    async def _create_redis(self):
        redis_url = self.shared_config.get_redis_url(db=REDIS_STREAM_DB)
        return await redis.connect_with_retries(str(redis_url), encoding=None)

    async def close(self) -> None:
        cancelled_tasks = []
        for task in self.consumer_taskset:
            if not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        for task in self.subscriber_taskset:
            if not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        self.consumer_loop_task.cancel()
        self.subscriber_loop_task.cancel()
        cancelled_tasks.append(self.consumer_loop_task)
        cancelled_tasks.append(self.subscriber_loop_task)
        await asyncio.gather(*cancelled_tasks, return_exceptions=True)
        self.redis_producer.close()
        self.redis_consumer.close()
        self.redis_subscriber.close()
        await self.redis_producer.wait_closed()
        await self.redis_consumer.wait_closed()
        await self.redis_subscriber.wait_closed()

    def consume(self, event_name: str, context: C, callback: EventCallback[C]) -> EventHandler[C]:
        handler = EventHandler(context, callback)
        self.consumers[event_name].add(handler)
        return handler

    def unconsume(self, event_name: str, handler: EventHandler[Any]) -> None:
        self.consumers[event_name].discard(handler)

    def subscribe(self, event_name: str, context: C, callback: EventCallback[C]) -> EventHandler[C]:
        handler = EventHandler(context, callback)
        self.subscribers[event_name].add(handler)
        return handler

    def unsubscribe(self, event_name: str, handler: EventHandler[Any]) -> None:
        self.subscribers[event_name].discard(handler)

    async def produce_event(self, event_name: str,
                            args: Sequence[Any] = tuple(), *,
                            agent_id: str = 'manager') -> None:
        raw_msg = msgpack.packb({
            'event_name': event_name,
            'agent_id': agent_id,
            'args': args,
        })
        async with self.producer_lock:
            def _pipe_builder():
                pipe = self.redis_producer.pipeline()
                pipe.rpush('events.prodcons', raw_msg)
                pipe.publish('events.pubsub', raw_msg)
                return pipe
            await redis.execute_with_retries(_pipe_builder)

    async def dispatch_consumers(self, event_name: str, agent_id: AgentId,
                                 args: Tuple[Any, ...] = tuple()) -> None:
        log_fmt = 'DISPATCH_CONSUMERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self.local_config['debug']['log-events']:
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for consumer in self.consumers[event_name]:
            cb = consumer.callback
            if asyncio.iscoroutine(cb):
                self.consumer_taskset.add(asyncio.create_task(cast(Awaitable, cb)))
            elif asyncio.iscoroutinefunction(cb):
                self.consumer_taskset.add(asyncio.create_task(
                    cb(consumer.context, agent_id, event_name, *args)
                ))
            else:
                cb = functools.partial(cb, consumer.context, agent_id, event_name, *args)
                loop.call_soon(cb)

    async def dispatch_subscribers(self, event_name: str, agent_id: AgentId,
                                   args: Tuple[Any, ...] = tuple()) -> None:
        log_fmt = 'DISPATCH_SUBSCRIBERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self.local_config['debug']['log-events']:
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for subscriber in self.subscribers[event_name]:
            cb = subscriber.callback
            if asyncio.iscoroutine(cb):
                self.subscriber_taskset.add(asyncio.create_task(cast(Awaitable, cb)))
            elif asyncio.iscoroutinefunction(cb):
                self.subscriber_taskset.add(asyncio.create_task(
                    cb(subscriber.context, agent_id, event_name, *args)
                ))
            else:
                cb = functools.partial(cb, subscriber.context, agent_id, event_name, *args)
                loop.call_soon(cb)

    async def _consume_loop(self) -> None:
        while True:
            try:
                key, raw_msg = await redis.execute_with_retries(
                    lambda: self.redis_consumer.blpop('events.prodcons'))
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_consumers(msg['event_name'],
                                              msg['agent_id'],
                                              msg['args'])
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.consume(): unexpected-error')

    async def _subscribe_loop(self) -> None:

        async def _subscribe_impl():
            channels = await self.redis_subscriber.subscribe('events.pubsub')
            async for raw_msg in channels[0].iter():
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_subscribers(msg['event_name'],
                                                msg['agent_id'],
                                                msg['args'])

        while True:
            try:
                await redis.execute_with_retries(lambda: _subscribe_impl())
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.subscribe(): unexpected-error')


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['name', 'sessionName'], default='*') >> 'session_name': t.String,
        t.Key('ownerAccessKey', default=None) >> 'owner_access_key': t.Null | t.String,
        t.Key('sessionId', default=None) >> 'session_id': t.Null | tx.UUID,
        # NOTE: if set, sessionId overrides sessionName and ownerAccessKey parameters.
        tx.AliasedKey(['group', 'groupName'], default='*') >> 'group_name': t.String,
        t.Key('scope', default='*'): t.Enum('*', 'session', 'kernel'),
    }))
@adefer
async def push_session_events(
    defer,
    request: web.Request,
    params: Mapping[str, Any],
) -> web.StreamResponse:
    app = request.app
    session_name = params['session_name']
    session_id = params['session_id']
    scope = params['scope']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']
    access_key = params['owner_access_key']
    if access_key is None:
        access_key = request['keypair']['access_key']
    if user_role == UserRole.USER:
        if access_key != request['keypair']['access_key']:
            raise GenericForbidden
    group_name = params['group_name']
    session_event_queues = app['session_event_queues']  # type: Set[asyncio.Queue]
    my_queue = asyncio.Queue()  # type: asyncio.Queue[Union[Sentinel, Tuple[str, dict, str]]]
    log.info('PUSH_SESSION_EVENTS (ak:{}, s:{}, g:{})', access_key, session_name, group_name)
    if group_name == '*':
        group_id = '*'
    else:
        async with app['dbpool'].acquire() as conn, conn.begin():
            query = (
                sa.select([groups.c.id])
                .select_from(groups)
                .where(groups.c.name == group_name)
            )
            row = await conn.first(query)
            if row is None:
                raise GroupNotFound
            group_id = row['id']
    session_event_queues.add(my_queue)
    defer(lambda: session_event_queues.remove(my_queue))
    try:
        async with sse_response(request) as resp:
            while True:
                evdata = await my_queue.get()
                try:
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
                    if scope == 'session' and not event_name.startswith('session_'):
                        continue
                    if scope == 'kernel' and not event_name.startswith('kernel_'):
                        continue
                    if session_id is not None:
                        if row['session_id'] != session_id:
                            continue
                    else:
                        if session_name != '*' and not (
                            (row['session_name'] == session_name) and
                            (row['access_key'] == access_key)):
                            continue
                    response_data = {
                        'reason': reason,
                        'sessionName': row['session_name'],
                        'ownerAccessKey': row['access_key'],
                        'sessionId': str(row['session_id']),
                    }
                    if kernel_id := row.get('id'):
                        response_data['kernelId'] = str(kernel_id)
                    if cluster_role := row.get('cluster_role'):
                        response_data['clusterRole'] = cluster_role
                    if cluster_idx := row.get('cluster_idx'):
                        response_data['clusterIdx'] = cluster_idx
                    await resp.send(json.dumps(response_data), event=event_name)
                finally:
                    my_queue.task_done()
    finally:
        return resp


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict({
    tx.AliasedKey(['task_id', 'taskId']): tx.UUID,
}))
@adefer
async def push_background_task_events(
    defer,
    request: web.Request,
    params: Mapping[str, Any],
) -> web.StreamResponse:
    app = request.app
    task_id = params['task_id']
    access_key = request['keypair']['access_key']
    log.info('PUSH_BACKGROUND_TASK_EVENTS (ak:{}, t:{})', access_key, task_id)

    tracker_key = f'bgtask.{task_id}'
    task_info = await app['redis_stream'].hgetall(tracker_key)
    if task_info is None:
        # The task ID is invalid or represents a task completed more than 24 hours ago.
        raise GenericNotFound('No such background task.')

    if task_info['status'] != 'started':
        # It is an already finished task!
        async with sse_response(request) as resp:
            try:
                body = {
                    'task_id': str(task_id),
                    'status': task_info['status'],
                    'current_progress': task_info['current'],
                    'total_progress': task_info['total'],
                    'message': task_info['msg'],
                }
                await resp.send(json.dumps(body), event=f"task_{task_info['status']}")
            finally:
                await resp.send(b'{}', event="server_close")
        return resp

    # It is an ongoing task.
    task_update_queues = app['task_update_queues']  # type: Set[asyncio.Queue]
    my_queue = asyncio.Queue()  # type: asyncio.Queue[Union[Sentinel, Tuple[str, Any]]]
    task_update_queues.add(my_queue)
    defer(lambda: task_update_queues.remove(my_queue))
    try:
        async with sse_response(request) as resp:
            while True:
                event_args = await my_queue.get()
                try:
                    if event_args is sentinel:
                        break
                    event_name = event_args[0]
                    event_data = BackgroundTaskEventArgs(**event_args[1])
                    if task_id != uuid.UUID(event_data.task_id):
                        continue
                    body = {
                        'task_id': str(task_id),
                        'message': event_data.message,
                    }
                    if event_data.current_progress is not None:
                        body['current_progress'] = event_data.current_progress
                    if event_data.total_progress is not None:
                        body['total_progress'] = event_data.total_progress
                    await resp.send(json.dumps(body), event=event_name, retry=5)
                    if event_name in ('task_done', 'task_failed', 'task_cancelled'):
                        await resp.send('{}', event="server_close")
                        break
                finally:
                    my_queue.task_done()
    finally:
        return resp


async def enqueue_kernel_status_update_with_creation_id(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_kernel_id: str,
    kernel_creation_id: str,
    reason: str = None,
    exit_code: int = None,
) -> None:
    if raw_kernel_id is None:
        return
    kernel_id = uuid.UUID(raw_kernel_id)

    async def _fetch():
        async with app['dbpool'].acquire() as conn, conn.begin():
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.session_id,
                    kernels.c.session_name,
                    kernels.c.access_key,
                    kernels.c.cluster_role,
                    kernels.c.cluster_idx,
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
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_kernel_status_update(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_kernel_id: str,
    reason: str = None,
    exit_code: int = None,
) -> None:
    if raw_kernel_id is None:
        return
    kernel_id = uuid.UUID(raw_kernel_id)

    async def _fetch():
        async with app['dbpool'].acquire() as conn, conn.begin():
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.session_id,
                    kernels.c.session_name,
                    kernels.c.access_key,
                    kernels.c.cluster_role,
                    kernels.c.cluster_idx,
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
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_session_status_update_with_creation_id(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_session_id: str,
    session_creation_id: str,
    reason: str = None,
    exit_code: int = None,
) -> None:
    if raw_session_id is None:
        return
    session_id = uuid.UUID(raw_session_id)

    async def _fetch():
        async with app['dbpool'].acquire() as conn:
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.session_id,
                    kernels.c.session_name,
                    kernels.c.access_key,
                    kernels.c.domain_name,
                    kernels.c.group_id,
                    kernels.c.user_uuid,
                ])
                .select_from(kernels)
                .where(
                    (kernels.c.id == session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_session_status_update(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_session_id: str,
    reason: str = None,
    exit_code: int = None,
) -> None:
    if raw_session_id is None:
        return
    session_id = uuid.UUID(raw_session_id)

    async def _fetch():
        async with app['dbpool'].acquire() as conn:
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.session_id,
                    kernels.c.session_name,
                    kernels.c.access_key,
                    kernels.c.domain_name,
                    kernels.c.group_id,
                    kernels.c.user_uuid,
                ])
                .select_from(kernels)
                .where(
                    (kernels.c.id == session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_batch_task_result_update(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_kernel_id: str,
    exit_code: int = None,
    exit_reason: str = None,
) -> None:
    kernel_id = uuid.UUID(raw_kernel_id)

    async def _fetch():
        async with app['dbpool'].acquire() as conn:
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.session_id,
                    kernels.c.session_name,
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
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    if event_name == 'session_success':
        reason = 'task-success'
    else:
        reason = 'task-failure'
    for q in app['session_event_queues']:
        q.put_nowait((event_name, row, reason))


async def enqueue_task_status_update(
    app: web.Application,
    agent_id: AgentId,
    event_name: str,
    raw_task_id: str,
    current_progress: Union[int, float] = None,
    total_progress: Union[int, float] = None,
    message: str = None,
) -> None:
    for q in app['task_update_queues']:
        q.put_nowait((event_name, raw_task_id, current_progress, total_progress, message, ))


async def events_app_ctx(app: web.Application) -> AsyncIterator[None]:
    app['session_event_queues'] = set()
    app['task_update_queues'] = set()
    event_dispatcher = app['event_dispatcher']
    event_dispatcher.subscribe('session_enqueued', app, enqueue_session_status_update_with_creation_id)
    event_dispatcher.subscribe('session_scheduled', app, enqueue_session_status_update_with_creation_id)
    event_dispatcher.subscribe('kernel_preparing', app, enqueue_kernel_status_update_with_creation_id)
    event_dispatcher.subscribe('kernel_pulling', app, enqueue_kernel_status_update_with_creation_id)
    event_dispatcher.subscribe('kernel_creating', app, enqueue_kernel_status_update_with_creation_id)
    event_dispatcher.subscribe('kernel_started', app, enqueue_kernel_status_update_with_creation_id)
    event_dispatcher.subscribe('session_started', app, enqueue_session_status_update_with_creation_id)
    event_dispatcher.subscribe('kernel_terminating', app, enqueue_kernel_status_update)
    event_dispatcher.subscribe('kernel_terminated', app, enqueue_kernel_status_update)
    event_dispatcher.subscribe('session_terminated', app, enqueue_session_status_update)
    event_dispatcher.subscribe('kernel_cancelled', app, enqueue_kernel_status_update)
    event_dispatcher.subscribe('session_cancelled', app, enqueue_session_status_update_with_creation_id)
    event_dispatcher.subscribe('session_success', app, enqueue_batch_task_result_update)
    event_dispatcher.subscribe('session_failure', app, enqueue_batch_task_result_update)
    event_dispatcher.subscribe('task_updated', app, enqueue_task_status_update)
    event_dispatcher.subscribe('task_done', app, enqueue_task_status_update)
    event_dispatcher.subscribe('task_cancelled', app, enqueue_task_status_update)
    event_dispatcher.subscribe('task_failed', app, enqueue_task_status_update)

    yield


async def events_shutdown(app: web.Application) -> None:
    # shutdown handler is called before waiting for closing active connections.
    # We need to put sentinels here to ensure delivery of them to active SSE connections.
    for q in app['session_event_queues']:
        q.put_nowait(sentinel)
    for q in app['task_update_queues']:
        q.put_nowait(sentinel)
    await asyncio.sleep(0)


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app['prefix'] = 'events'
    app['api_versions'] = (3, 4)
    app.on_shutdown.append(events_shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    app.cleanup_ctx.append(events_app_ctx)
    cors.add(add_route('GET', r'/background-task', push_background_task_events))
    cors.add(add_route('GET', r'/session', push_session_events))
    return app, []
