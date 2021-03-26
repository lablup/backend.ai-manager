from __future__ import annotations

import asyncio
import logging
import json
from typing import (
    Any,
    AsyncIterator,
    Final,
    Iterable,
    Mapping,
    Set,
    Tuple,
    Union,
    TYPE_CHECKING,
)

from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response
from aiotools import adefer
import attr
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.events import (
    BgtaskCancelledEvent,
    BgtaskDoneEvent,
    BgtaskFailedEvent,
    BgtaskUpdatedEvent,
    EventDispatcher,
    KernelCancelledEvent,
    KernelCreatingEvent,
    KernelPreparingEvent,
    KernelPullingEvent,
    KernelStartedEvent,
    KernelTerminatedEvent,
    KernelTerminatingEvent,
    SessionCancelledEvent,
    SessionEnqueuedEvent,
    SessionFailureEvent,
    SessionScheduledEvent,
    SessionStartedEvent,
    SessionSuccessEvent,
    SessionTerminatedEvent,
)
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import AgentId

from ..models import kernels, groups, UserRole
from ..types import Sentinel
from .auth import auth_required
from .exceptions import GenericNotFound, GenericForbidden, GroupNotFound
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params

if TYPE_CHECKING:
    from .context import RootContext
    from .types import CORSOptions, WebMiddleware

log = BraceStyleAdapter(logging.getLogger(__name__))

sentinel: Final = Sentinel.token

SessionEventInfo = Tuple[str, dict, str]
BgtaskEvents = Union[BgtaskUpdatedEvent, BgtaskDoneEvent, BgtaskCancelledEvent, BgtaskFailedEvent]


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
    root_ctx: RootContext = request.app['_root.context']
    app_ctx: PrivateContext = request.app['events.context']
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
    my_queue: asyncio.Queue[Sentinel | SessionEventInfo] = asyncio.Queue()
    log.info('PUSH_SESSION_EVENTS (ak:{}, s:{}, g:{})', access_key, session_name, group_name)
    if group_name == '*':
        group_id = '*'
    else:
        async with root_ctx.db.begin() as conn:
            query = (
                sa.select([groups.c.id])
                .select_from(groups)
                .where(groups.c.name == group_name)
            )
            row = await conn.first(query)
            if row is None:
                raise GroupNotFound
            group_id = row['id']
    app_ctx.session_event_queues.add(my_queue)
    defer(lambda: app_ctx.session_event_queues.remove(my_queue))
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
    root_ctx: RootContext = request.app['_root.context']
    app_ctx: PrivateContext = request.app['events.context']
    task_id = params['task_id']
    access_key = request['keypair']['access_key']
    log.info('PUSH_BACKGROUND_TASK_EVENTS (ak:{}, t:{})', access_key, task_id)

    tracker_key = f'bgtask.{task_id}'
    task_info = await root_ctx.redis_stream.hgetall(tracker_key)
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
                await resp.send('{}', event="server_close")
        return resp

    # It is an ongoing task.
    my_queue: asyncio.Queue[BgtaskEvents | Sentinel] = asyncio.Queue()
    app_ctx.task_update_queues.add(my_queue)
    defer(lambda: app_ctx.task_update_queues.remove(my_queue))
    try:
        async with sse_response(request) as resp:
            while True:
                event = await my_queue.get()
                try:
                    if event is sentinel:
                        break
                    if task_id != event.task_id:
                        continue
                    body = {
                        'task_id': str(task_id),
                        'message': event.message,
                    }
                    if isinstance(event, BgtaskUpdatedEvent):
                        body['current_progress'] = event.current_progress
                        body['total_progress'] = event.total_progress
                    await resp.send(json.dumps(body), event=event.name, retry=5)
                    if event.name in ('bgtask_done', 'bgtask_failed', 'bgtask_cancelled'):
                        await resp.send('{}', event="server_close")
                        break
                finally:
                    my_queue.task_done()
    finally:
        return resp


async def enqueue_kernel_creation_status_update(
    app: web.Application,
    source: AgentId,
    event: KernelPreparingEvent | KernelPullingEvent | KernelCreatingEvent | KernelStartedEvent,
) -> None:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']

    async def _fetch():
        async with root_ctx.db.begin() as conn:
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
                    (kernels.c.id == event.kernel_id)
                )
            )
            result = await conn.execute(query)
            return result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app_ctx.session_event_queues:
        q.put_nowait((event.name, row._mapping, event.reason))


async def enqueue_kernel_termination_status_update(
    app: web.Application,
    agent_id: AgentId,
    event: KernelCancelledEvent | KernelTerminatingEvent | KernelTerminatedEvent,
) -> None:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']

    async def _fetch():
        async with root_ctx.db.begin() as conn:
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
                    (kernels.c.id == event.kernel_id)
                )
            )
            result = await conn.execute(query)
            return result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app_ctx.session_event_queues:
        q.put_nowait((event.name, row._mapping, event.reason))


async def enqueue_session_creation_status_update(
    app: web.Application,
    source: AgentId,
    event: SessionEnqueuedEvent | SessionScheduledEvent | SessionStartedEvent | SessionCancelledEvent,
) -> None:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']

    async def _fetch():
        async with root_ctx.db.begin() as conn:
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
                    (kernels.c.id == event.session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app_ctx.session_event_queues:
        q.put_nowait((event.name, row._mapping, event.reason))


async def enqueue_session_termination_status_update(
    app: web.Application,
    agent_id: AgentId,
    event: SessionTerminatedEvent,
) -> None:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']

    async def _fetch():
        async with root_ctx.db.begin() as conn:
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
                    (kernels.c.id == event.session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app_ctx.session_event_queues:
        q.put_nowait((event.name, row._mapping, event.reason))


async def enqueue_batch_task_result_update(
    app: web.Application,
    agent_id: AgentId,
    event: SessionSuccessEvent | SessionFailureEvent,
) -> None:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']

    async def _fetch():
        async with root_ctx.db.begin() as conn:
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
                    (kernels.c.id == event.session_id)
                )
            )
            result = await conn.execute(query)
            return result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app_ctx.session_event_queues:
        q.put_nowait((event.name, row._mapping, event.reason))


async def enqueue_bgtask_status_update(
    app: web.Application,
    source: AgentId,
    event: BgtaskUpdatedEvent | BgtaskDoneEvent | BgtaskCancelledEvent | BgtaskFailedEvent,
) -> None:
    app_ctx: PrivateContext = app['events.context']
    for q in app_ctx.task_update_queues:
        q.put_nowait(event)


@attr.s(slots=True, auto_attribs=True, init=False)
class PrivateContext:
    session_event_queues: Set[asyncio.Queue[Sentinel | SessionEventInfo]]
    task_update_queues: Set[asyncio.Queue[Sentinel | BgtaskEvents]]


async def events_app_ctx(app: web.Application) -> AsyncIterator[None]:
    root_ctx: RootContext = app['_root.context']
    app_ctx: PrivateContext = app['events.context']
    app_ctx.session_event_queues = set()
    app_ctx.task_update_queues = set()
    event_dispatcher: EventDispatcher = root_ctx.event_dispatcher
    event_dispatcher.subscribe(SessionEnqueuedEvent, app, enqueue_session_creation_status_update)
    event_dispatcher.subscribe(SessionScheduledEvent, app, enqueue_session_creation_status_update)
    event_dispatcher.subscribe(KernelPreparingEvent, app, enqueue_kernel_creation_status_update)
    event_dispatcher.subscribe(KernelPullingEvent, app, enqueue_kernel_creation_status_update)
    event_dispatcher.subscribe(KernelCreatingEvent, app, enqueue_kernel_creation_status_update)
    event_dispatcher.subscribe(KernelStartedEvent, app, enqueue_kernel_creation_status_update)
    event_dispatcher.subscribe(SessionStartedEvent, app, enqueue_session_creation_status_update)
    event_dispatcher.subscribe(KernelTerminatingEvent, app, enqueue_kernel_termination_status_update)
    event_dispatcher.subscribe(KernelTerminatedEvent, app, enqueue_kernel_termination_status_update)
    event_dispatcher.subscribe(KernelCancelledEvent, app, enqueue_kernel_termination_status_update)
    event_dispatcher.subscribe(SessionTerminatedEvent, app, enqueue_session_termination_status_update)
    event_dispatcher.subscribe(SessionCancelledEvent, app, enqueue_session_creation_status_update)
    event_dispatcher.subscribe(SessionSuccessEvent, app, enqueue_batch_task_result_update)
    event_dispatcher.subscribe(SessionFailureEvent, app, enqueue_batch_task_result_update)
    event_dispatcher.subscribe(BgtaskUpdatedEvent, app, enqueue_bgtask_status_update)
    event_dispatcher.subscribe(BgtaskDoneEvent, app, enqueue_bgtask_status_update)
    event_dispatcher.subscribe(BgtaskCancelledEvent, app, enqueue_bgtask_status_update)
    event_dispatcher.subscribe(BgtaskFailedEvent, app, enqueue_bgtask_status_update)

    yield


async def events_shutdown(app: web.Application) -> None:
    # shutdown handler is called before waiting for closing active connections.
    # We need to put sentinels here to ensure delivery of them to active SSE connections.
    app_ctx: PrivateContext = app['events.context']
    for sq in app_ctx.session_event_queues:
        sq.put_nowait(sentinel)
    for tq in app_ctx.task_update_queues:
        tq.put_nowait(sentinel)
    await asyncio.sleep(0)


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app['prefix'] = 'events'
    app['events.context'] = PrivateContext()
    app['api_versions'] = (3, 4)
    app.on_shutdown.append(events_shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    app.cleanup_ctx.append(events_app_ctx)
    cors.add(add_route('GET', r'/background-task', push_background_task_events))
    cors.add(add_route('GET', r'/session', push_session_events))
    return app, []
