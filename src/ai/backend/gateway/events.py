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
import uuid

from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response
from aiotools import adefer
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
from .auth import auth_required
from .exceptions import GenericNotFound, GenericForbidden, GroupNotFound
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params
from ..manager.models import kernels, groups, UserRole
from ..manager.types import BackgroundTaskEventArgs, Sentinel
if TYPE_CHECKING:
    from .types import CORSOptions, WebMiddleware

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.events'))

sentinel: Final = Sentinel.token


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


async def enqueue_kernel_creation_status_update(
    app: web.Application,
    source: AgentId,
    event: KernelPreparingEvent | KernelPullingEvent | KernelCreatingEvent | KernelStartedEvent,
) -> None:

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
                    (kernels.c.id == event.kernel_id)
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event.name, row, event.reason))


async def enqueue_kernel_termination_status_update(
    app: web.Application,
    agent_id: AgentId,
    event: KernelCancelledEvent | KernelTerminatingEvent | KernelTerminatedEvent,
) -> None:

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
                    (kernels.c.id == event.kernel_id)
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event.name, row, event.reason))


async def enqueue_session_creation_status_update(
    app: web.Application,
    source: AgentId,
    event: SessionEnqueuedEvent | SessionScheduledEvent | SessionStartedEvent | SessionCancelledEvent,
) -> None:

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
                    (kernels.c.id == event.session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event.name, row, event.reason))


async def enqueue_session_termination_status_update(
    app: web.Application,
    agent_id: AgentId,
    event: SessionTerminatedEvent,
) -> None:

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
                    (kernels.c.id == event.session_id)
                    # for the main kernel, kernel ID == session ID
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event.name, row, event.reason))


async def enqueue_batch_task_result_update(
    app: web.Application,
    agent_id: AgentId,
    event: SessionSuccessEvent | SessionFailureEvent,
) -> None:

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
                    (kernels.c.id == event.session_id)
                )
            )
            result = await conn.execute(query)
            return await result.first()

    row = await asyncio.shield(_fetch())
    if row is None:
        return
    for q in app['session_event_queues']:
        q.put_nowait((event.name, row, event.reason))


async def enqueue_bgtask_status_update(
    app: web.Application,
    source: AgentId,
    event: BgtaskUpdatedEvent | BgtaskDoneEvent | BgtaskCancelledEvent | BgtaskFailedEvent,
) -> None:
    for q in app['task_update_queues']:
        q.put_nowait(
            (event.name, event.task_id,
             event.current_progress, event.total_progress,
             event.message,)
        )


async def events_app_ctx(app: web.Application) -> AsyncIterator[None]:
    app['session_event_queues'] = set()
    app['task_update_queues'] = set()
    event_dispatcher: EventDispatcher = app['event_dispatcher']
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
