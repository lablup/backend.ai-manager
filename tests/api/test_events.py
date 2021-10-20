from __future__ import annotations

import asyncio

from aiohttp import web
import attr
import pytest

from ai.backend.common.events import (
    AbstractEvent,
    BgtaskDoneEvent,
    BgtaskFailedEvent,
    BgtaskUpdatedEvent,
    EventDispatcher,
    EventProducer,
)
from ai.backend.manager.api.context import RootContext
from ai.backend.manager.server import (
    shared_config_ctx, event_dispatcher_ctx, background_task_ctx,
)
from ai.backend.common.types import (
    AgentId,
)


@attr.s(slots=True, frozen=True)
class TestEvent(AbstractEvent):
    name = "testing"

    value: int = attr.ib()

    def serialize(self) -> tuple:
        return (self.value + 1, )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0] + 1)


@pytest.mark.asyncio
async def test_dispatch(etcd_fixture, create_app_and_client) -> None:
    app, client = await create_app_and_client(
        [shared_config_ctx, event_dispatcher_ctx],
        ['.events'],
    )
    root_ctx: RootContext = app['_root.context']
    producer: EventProducer = root_ctx.event_producer
    dispatcher: EventDispatcher = root_ctx.event_dispatcher

    records = set()

    async def acb(context: web.Application, source: AgentId, event: TestEvent) -> None:
        assert context is app
        assert source == AgentId('i-test')
        assert isinstance(event, TestEvent)
        assert event.name == "testing"
        assert event.value == 1001
        await asyncio.sleep(0.01)
        records.add('async')

    def scb(context: web.Application, source: AgentId, event: TestEvent) -> None:
        assert context is app
        assert source == AgentId('i-test')
        assert isinstance(event, TestEvent)
        assert event.name == "testing"
        assert event.value == 1001
        records.add('sync')

    dispatcher.subscribe(TestEvent, app, acb)
    dispatcher.subscribe(TestEvent, app, scb)

    # Dispatch the event
    await producer.produce_event(TestEvent(999), source='i-test')
    await asyncio.sleep(0.2)
    assert records == {'async', 'sync'}

    await producer.redis_client.flushdb()
    await producer.close()
    await dispatcher.close()


@pytest.mark.asyncio
async def test_error_on_dispatch(etcd_fixture, create_app_and_client, event_loop) -> None:

    def handle_exception(loop, context):
        exc = context['exception']
        exception_log.append(type(exc).__name__)

    app, client = await create_app_and_client(
        [shared_config_ctx, event_dispatcher_ctx],
        ['.events'],
        scheduler_opts={'exception_handler': handle_exception},
    )
    root_ctx: RootContext = app['_root.context']
    producer: EventProducer = root_ctx.event_producer
    dispatcher: EventDispatcher = root_ctx.event_dispatcher
    old_handler = event_loop.get_exception_handler()
    event_loop.set_exception_handler(handle_exception)

    exception_log: list[str] = []

    async def acb(context: web.Application, source: AgentId, event: TestEvent) -> None:
        assert context is app
        assert source == AgentId('i-test')
        assert isinstance(event, TestEvent)
        raise ZeroDivisionError

    def scb(context: web.Application, source: AgentId, event: TestEvent) -> None:
        assert context is app
        assert source == AgentId('i-test')
        assert isinstance(event, TestEvent)
        raise OverflowError

    dispatcher.subscribe(TestEvent, app, scb)
    dispatcher.subscribe(TestEvent, app, acb)
    await producer.produce_event(TestEvent(0), source='i-test')
    await asyncio.sleep(0.2)
    assert len(exception_log) == 2
    assert 'ZeroDivisionError' in exception_log
    assert 'OverflowError' in exception_log

    event_loop.set_exception_handler(old_handler)

    await producer.redis_client.flushdb()
    await producer.close()
    await dispatcher.close()


@pytest.mark.asyncio
async def test_background_task(etcd_fixture, create_app_and_client) -> None:
    app, client = await create_app_and_client(
        [shared_config_ctx, event_dispatcher_ctx, background_task_ctx],
        ['.events'],
    )
    root_ctx: RootContext = app['_root.context']
    producer: EventProducer = root_ctx.event_producer
    dispatcher: EventDispatcher = root_ctx.event_dispatcher
    update_handler_ctx = {}
    done_handler_ctx = {}

    async def update_sub(
        context: web.Application,
        source: AgentId,
        event: BgtaskUpdatedEvent,
    ) -> None:
        # Copy the arguments to the uppser scope
        # since assertions inside the handler does not affect the test result
        # because the handlers are executed inside a separate asyncio task.
        update_handler_ctx['event_name'] = event.name
        update_handler_ctx.update(**attr.asdict(event))

    async def done_sub(
        context: web.Application,
        source: AgentId,
        event: BgtaskDoneEvent,
    ) -> None:
        done_handler_ctx['event_name'] = event.name
        done_handler_ctx.update(**attr.asdict(event))

    async def _mock_task(reporter):
        reporter.total_progress = 2
        await asyncio.sleep(1)
        await reporter.update(1, message='BGTask ex1')
        await asyncio.sleep(0.5)
        await reporter.update(1, message='BGTask ex2')
        return 'hooray'

    dispatcher.subscribe(BgtaskUpdatedEvent, app, update_sub)
    dispatcher.subscribe(BgtaskDoneEvent, app, done_sub)
    task_id = await root_ctx.background_task_manager.start(_mock_task, name='MockTask1234')
    await asyncio.sleep(2)

    try:
        assert update_handler_ctx['task_id'] == task_id
        assert update_handler_ctx['event_name'] == 'bgtask_updated'
        assert update_handler_ctx['total_progress'] == 2
        assert update_handler_ctx['message'] in ['BGTask ex1', 'BGTask ex2']
        if update_handler_ctx['message'] == 'BGTask ex1':
            assert update_handler_ctx['current_progress'] == 1
        else:
            assert update_handler_ctx['current_progress'] == 2
        assert done_handler_ctx['task_id'] == task_id
        assert done_handler_ctx['event_name'] == 'bgtask_done'
        assert done_handler_ctx['message'] == 'hooray'
    finally:
        await producer.redis_client.flushdb()
        await producer.close()
        await dispatcher.close()


@pytest.mark.asyncio
async def test_background_task_fail(etcd_fixture, create_app_and_client) -> None:
    app, client = await create_app_and_client(
        [shared_config_ctx, event_dispatcher_ctx, background_task_ctx],
        ['.events'],
    )
    root_ctx: RootContext = app['_root.context']
    producer: EventProducer = root_ctx.event_producer
    dispatcher: EventDispatcher = root_ctx.event_dispatcher
    fail_handler_ctx = {}

    async def fail_sub(
        context: web.Application,
        source: AgentId,
        event: BgtaskFailedEvent,
    ) -> None:
        fail_handler_ctx['event_name'] = event.name
        fail_handler_ctx.update(**attr.asdict(event))

    async def _mock_task(reporter):
        reporter.total_progress = 2
        await asyncio.sleep(1)
        await reporter.update(1, message='BGTask ex1')
        raise ZeroDivisionError('oops')

    dispatcher.subscribe(BgtaskFailedEvent, app, fail_sub)
    task_id = await root_ctx.background_task_manager.start(_mock_task, name='MockTask1234')
    await asyncio.sleep(2)
    try:
        assert fail_handler_ctx['task_id'] == task_id
        assert fail_handler_ctx['event_name'] == 'bgtask_failed'
        assert fail_handler_ctx['message'] is not None
        assert 'ZeroDivisionError' in fail_handler_ctx['message']
    finally:
        await producer.redis_client.flushdb()
        await producer.close()
        await dispatcher.close()
