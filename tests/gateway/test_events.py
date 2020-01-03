import asyncio

from aiohttp import web
import pytest

from ai.backend.gateway.server import (
    config_server_ctx, event_dispatcher_ctx,
)
from ai.backend.common.types import (
    AgentId,
)


@pytest.mark.asyncio
async def test_dispatch(etcd_fixture, create_app_and_client):
    app, client = await create_app_and_client(
        [config_server_ctx, event_dispatcher_ctx],
        ['.events'],
    )
    dispatcher = app['event_dispatcher']
    assert len(dispatcher.subscribers) == 0

    records = {'test-var': set()}
    event_name = 'test-event-01'

    async def acb(app_ctx: web.Application, agent_id: AgentId, event_name: str):
        assert app_ctx is app
        assert agent_id == AgentId('i-test')
        assert event_name == 'test-event-01'
        records['test-var'].add('async')

    def scb(app_ctx: web.Application, agent_id: AgentId, event_name: str):
        assert app_ctx is app
        assert agent_id == AgentId('i-test')
        assert event_name == 'test-event-01'
        records['test-var'].add('sync')

    dispatcher.subscribe(event_name, app, acb)
    dispatcher.subscribe(event_name, app, scb)

    # Dispatch the event
    await dispatcher.produce_event(event_name, agent_id='i-test')
    await asyncio.sleep(0.2)
    assert len(records['test-var']) == 2
    assert 'async' in records['test-var']
    assert 'sync' in records['test-var']

    await dispatcher.redis_producer.flushdb()
    await dispatcher.close()


@pytest.mark.asyncio
async def test_error_on_dispatch(etcd_fixture, create_app_and_client, event_loop):

    def handle_exception(loop, context):
        exc = context['exception']
        exception_log.append(type(exc).__name__)

    app, client = await create_app_and_client(
        [config_server_ctx, event_dispatcher_ctx],
        ['.events'],
        scheduler_opts={'exception_handler': handle_exception},
    )
    dispatcher = app['event_dispatcher']
    old_handler = event_loop.get_exception_handler()
    event_loop.set_exception_handler(handle_exception)
    assert len(dispatcher.subscribers) == 0

    exception_log = []
    event_name = 'test-event-02'

    async def acb(app_ctx: web.Application, agent_id: AgentId, event_name: str):
        assert app_ctx is app
        assert agent_id == AgentId('i-test')
        assert event_name == 'test-event-02'
        raise ZeroDivisionError

    def scb(app_ctx: web.Application, agent_id: AgentId, event_name: str):
        assert app_ctx is app
        assert agent_id == AgentId('i-test')
        assert event_name == 'test-event-02'
        raise OverflowError

    dispatcher.subscribe(event_name, app, scb)
    dispatcher.subscribe(event_name, app, acb)
    await dispatcher.produce_event(event_name, agent_id='i-test')
    await asyncio.sleep(0.2)
    assert len(exception_log) == 2
    assert 'ZeroDivisionError' in exception_log
    assert 'OverflowError' in exception_log

    event_loop.set_exception_handler(old_handler)

    await dispatcher.redis_producer.flushdb()
    await dispatcher.close()
