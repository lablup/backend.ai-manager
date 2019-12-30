import asyncio

import aiojobs.aiohttp
from aiohttp import web
import pytest

from ai.backend.gateway.events import (
    EventDispatcher,
)
from ai.backend.common.types import (
    AgentId,
)


@pytest.fixture
async def dispatcher(app, test_config, event_loop):
    aiojobs.aiohttp.setup(app)
    app.freeze()
    app['config'] = test_config
    await app.on_startup.send(app)
    scheduler = aiojobs.aiohttp.get_scheduler_from_app(app)
    assert scheduler is not None
    o = await EventDispatcher(app)

    yield o

    await o.redis_producer.flushdb()
    await o.close()
    await app.on_cleanup.send(app)


@pytest.mark.asyncio
async def test_dispatch(app, dispatcher):
    event_name = 'test-event-01'
    assert len(dispatcher.subscribers) == 0

    async def acb(app: web.Application, agent_id: AgentId, event_name: str):
        app['test-var'].add('async')

    def scb(app: web.Application, agent_id: AgentId, event_name: str):
        app['test-var'].add('sync')

    dispatcher.subscribe(event_name, app, acb)
    dispatcher.subscribe(event_name, app, scb)

    # Dispatch the event
    app['test-var'] = set()
    await dispatcher.produce_event(event_name, agent_id='i-test')
    await asyncio.sleep(0.1)
    assert len(app['test-var']) == 2
    assert 'async' in app['test-var']
    assert 'sync' in app['test-var']


@pytest.mark.asyncio
async def test_error_on_dispatch(app, dispatcher, event_loop):
    event_name = 'test-event-02'
    assert len(dispatcher.subscribers) == 0
    exception_log = []

    def handle_exception(loop, context):
        exc = context['exception']
        exception_log.append(type(exc).__name__)

    event_loop.set_exception_handler(handle_exception)

    async def acb(app: web.Application, agent_id: AgentId, event_name: str):
        raise ZeroDivisionError

    def scb(app: web.Application, agent_id: AgentId, event_name: str):
        raise OverflowError

    dispatcher.subscribe(event_name, app, scb)
    dispatcher.subscribe(event_name, app, acb)
    await dispatcher.produce_event(event_name, agent_id='i-test')
    await asyncio.sleep(0.1)
    assert len(exception_log) == 2
    assert 'ZeroDivisionError' in exception_log
    assert 'OverflowError' in exception_log

    event_loop.set_exception_handler(None)
