import asyncio

import aiojobs.aiohttp
import pytest

from ai.backend.gateway.events import (
    EventDispatcher,
)


@pytest.fixture
async def dispatcher(app, event_loop):
    aiojobs.aiohttp.setup(app)
    app.freeze()
    await app.on_startup.send(app)
    scheduler = aiojobs.aiohttp.get_scheduler_from_app(app)
    assert scheduler is not None

    yield EventDispatcher(app, loop=event_loop)

    await app.on_cleanup.send(app)


@pytest.mark.asyncio
async def test_dispatch(app, dispatcher):
    event_name = 'test-event-01'
    assert len(dispatcher.handlers) == 0

    async def acb(app, agent_id):
        app['test-var'].add('async')

    def scb(app, agent_id):
        app['test-var'].add('sync')

    dispatcher.add_handler(event_name, app, acb)
    dispatcher.add_handler(event_name, app, scb)

    # Dispatch the event
    app['test-var'] = set()
    await dispatcher.dispatch(event_name, agent_id='')
    await asyncio.sleep(0.1)
    assert len(app['test-var']) == 2
    assert 'async' in app['test-var']
    assert 'sync' in app['test-var']


@pytest.mark.asyncio
async def test_error_on_dispatch(app, dispatcher, event_loop):
    event_name = 'test-event-02'
    assert len(dispatcher.handlers) == 0
    exception_log = []

    def handle_exception(loop, context):
        exc = context['exception']
        exception_log.append(type(exc).__name__)

    event_loop.set_exception_handler(handle_exception)

    async def acb(app, agent_id):
        raise ZeroDivisionError

    def scb(app, agent_id):
        raise OverflowError

    dispatcher.add_handler(event_name, app, scb)
    dispatcher.add_handler(event_name, app, acb)
    await dispatcher.dispatch(event_name, agent_id='')
    await asyncio.sleep(0.1)
    assert len(exception_log) == 2
    assert 'ZeroDivisionError' in exception_log
    assert 'OverflowError' in exception_log

    event_loop.set_exception_handler(None)
