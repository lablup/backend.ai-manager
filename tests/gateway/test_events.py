import asyncio
from multiprocessing import Process
import os
import signal
import sys

import aiojobs.aiohttp
import aiozmq
import pytest
import zmq

from ai.backend.common import msgpack
from ai.backend.gateway.events import (
    event_router, event_subscriber, EventDispatcher, EVENT_IPC_ADDR
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


@pytest.mark.asyncio
async def test_event_router(app, event_loop, unused_tcp_port, mocker):
    TEST_EVENT_IPC_ADDR = EVENT_IPC_ADDR + '-test-router'
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR', TEST_EVENT_IPC_ADDR)

    app['config'].events_port = unused_tcp_port
    args = (app['config'],)

    try:
        # Router process
        p = Process(target=event_router, args=('', 0, args,))
        p.start()

        # Task for testing router
        msg = [b'test message']
        recv_msg = None

        async def _router_test():
            nonlocal recv_msg

            pub_sock = await aiozmq.create_zmq_stream(
                zmq.PUSH, connect=f'tcp://127.0.0.1:{args[0].events_port}')
            sub_sock = await aiozmq.create_zmq_stream(
                zmq.PULL, connect=TEST_EVENT_IPC_ADDR)

            pub_sock.write(msg)
            recv_msg = await sub_sock.read()

        task_test = event_loop.create_task(_router_test())
        await task_test
    finally:
        os.kill(p.pid, signal.SIGINT)

    assert recv_msg == msg


@pytest.mark.asyncio
async def test_event_subscriber(app, dispatcher, event_loop, mocker):
    TEST_EVENT_IPC_ADDR = EVENT_IPC_ADDR + '-test-event-subscriber'
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR', TEST_EVENT_IPC_ADDR)

    event_name = 'test-event'
    app['test-flag'] = False

    # Add test handler to dispatcher
    def cb(app, agent_id, *args):
        app['test-flag'] = True

    dispatcher.add_handler(event_name, app, cb)

    # Create a task that runs event subscriber
    sub_task = event_loop.create_task(event_subscriber(dispatcher))

    # Create a task that send message to event subscriber
    async def _send_msg():
        pub_sock = await aiozmq.create_zmq_stream(zmq.PUSH,
                                                  bind=TEST_EVENT_IPC_ADDR)
        msg = (event_name.encode('ascii'),
               b'fake-agent-id',
               msgpack.packb(['test message']))
        pub_sock.write(msg)

        # Checking flas is set by the subscriber
        while True:
            if app['test-flag']:
                break
            await asyncio.sleep(0.5)
    send_task = event_loop.create_task(_send_msg())
    await send_task

    # Stop event subscriber task
    sub_task.cancel()
    await sub_task

    assert app['test-flag']
