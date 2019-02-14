import asyncio
from multiprocessing import Process
import os
import signal

import aiojobs.aiohttp
import pytest
import zmq, zmq.asyncio

from ai.backend.common import msgpack
from ai.backend.gateway.events import (
    event_router, event_subscriber, EventDispatcher,
    ipc_events_sockpath
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
    test_ipc_events_sockpath = ipc_events_sockpath.with_name(
        ipc_events_sockpath.name + '.test-router')
    TEST_EVENT_IPC_ADDR = f'ipc://{test_ipc_events_sockpath}'
    mocker.patch('ai.backend.gateway.events.ipc_events_sockpath',
                 test_ipc_events_sockpath)
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR',
                 TEST_EVENT_IPC_ADDR)

    app['config'].events_port = unused_tcp_port
    args = (app['config'],)
    ctx = zmq.asyncio.Context()

    try:
        # Router process
        p = Process(target=event_router, args=('', 0, args,))
        p.start()

        # Task for testing router
        msg = [b'test message']
        recv_msg = None

        async def _router_test():
            nonlocal recv_msg

            pub_sock = ctx.socket(zmq.PUSH)
            pub_sock.connect(f'tcp://127.0.0.1:{args[0].events_port}')
            sub_sock = ctx.socket(zmq.PULL)
            sub_sock.connect(TEST_EVENT_IPC_ADDR)

            await pub_sock.send_multipart(msg)
            recv_msg = await sub_sock.recv_multipart()

            pub_sock.close()
            sub_sock.close()

        task_test = event_loop.create_task(_router_test())
        await task_test
    finally:
        os.kill(p.pid, signal.SIGINT)
        ctx.term()

    assert recv_msg == msg


@pytest.mark.asyncio
async def test_event_subscriber(app, dispatcher, event_loop, mocker):
    test_ipc_events_sockpath = ipc_events_sockpath.with_name(
        ipc_events_sockpath.name + '.test-subscriber')
    TEST_EVENT_IPC_ADDR = f'ipc://{test_ipc_events_sockpath}'
    mocker.patch('ai.backend.gateway.events.ipc_events_sockpath',
                 test_ipc_events_sockpath)
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR',
                 TEST_EVENT_IPC_ADDR)

    event_name = 'test-event'
    test_context = {}
    test_context['test-flag'] = False
    ctx = zmq.asyncio.Context()
    ctx.linger = 50

    # Add test handler to dispatcher
    def cb(app, agent_id, *args):
        test_context['test-flag'] = True

    dispatcher.add_handler(event_name, app, cb)

    # Create a task that runs event subscriber
    sub_task = event_loop.create_task(event_subscriber(dispatcher))

    # Create a task that send message to event subscriber
    async def _send_msg():
        pub_sock = ctx.socket(zmq.PUSH)
        pub_sock.bind(TEST_EVENT_IPC_ADDR)
        msg = (event_name.encode('ascii'),
               b'fake-agent-id',
               msgpack.packb(['test message']))
        await pub_sock.send_multipart(msg)

        # Checking flas is set by the subscriber
        while True:
            if test_context['test-flag']:
                break
            await asyncio.sleep(0.5)

        pub_sock.close()

    send_task = event_loop.create_task(_send_msg())
    await send_task

    # Stop event subscriber task
    sub_task.cancel()
    await sub_task

    try:
        assert test_context['test-flag']
    finally:
        ctx.term()
