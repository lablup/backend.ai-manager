import asyncio
from multiprocessing import Process
import os
import signal

import aiozmq
import pytest
import zmq

from ai.backend.common import msgpack
from ai.backend.gateway.events import (
    event_router, event_subscriber, EventDispatcher, EVENT_IPC_ADDR
)


@pytest.fixture
def dispatcher(pre_app, event_loop):
    return EventDispatcher(pre_app, loop=event_loop)


class TestEventDispatcher:

    def test_add_and_dispatch_handler(self, pre_app, dispatcher, event_loop):
        event_name = 'test-event'
        assert len(dispatcher.handlers) == 0

        # Add test handler
        def cb(app, agent_id):
            print('executing test event callback...')
            app['test-flag'] = True
        dispatcher.add_handler(event_name, cb)

        assert dispatcher.handlers[event_name][0] == cb

        # Dispatch the event
        pre_app['test-flag'] = False
        dispatcher.dispatch(event_name, agent_id='')

        # Run loop only once (https://stackoverflow.com/a/29797709/7397571)
        event_loop.call_soon(event_loop.stop)
        event_loop.run_forever()

        assert pre_app['test-flag']


@pytest.mark.asyncio
async def test_event_router(pre_app, event_loop, unused_port, mocker):
    TEST_EVENT_IPC_ADDR = EVENT_IPC_ADDR + '-test-router'
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR', TEST_EVENT_IPC_ADDR)

    pre_app.config.events_port = unused_port
    args = (pre_app.config,)

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
async def test_event_subscriber(pre_app, dispatcher, event_loop, mocker):
    TEST_EVENT_IPC_ADDR = EVENT_IPC_ADDR + '-test-event-subscriber'
    mocker.patch('ai.backend.gateway.events.EVENT_IPC_ADDR', TEST_EVENT_IPC_ADDR)

    event_name = 'test-event'
    pre_app['test-flag'] = False

    # Add test handler to dispatcher
    def cb(app, agent_id, *args):
        app['test-flag'] = True
    dispatcher.add_handler(event_name, cb)

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
            if pre_app['test-flag']:
                break
            await asyncio.sleep(0.5)
    send_task = event_loop.create_task(_send_msg())
    await send_task

    # Stop event subscriber task
    sub_task.cancel()
    await sub_task

    assert pre_app['test-flag']
