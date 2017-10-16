import asyncio
from collections import defaultdict
import functools
import logging
import sys

import aiozmq
import zmq

from ai.backend.common import msgpack

log = logging.getLogger('ai.backend.gateway.events')

if sys.platform == 'darwin':
    EVENT_IPC_ADDR = 'ipc://ai.backend.agent-events'
elif sys.platform == 'linux':
    EVENT_IPC_ADDR = 'ipc://@/ai.backend.agent-events'
else:
    raise NotImplementedError('unsupported platform')


def event_router(_, pidx, args):
    # run as extra_procs by aiotools
    ctx = zmq.Context()
    ctx.linger = 50
    in_sock = ctx.socket(zmq.PULL)
    in_sock.bind(f'tcp://*:{args[0].events_port}')
    out_sock = ctx.socket(zmq.PUSH)
    out_sock.bind(EVENT_IPC_ADDR)
    try:
        zmq.proxy(in_sock, out_sock)
    except KeyboardInterrupt:
        pass
    except:
        log.exception('unexpected error')
        # raven.captureException()
    finally:
        in_sock.close()
        out_sock.close()
        ctx.term()


class EventDispatcher:

    def __init__(self, app, loop=None):
        self.app = app
        self.loop = loop if loop else asyncio.get_event_loop()
        self.handlers = defaultdict(list)

    def add_handler(self, event_name, callback):
        assert callable(callback)
        self.handlers[event_name].append(callback)

    def dispatch(self, event_name, agent_id, args=tuple()):
        first_arg = f', {args[0]}' if args else ''
        log.debug(f"DISPATCH({event_name}/{agent_id}{first_arg})")
        for handler in self.handlers[event_name]:
            if asyncio.iscoroutine(handler) or asyncio.iscoroutinefunction(handler):
                self.loop.create_task(handler(self.app, agent_id, *args))
            else:
                cb = functools.partial(handler, self.app, agent_id, *args)
                self.loop.call_soon(cb)


async def event_subscriber(dispatcher):
    event_sock = await aiozmq.create_zmq_stream(
        zmq.PULL, connect=EVENT_IPC_ADDR)
    try:
        while True:
            data = await event_sock.read()
            event_name = data[0].decode('ascii')
            agent_id = data[1].decode('utf8')
            args = msgpack.unpackb(data[2])
            dispatcher.dispatch(event_name, agent_id, args)
    except asyncio.CancelledError:
        pass
    except:
        log.exception('unexpected error')
    finally:
        event_sock.close()


async def init(app):
    loop = asyncio.get_event_loop()
    dispatcher = EventDispatcher(app)
    app['event_dispatcher'] = dispatcher
    app['event_subscriber'] = loop.create_task(event_subscriber(dispatcher))


async def shutdown(app):
    app['event_subscriber'].cancel()
    await app['event_subscriber']
