import asyncio
from collections import defaultdict
import logging
import time

import aiotools
import aiozmq
import zmq

from sorna.common import msgpack

log = logging.getLogger('sorna.gateway.events')


def event_router(_, pidx, args):
    # run as extra_procs by aiotools
    ctx = zmq.Context()
    ctx.linger = 50
    in_sock = ctx.socket(zmq.PULL)
    in_sock.bind(f'tcp://*:{args[0].events_port}')
    out_sock = ctx.socket(zmq.PUSH)
    out_sock.bind('ipc://sorna.agent-events')
    try:
        zmq.proxy(in_sock, out_sock)
    except KeyboardInterrupt:
        pass
    except:
        log.exception('unexpected error')
        #raven.captureException()
    finally:
        in_sock.close()
        out_sock.close()
        ctx.term()


class EventDispatcher:

    def __init__(self, app, loop=None):
        self.app = app
        self.loop = loop if loop else asyncio.get_event_loop()
        self.handlers = defaultdict(list)
        self.heartbeat_timeout = app.config.heartbeat_timeout
        # TODO: optimize?
        self.last_seen = app['shared_states']['agent_last_seen']

    def add_handler(self, event_name, callback):
        assert callable(callback)
        self.handlers[event_name].append(callback)

    def dispatch(self, event_name, agent_id, args=tuple()):
        log.debug(f"DISPATCH({event_name}/{agent_id}, {str(args[0]) if args else ''})")
        self.last_seen[agent_id] = time.monotonic()
        for handler in self.handlers[event_name]:
            if asyncio.iscoroutine(handler) or asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(self.app, agent_id, *args))
            else:
                cb = functools.partial(handler, self.app, agent_id, *args)
                self.loop.call_soon(cb)

    async def check_lost(self, interval):
        now = time.monotonic()
        for agent_id, prev in self.last_seen.copy().items():
            if now - prev >= self.heartbeat_timeout:
                del self.last_seen[agent_id]
                self.dispatch('instance_terminated', agent_id, ('agent-lost', ))


async def event_subscriber(dispatcher):
    event_sock = await aiozmq.create_zmq_stream(
        zmq.PULL, connect=f'ipc://sorna.agent-events')
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
    app['agent_lost_checker'] = aiotools.create_timer(dispatcher.check_lost, 1.0)


async def shutdown(app):
    app['event_subscriber'].cancel()
    await app['event_subscriber']
    app['agent_lost_checker'].cancel()
    await app['agent_lost_checker']
