import asyncio
import logging
from collections import defaultdict

import aiozmq
import aiozmq.rpc
import zmq

log = logging.getLogger('sorna.gateway.events')


def agent_event_router(_, pidx, args):
    # run as extra_procs by aiotools

    ctx = zmq.Context()
    ctx.linger = 0

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


class EventSubscriber(aiozmq.rpc.AttrHandler):

    def __init__(self, app, loop=None):
        self.app = app
        self.loop = loop if loop else asyncio.get_event_loop()
        self.handlers = defaultdict(list)

    def add_handler(self, event_name, callback):
        assert callable(callback)
        self.handlers[event_name].append(callback)

    def local_dispatch(self, event_name, *args, **kwargs):
        log.debug(f"DISPATCH({event_name}, {str(args[0]) if args else ''})")
        for handler in self.handlers[event_name]:
            if asyncio.iscoroutine(handler) or asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(self.app, *args, **kwargs))
            else:
                cb = functools.partial(handler, self.app, *args, **kwargs)
                self.loop.call_soon(cb)

    @aiozmq.rpc.method
    def dispatch(self, event_name, *args, **kwargs):
        self.local_dispatch(event_name, *args, **kwargs)


async def init(app):
    app['event_subscriber'] = EventSubscriber(app)
    app['event_sock'] = await aiozmq.rpc.serve_rpc(
        app['event_subscriber'],
        connect=f'ipc://sorna.agent-events')


async def shutdown(app):
    app['event_sock'].close()
    await app['event_sock'].wait_closed()
