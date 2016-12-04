import asyncio
from collections import defaultdict
import functools
import logging

import aiozmq, aiozmq.rpc

log = logging.getLogger('sorna.gateway.events')


class EventServer(aiozmq.rpc.AttrHandler):

    def __init__(self, app, loop=None):
        self.app = app
        self.loop = loop if loop else asyncio.get_event_loop()
        self.handlers = defaultdict(list)

    def add_handler(self, event_name, callback):
        assert callable(callback)
        self.handlers[event_name].append(callback)

    @aiozmq.rpc.method
    def dispatch(self, event_name, *args, **kwargs):
        log.debug('DISPATCH({})'.format(event_name))
        for handler in self.handlers[event_name]:
            if asyncio.iscoroutine(handler) or asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(self.app, *args, **kwargs))
            else:
                cb = functools.partial(handler, self.app, *args, **kwargs)
                self.loop.call_soon(cb)


async def init(app):
    app['event_server'] = EventServer(app)
    app['event_sock'] = await aiozmq.rpc.serve_rpc(
        app['event_server'],
        bind='tcp://*:{}'.format(app.config.events_port))


async def shutdown(app):
    app['event_sock'].close()
    await app['event_sock'].wait_closed()
