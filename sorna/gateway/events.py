import asyncio
import logging
from collections import defaultdict

import msgpack

from sorna.common.msgbus import ExchangeTypes, Subscriber

log = logging.getLogger('sorna.gateway.events')


class AgentEventSubscriber(Subscriber):
    exchange_name = 'agent-events'
    exchange_type = ExchangeTypes.DIRECT
    queue_name = 'events'

    def __init__(self, *args, app=None, **kwargs):
        kwargs['decoder'] = msgpack.unpackb
        super().__init__(*args, topic='events', **kwargs)
        self.handlers = defaultdict(list)
        self.app = app

    async def init(self, *args, **kwargs):
        await super().init(*args, **kwargs)
        await self.subscribe(self._dispatch)

    async def _dispatch(self, body, envelope, props):
        loop = asyncio.get_event_loop()
        for handler in self.handlers[body['type']]:
            if asyncio.iscoroutine(handler) or asyncio.iscoroutinefunction(handler):
                loop.create_task(handler(self.app, body))
            else:
                loop.call_soon(handler(self.app, body))

    def local_dispatch(self, *args):
        raise NotImplementedError

    def add_handler(self, key, cb):
        self.handlers[key].append(cb)


async def init(app):
    evs = AgentEventSubscriber(app.config.mq_addr, app=app)
    await evs.init(
        user=app.config.mq_user,
        passwd=app.config.mq_password,
        vhost=app.config.namespace)
    app['event_subscriber'] = evs


async def shutdown(app):
    await app['event_subscriber'].close()
