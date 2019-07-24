import asyncio
from collections import defaultdict
from dataclasses import dataclass
import functools
import logging
from pathlib import Path
import secrets
from typing import Callable

from aiohttp import web
from aiojobs.aiohttp import get_scheduler_from_app
import zmq, zmq.asyncio

from ai.backend.common import msgpack
from ai.backend.common.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.events'))

ipc_base_path = Path('/tmp/backend.ai/ipc')
ipc_key = secrets.token_hex(8)
ipc_events_sockpath = ipc_base_path / f'events-{ipc_key}.sock'
EVENT_IPC_ADDR = f'ipc://{ipc_events_sockpath}'


def event_router(_, pidx, args):
    # run as extra_procs by aiotools
    ctx = zmq.Context()
    ctx.linger = 50
    in_sock = ctx.socket(zmq.PULL)
    in_sock.bind("tcp://{0.host}:{0.port}".format(args[0]['manager']['event-listen-addr']))
    out_sock = ctx.socket(zmq.PUSH)
    ipc_base_path.mkdir(parents=True, exist_ok=True)
    try:
        out_sock.bind(EVENT_IPC_ADDR)
        zmq.proxy(in_sock, out_sock)
    except zmq.error.ZMQError:
        log.error('Cannot bind the event router socket to {}', EVENT_IPC_ADDR)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        log.exception('unexpected error')
        # raven.captureException()
    finally:
        in_sock.close()
        out_sock.close()
        ctx.term()
        ipc_events_sockpath.unlink()


@dataclass
class EventHandler:
    app: web.Application
    callback: Callable


class EventDispatcher:

    def __init__(self, app, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.root_app = app
        self.handlers = defaultdict(list)

    def add_handler(self, event_name, app, callback):
        assert callable(callback)
        self.handlers[event_name].append(EventHandler(app, callback))

    async def dispatch(self, event_name, agent_id, args=tuple()):
        log.debug('DISPATCH({0}/{1})', event_name, agent_id)
        scheduler = get_scheduler_from_app(self.root_app)
        for handler in self.handlers[event_name]:
            cb = handler.callback
            if asyncio.iscoroutine(cb) or asyncio.iscoroutinefunction(cb):
                await scheduler.spawn(cb(handler.app, agent_id, *args))
            else:
                cb = functools.partial(cb, handler.app, agent_id, *args)
                self.loop.call_soon(cb)


async def event_subscriber(dispatcher):
    ctx = zmq.asyncio.Context()
    event_sock = ctx.socket(zmq.PULL)
    event_sock.connect(EVENT_IPC_ADDR)
    try:
        while True:
            try:
                data = await event_sock.recv_multipart()
                if not data:
                    break
                event_name = data[0].decode('ascii')
                agent_id = data[1].decode('utf8')
                args = msgpack.unpackb(data[2])
                await dispatcher.dispatch(event_name, agent_id, args)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error -- resuming operation')
    finally:
        event_sock.close()
        ctx.term()


async def init(app: web.Application):
    pass


async def shutdown(app: web.Application):
    pass


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
