#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import asyncio
import ipaddress
import logging
import signal
import sys
from urllib.parse import urlsplit

import zmq, aiozmq
import aioredis
import uvloop

from sorna import defs
from sorna.argparse import port_no
from sorna.exceptions import \
    InstanceNotAvailable, \
    KernelCreationFailed, KernelDestructionFailed, \
    KernelNotFound, QuotaExceeded
from sorna.proto import Message
from sorna.utils import AsyncBarrier, get_instance_ip
from sorna.proto.msgtypes import ManagerRequestTypes, SornaResponseTypes

from ..gateway.config import init_logger, load_config
from .registry import InstanceRegistry
from . import __version__


log = logging.getLogger('sorna.manager.server')

# Kernel IP overrides
kernel_ip_override = None

# Shortcuts for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_r = lambda fmt, req_id, *args, **kwargs: \
    'request[{}]: '.format(req_id) + fmt.format(*args, **kwargs)


async def handle_api(loop, term_ev, term_barrier, server, registry):
    request_id = 0
    while not term_ev.is_set():
        try:
            req_data = await server.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        req = Message.decode(req_data[0])
        resp = Message()
        request_id += 1
        if 'action' not in req:

            log.warn(_r('Malformed API request!', request_id))
            resp['reply'] = SornaResponseTypes.INVALID_INPUT
            resp['cause'] = 'Malformed API request.'

        elif req['action'] == ManagerRequestTypes.PING:

            try:
                log.info(_r('PING (body:{})', request_id, req['body']))
            except KeyError:
                log.warn(_r('PING: invalid parameters', request_id))
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Missing API parameters.'
            else:
                resp['reply'] = SornaResponseTypes.PONG
                resp['body']  = req['body']

        elif req['action'] == ManagerRequestTypes.GET_OR_CREATE:

            try:
                log.info(_r('GET_OR_CREATE (u:{}, e:{}, l:{})', request_id,
                         req['user_id'], req['entry_id'], req['lang']))
            except KeyError:
                log.warn(_r('GET_OR_CREATE: invalid parameters', request_id))
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Missing API parameters.'
            else:
                try:
                    client_sess_token = '{0}:{1}:{2}'.format(req['user_id'],
                                                             req['entry_id'],
                                                             req['lang'])
                    kernel = await registry.get_or_create_kernel(client_sess_token,
                                                                 req['lang'])
                    log.info(_r('got/created kernel {} successfully.', request_id, kernel.id))
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['kernel_id'] = kernel.id
                    if kernel_ip_override:
                        p = urlsplit(kernel.addr)
                        try:
                            addr = ipaddress.ip_address(p.hostname)
                            assert addr.is_loopback  # 127.0.0.0/8
                        except ValueError:
                            assert p.hostname == 'localhost'
                        p = p._replace(netloc='{0}:{1}'.format(kernel_ip_override, p.port))
                        resp['kernel_addr'] = p.geturl()
                        resp['stdin_addr']  = 'tcp://{}:{}' \
                                              .format(kernel_ip_override,
                                                      kernel.stdin_port)
                        resp['stdout_addr'] = 'tcp://{}:{}' \
                                              .format(kernel_ip_override,
                                                      kernel.stdout_port)
                    else:
                        p = urlsplit(kernel.addr)
                        kernel_ip = ipaddress.ip_address(p.hostname)
                        resp['kernel_addr'] = kernel.addr
                        resp['stdin_addr']  = 'tcp://{}:{}' \
                                              .format(kernel_ip,
                                                      kernel.stdin_port)
                        resp['stdout_addr'] = 'tcp://{}:{}' \
                                              .format(kernel_ip,
                                                      kernel.stdout_port)
                except InstanceNotAvailable:
                    log.error(_r('instance not available', request_id))
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = 'There is no available instance.'
                except KernelCreationFailed:
                    log.error(_r('kernel creation failed', request_id))
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = 'Kernel creation failed. Try again later.'
                except QuotaExceeded:
                    # currently unused. reserved for future per-user quota impl.
                    log.error(_r('quota exceeded', request_id))
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = 'You cannot create more kernels.'

        elif req['action'] == ManagerRequestTypes.DESTROY:

            try:
                log.info(_r('DESTROY (k:{})', request_id, req['kernel_id']))
            except KeyError:
                log.warn(_r('DESTROY: invalid parameters', request_id))
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Missing API parameters.'
            else:
                # TODO: assert if session matches with the kernel id?
                try:
                    await registry.destroy_kernel(req['kernel_id'])
                    log.info(_r('destroyed successfully.', request_id))
                    resp['reply'] = SornaResponseTypes.SUCCESS
                except KernelNotFound:
                    log.error(_r('kernel not found.', request_id))
                    resp['reply'] = SornaResponseTypes.INVALID_INPUT
                    resp['cause'] = 'No such kernel.'
                except KernelDestructionFailed:
                    log.error(_r('kernel not found.', request_id))
                    resp['reply'] = SornaResponseTypes.INVALID_INPUT
                    resp['cause']  = 'No such kernel.'

        server.write([resp.encode()])

    await registry.terminate()
    await term_barrier.wait()


async def handle_notifications(loop, term_ev, term_barrier, registry):
    redis_sub = await aioredis.create_redis(registry.redis_addr,
                                            encoding='utf8',
                                            loop=loop)
    redis = await aioredis.create_redis(registry.redis_addr,
                                        encoding='utf8',
                                        loop=loop)
    # Enable "expired" event notification
    # See more details at: http://redis.io/topics/notifications
    await redis_sub.config_set('notify-keyspace-events', 'Ex')
    chprefix = '__keyevent@{}__*'.format(defs.SORNA_INSTANCE_DB)
    channels = await redis_sub.psubscribe(chprefix)
    log.info('subscribed redis notifications.')
    g = None
    while not term_ev.is_set():
        msg = None
        try:
            g = asyncio.Task(channels[0].get(encoding='utf8'))
            await asyncio.wait_for(g, 0.5, loop=loop)
        except asyncio.TimeoutError:
            continue
        if msg is None:
            break
        evname = msg[0].decode('ascii').split(':')[1]
        evkey  = msg[1]
        if evname == 'expired':
            if evkey.startswith('shadow:'):
                inst_id = evkey.split(':', 1)[1]
                log.warning('instance {} has expired (terminated).'.format(inst_id))
                # Let's actually delete the original key.
                await redis.select(defs.SORNA_INSTANCE_DB)
                kern_ids = await redis.smembers(inst_id + '.kernels')
                pipe = redis.pipeline()
                pipe.delete(inst_id)
                pipe.delete(inst_id + '.kernels')
                await pipe.execute()
                await redis.select(defs.SORNA_KERNEL_DB)
                if kern_ids:
                    await redis.delete(*kern_ids)
                # Session entries will become stale, but accessing it will raise
                # KernelNotFound and the registry will create a new kernel
                # afterwards via get_or_create_kernel().
    await redis_sub.unsubscribe(chprefix)
    redis_sub.close()
    await redis_sub.wait_closed()
    redis.close()
    await redis.wait_closed()
    await term_barrier.wait()


async def graceful_shutdown(loop, term_barrier):
    asyncio.ensure_future(asyncio.gather(*asyncio.Task.all_tasks(),
                                         loop=loop, return_exceptions=True))
    await term_barrier.wait()


def main():
    global kernel_ip_override

    def manager_args(parser):
        parser.add('--manager-port', env_var='SORNA_MANAGER_PORT', type=port_no, default=5001,
                   help='The TCP port number where the legacy manager listens on. '
                        '(default: 5001)')

    config = load_config(extra_args_func=manager_args)
    init_logger(config)

    def handle_signal(loop, term_ev):
        if term_ev.is_set():
            log.warning('Forced shutdown!')
            sys.exit(1)
        else:
            term_ev.set()
            loop.stop()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    log.info('Sorna Manager {}'.format(__version__))

    server = loop.run_until_complete(
        aiozmq.create_zmq_stream(zmq.REP, bind='tcp://*:{0}'.format(config.manager_port),
                                 loop=loop))

    my_ip = loop.run_until_complete(get_instance_ip())
    log.info(_f('serving on tcp://{0}:{1}', my_ip, config.manager_port))
    log.info(_f('using redis on tcp://{0}:{1}', *config.redis_addr))
    kernel_ip_override = config.kernel_ip_override

    registry = InstanceRegistry(config.redis_addr, loop=loop)
    loop.run_until_complete(registry.init())

    term_ev = asyncio.Event()
    term_barrier = AsyncBarrier(3)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)
    asyncio.ensure_future(handle_api(loop, term_ev, term_barrier, server, registry))
    asyncio.ensure_future(handle_notifications(loop, term_ev, term_barrier, registry))
    try:
        loop.run_forever()
        # interrupted
        server.close()
        loop.run_until_complete(graceful_shutdown(loop, term_barrier))
        loop.run_until_complete(asyncio.sleep(0.1))
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
