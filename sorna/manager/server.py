#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import argparse
import asyncio, aiozmq, zmq, aioredis
import os, signal, sys, re
import logging, logging.config
from sorna import utils, defs
from sorna.argparse import port_no, host_port_pair
from sorna.exceptions import *
from sorna.proto import Message, odict
from sorna.proto.msgtypes import ManagerRequestTypes, SornaResponseTypes
from .registry import InstanceRegistry


# Get the address of Redis server from docker links named "redis".
REDIS_HOST = os.environ.get('REDIS_PORT_6379_TCP_ADDR', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT_6379_TCP_PORT', '6379'))

# Shortcuts for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_r = lambda fmt, req_id, *args, **kwargs: 'request[{}]: '.format(req_id) + fmt.format(*args, **kwargs)

log = logging.getLogger('sorna.manager.server')
log.setLevel(logging.DEBUG)


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
                    kernel = await registry.get_or_create_kernel(req['user_id'],
                                                                 req['entry_id'],
                                                                 req['lang'])
                    log.info(_r('got/created kernel {} successfully.', request_id, kernel.id))
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['kernel_id'] = kernel.id
                    resp['kernel_addr'] = kernel.addr
                except InstanceNotAvailableError:
                    log.error(_r('instance not available', request_id))
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = 'There is no available instance.'
                except KernelCreationFailedError:
                    log.error(_r('kernel creation failed', request_id))
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = 'Kernel creation failed. Try again later.'
                except QuotaExceededError:
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
                except KernelNotFoundError:
                    log.error(_r('kernel not found.', request_id))
                    resp['reply'] = SornaResponseTypes.INVALID_INPUT
                    resp['cause'] = 'No such kernel.'
                except KernelDestructionFailedError:
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
            has_msg = await asyncio.wait_for(g, 0.5, loop=loop)
        except asyncio.TimeoutError:
            continue
        if msg is None:
            break
        evname = msg[0].decode('ascii').split(':')[1]
        evkey  = msg[1]
        if evname == 'expired':
            if evkey.startswith('shadow:'):
                inst_id = evkey.split(':', 1)[1]
                log.info('instance {} has expired (terminated).'.format(inst_id))
                # Let's actually delete the original key.
                await redis.select(defs.SORNA_INSTANCE_DB)
                kern_ids = await redis.smembers(inst_id + '.kernels')
                pipe = redis.multi_exec()
                pipe.delete(inst_id)
                pipe.delete(inst_id + '.kernels')
                await pipe.execute()
                await redis.select(defs.SORNA_KERNEL_DB)
                if kern_ids:
                    await redis.delete(*kern_ids)
                # Session entries will become stale, but accessing it will raise
                # KernelNotFoundError and the registry will create a new kernel
                # afterwards via get_or_create_kernel().
    await redis_sub.unsubscribe(chprefix)
    redis_sub.close()
    await redis_sub.wait_closed()
    redis.close()
    await redis.wait_closed()
    await term_barrier.wait()


def handle_signal(loop, term_ev, server):
    term_ev.set()
    server.close()
    loop.stop()


async def graceful_shutdown(loop, term_barrier):
    asyncio.ensure_future(asyncio.gather(*asyncio.Task.all_tasks(),
                                         loop=loop, return_exceptions=True))
    await term_barrier.wait()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--manager-port', type=port_no, default=5001)
    argparser.add_argument('--redis-addr', type=host_port_pair, default=(REDIS_HOST, REDIS_PORT))
    args = argparser.parse_args()

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': { 'precise': {
                'format': '%(asctime)s %(levelname)-8s %(name)-15s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'precise',
                'stream': 'ext://sys.stdout',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
            'logstash': {
                'class': 'sorna.logging.LogstashHandler',
                'level': 'INFO',
                'endpoint': 'tcp://logger.lablup:2121',
            },
        },
        'loggers': {
            'sorna': {
                'handlers': ['console', 'logstash'],
                'level': 'INFO',
            },
        },
    })

    loop = asyncio.get_event_loop()
    log.info('starting manager on port {0}...'.format(args.manager_port))
    server = loop.run_until_complete(
        aiozmq.create_zmq_stream(zmq.REP, bind='tcp://*:{0}'.format(args.manager_port),
                                 loop=loop))

    my_ip = loop.run_until_complete(utils.get_instance_ip())
    manager_addr = 'tcp://{0}:{1}'.format(my_ip, args.manager_port)
    log.info(_f('manager address: {}', manager_addr))
    log.info(_f('redis address: {}', args.redis_addr))

    registry = InstanceRegistry(args.redis_addr,
                                manager_addr=manager_addr,
                                loop=loop)
    loop.run_until_complete(registry.init())
    log.info('registry initialized.')

    term_ev = asyncio.Event()
    term_barrier = utils.AsyncBarrier(3)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev, server)
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev, server)
    loop.create_task(handle_api(loop, term_ev, term_barrier, server, registry))
    loop.create_task(handle_notifications(loop, term_ev, term_barrier, registry))
    try:
        log.info('started.')
        loop.run_forever()
        loop.run_until_complete(graceful_shutdown(loop, term_barrier))
    finally:
        loop.close()
        log.info('exit.')

if __name__ == '__main__':
    main()
