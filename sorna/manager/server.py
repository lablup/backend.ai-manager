#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import argparse
import asyncio, aiozmq, zmq, aioredis
import os, signal, sys
import logging, logging.config
from sorna import utils, defs
from sorna.exceptions import InstanceNotAvailableError, KernelNotFoundError, QuotaExceededError
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


async def handle_api(loop, server, registry):
    request_id = 0
    while True:
        try:
            req_data = await server.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        req = Message.decode(req_data[0])
        resp = Message()
        request_id += 1

        if req['action'] == ManagerRequestTypes.PING:

            log.info(_r('PING', request_id))
            resp['reply'] = SornaResponseTypes.PONG
            resp['body']  = req['body']

        elif req['action'] == ManagerRequestTypes.GET_OR_CREATE:

            log.info(_r('GET_OR_CREATE (user_id: {}, entry_id: {})', request_id,
                     req['user_id'], req['entry_id']))
            try:
                kernel = await registry.get_or_create_kernel(req['user_id'],
                                                                  req['entry_id'])
            except InstanceNotAvailableError:
                log.error(_r('instance not available', request_id))
                resp['reply'] = SornaResponseTypes.FAILURE
                resp['body']  = 'There is no available instance.'
                server.write([resp.encode()])
                continue
            except QuotaExceededError:
                log.error(_r('quota exceeded', request_id))
                resp['reply'] = SornaResponseTypes.FAILURE
                resp['body']  = 'You cannot create more kernels.'
                server.write([resp.encode()])
                continue

            log.info(_r('got/created kernel {} successfully.', request_id, kernel.id))
            resp['reply'] = SornaResponseTypes.SUCCESS
            resp['kernel_id'] = kernel.id
            resp['body'] = odict(
                ('agent_sock', kernel.agent_sock),
                ('stdout_sock', kernel.stdout_sock),
                ('stderr_sock', kernel.stderr_sock),
            )

        elif req['action'] == ManagerRequestTypes.DESTROY:

            log.info(_r('DESTROY (kernel_id: {})', request_id, req['kernel_id']))
            if 'kernel_id' not in req:
                req['kernel_id'] = await registry.get_kernel_from_session(req['user_id'],
                                                                               req['entry_id'])
            try:
                await registry.destroy_kernel(req['kernel_id'])
                log.info(_r('destroyed successfully.', request_id))
                resp['reply']     = SornaResponseTypes.SUCCESS
                resp['kernel_id'] = req['kernel_id']
                resp['body']      = ''
            except KernelNotFoundError:
                log.error(_r('kernel not found.', request_id))
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body']  = 'No such kernel.'

        server.write([resp.encode()])

async def handle_timer(loop, registry, period=10.0):
    while True:
        await asyncio.sleep(period)
        log.info(_f('TIMER (loop_time: {})', loop.time()))
        await registry.clean_old_kernels()

async def handle_notifications(loop, registry):
    redis_sub = await aioredis.create_redis(registry.redis_addr,
                                            encoding='utf8',
                                            loop=loop)
    # Enable "expired" event notification
    # See more details at: http://redis.io/topics/notifications
    await redis_sub.config_set('notify-keyspace-events', 'Ehx')
    chprefix = '__keyevent@{}__*'.format(defs.SORNA_INSTANCE_DB)
    channels = await redis_sub.psubscribe(chprefix)
    log.info('subscribed redis notifications.')
    while (await channels[0].wait_message()):
        msg = await channels[0].get(encoding='utf8')
        if msg is None: break
        evname = msg[0].decode('ascii').split(':')[1]
        evkey  = msg[1]
        if evname == 'expired':
            log.info('instance {} has expired (terminated).'.format(evkey))
    await redis_sub.quit()

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--reattach', dest='reattach_registry_id', default=None, type=str,
                           help='Reattach to the existing database using the given registry ID. '
                                'Use this option when the manager has crashed '
                                'but there are running instances and kernels.')
    argparser.add_argument('--cleanup-interval', default=10, type=int,
                           help='Interval (seconds) to do clean up operations '
                                'such as killing idle kernels.')
    argparser.add_argument('--kernel-timeout', default=600, type=int,
                           help='Timeout (seconds) for idle kernels before automatic termination. ')
    args = argparser.parse_args()

    assert args.cleanup_interval > 0
    assert args.kernel_timeout >= 0

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
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

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()

    log.info('starting manager on port 5001...')
    server = loop.run_until_complete(
        aiozmq.create_zmq_stream(zmq.REP, bind='tcp://*:5001', loop=loop))

    my_ip = loop.run_until_complete(utils.get_instance_ip())
    manager_addr = 'tcp://{0}:{1}'.format(my_ip, 5001)
    log.info(_f('manager address: {}', manager_addr))

    registry = InstanceRegistry((REDIS_HOST, REDIS_PORT),
                                kernel_timeout=args.kernel_timeout,
                                manager_addr=manager_addr,
                                loop=loop)
    loop.run_until_complete(registry.init())
    log.info('registry initialized.')

    try:
        asyncio.ensure_future(handle_api(loop, server, registry), loop=loop)
        asyncio.ensure_future(handle_notifications(loop, registry), loop=loop)
        #asyncio.ensure_future(handle_timer(loop, registry, args.cleanup_interval), loop=loop)
        log.info('started.')
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(registry.terminate())
        server.close()
        for t in asyncio.Task.all_tasks():
            if not t.done():
                t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0, loop=loop))
        except asyncio.CancelledError:
            pass
    finally:
        loop.close()
        log.info('exit.')

if __name__ == '__main__':
    main()
