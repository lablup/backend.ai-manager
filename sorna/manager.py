#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import argparse
import asyncio, aiozmq, zmq, asyncio_redis as aioredis
import os, signal, sys
import logging, logging.config
from .proto import Message, odict
from .proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes
from .driver import DriverTypes, create_driver
from .instance import InstanceRegistry, InstanceNotAvailableError, KernelNotFoundError, QuotaExceededError

# Get the address of Redis server from docker links named "redis".
REDIS_HOST = os.environ.get('REDIS_PORT_6379_TCP_ADDR', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT_6379_TCP_PORT', '6379'))

# VM instances should run a docker daemon using "-H tcp://0.0.0.0:2375" in DOCKER_OPTS.

_terminated = False

# Shortcuts for str.format
__ = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
__r = lambda fmt, req_id, *args, **kwargs: 'request[{}]: '.format(req_id) + fmt.format(*args, **kwargs)

log = logging.getLogger(__name__)

@asyncio.coroutine
def handle_api(loop, server, registry):
    request_id = 0
    while not _terminated:
        try:
            req_data = yield from server.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        req = Message.decode(req_data[0])
        resp = Message()
        request_id += 1

        if req['action'] == ManagerRequestTypes.PING:

            log.info(__r('PING', request_id))
            resp['reply'] = ManagerResponseTypes.PONG
            resp['body']  = req['body']

        elif req['action'] == ManagerRequestTypes.CREATE:

            log.info(__r('CREATE', request_id))
            try:
                instance, kernel = yield from registry.create_kernel()
            except InstanceNotAvailableError as e:
                resp['reply'] = ManagerResponseTypes.FAILURE
                resp['body']  = '\n'.join(map(str, e.args))
                server.write([resp.encode()])
                continue

            yield from asyncio.sleep(0.2, loop=loop)
            tries = 0
            while tries < 5:
                log.info(__r('pinging kernel {} (trial {}) ...', request_id,
                             kernel.id, tries + 1))
                success = yield from registry.ping_kernel(kernel.id)
                if success:
                    break
                else:
                    yield from asyncio.sleep(1, loop=loop)
                    tries += 1
            else:
                log.error(__r('created kernel {} did not respond.', request_id, kernel.id))
                resp['reply'] = ManagerResponseTypes.FAILURE
                resp['body']  = 'The created kernel did not respond!'
                server.write([resp.encode()])
                continue

            # TODO: restore the user module state?

            log.info(__r('created kernel {} successfully.', request_id, kernel.id))
            resp['reply']     = ManagerResponseTypes.SUCCESS
            resp['kernel_id'] = kernel.id
            resp['body']      = odict(
                ('agent_sock', kernel.agent_sock),
                ('stdin_sock', None),
                ('stdout_sock', kernel.stdout_sock),
                ('stderr_sock', kernel.stderr_sock),
            )

        elif req['action'] == ManagerRequestTypes.GET_OR_CREATE:

            log.info(__r('GET_OR_CREATE (user_id: {}, entry_id: {})', request_id,
                     req['user_id'], req['entry_id']))
            try:
                kernel = yield from registry.get_or_create_kernel(req['user_id'],
                                                                  req['entry_id'])
            except InstanceNotAvailableError:
                log.error(__r('instance not available', request_id))
                resp['reply'] = ManagerResponseTypes.FAILURE
                resp['body']  = 'There is no available instance.'
                server.write([resp.encode()])
                continue
            except QuotaExceededError:
                log.error(__r('quota exceeded', request_id))
                resp['reply'] = ManagerResponseTypes.FAILURE
                resp['body']  = 'You cannot create more kernels.'
                server.write([resp.encode()])
                continue

            log.info(__r('got/created kernel {} successfully.', request_id, kernel.id))
            resp['reply'] = ManagerResponseTypes.SUCCESS
            resp['kernel_id'] = kernel.id
            resp['body'] = odict(
                ('agent_sock', kernel.agent_sock),
                ('stdout_sock', kernel.stdout_sock),
                ('stderr_sock', kernel.stderr_sock),
            )

        elif req['action'] == ManagerRequestTypes.DESTROY:

            log.info(__r('DESTROY (kernel_id: {})', request_id, req['kernel_id']))
            if 'kernel_id' not in req:
                req['kernel_id'] = yield from registry.get_kernel_from_session(req['user_id'],
                                                                               req['entry_id'])
            try:
                yield from registry.destroy_kernel(req['kernel_id'])
                log.info(__r('destroyed successfully.', request_id))
                resp['reply']     = ManagerResponseTypes.SUCCESS
                resp['kernel_id'] = req['kernel_id']
                resp['body']      = ''
            except KernelNotFoundError:
                log.error(__r('kernel not found.', request_id))
                resp['reply'] = ManagerResponseTypes.INVALID_INPUT
                resp['body']  = 'No such kernel.'

        elif req['action'] == ManagerRequestTypes.REFRESH:

            log.info(__r('REFRESH (kernel_id: {})', request_id, req['kernel_id']))
            try:
                yield from registry.refresh_kernel(req['kernel_id'])
                log.info(__r('refreshed successfully.', request_id))
                resp['reply'] = ManagerResponseTypes.SUCCESS
                resp['kernel_id'] = req['kernel_id']
                resp['body'] = ''
            except KernelNotFoundError:
                log.error(__r('kernel not found.', request_id))
                resp['reply'] = ManagerResponseTypes.INVALID_INPUT
                resp['body'] = 'No such kernel.'

        server.write([resp.encode()])

@asyncio.coroutine
def handle_timer(loop, registry, period=10.0):
    while True:
        yield from asyncio.sleep(period)
        log.info(__('TIMER (loop_time: {})', loop.time()))
        yield from registry.clean_old_kernels()


def main():
    global _terminated
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--kernel-driver', default='docker',
                           choices=tuple(t.name for t in DriverTypes),
                           help='Use the given driver to control computing resources.')
    argparser.add_argument('--reattach', dest='reattach_registry_id', default=None, type=str,
                           help='Reattach to the existing database using the given registry ID. '
                                'Use this option when the manager has crashed '
                                'but there are running instances and kernels.')
    argparser.add_argument('--cleanup-interval', default=10, type=int,
                           help='Interval (seconds) to do clean up operations '
                                'such as killing idle kernels.')
    argparser.add_argument('--kernel-timeout', default=600, type=int,
                           help='Timeout (seconds) for idle kernels before automatic termination. ')
    argparser.add_argument('--max-kernels', default=0, type=int,
                           help='Set the max# of kernels per instance. Only for the local driver.')
    args = argparser.parse_args()

    assert args.cleanup_interval > 0
    assert args.kernel_timeout >= 0

    def handle_exit():
        raise SystemExit()

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
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
    log = logging.getLogger(__name__)

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()

    log.info('starting manager on port 5001...')
    server = loop.run_until_complete(
        aiozmq.create_zmq_stream(zmq.REP, bind='tcp://*:5001', loop=loop))
    try:
        redis_conn_pool = loop.run_until_complete(asyncio.wait_for(
            aioredis.Pool.create(host=REDIS_HOST, port=REDIS_PORT), 2))
    except asyncio.TimeoutError:
        log.error('could not connect to the redis server at tcp://{0}:{1}'.format(REDIS_HOST, REDIS_PORT))
        return

    log.info(__('creating {} driver...', args.kernel_driver))
    driver = create_driver(args.kernel_driver)
    my_ip = loop.run_until_complete(driver.get_internal_ip())
    manager_addr = 'tcp://{0}:{1}'.format(my_ip, 5001)
    log.info(__('manager address: {}', manager_addr))
    registry = InstanceRegistry(redis_conn_pool, driver,
                                registry_id=args.reattach_registry_id,
                                kernel_timeout=args.kernel_timeout,
                                manager_addr=manager_addr,
                                loop=loop)
    loop.run_until_complete(registry.init())
    log.info('registry initialized.')
    if args.kernel_driver == 'local':
        assert args.max_kernels > 0
        inst = loop.run_until_complete(registry.add_instance(max_kernels=args.max_kernels))
        assert inst is not None
        log.info(__('local: added a local dummy instance with max_kernels={}.',
                    args.max_kernels))

    log.info('started.')
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    try:
        asyncio.async(handle_api(loop, server, registry), loop=loop)
        asyncio.async(handle_timer(loop, registry, args.cleanup_interval), loop=loop)
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        _terminated = True
        loop.run_until_complete(registry.terminate())
        server.close()
        redis_conn_pool.close()
        for t in asyncio.Task.all_tasks():
            t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0))
        except asyncio.CancelledError:
            pass
    finally:
        loop.close()
        log.info('exit.')

if __name__ == '__main__':
    main()
