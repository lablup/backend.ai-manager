#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import argparse
import asyncio, aiozmq, zmq, asyncio_redis as aioredis
import os, signal, sys
from .proto import Namespace, encode, decode
from .proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes
from .driver import DriverTypes, create_driver
from .instance import InstanceRegistry, InstanceNotAvailableError, KernelNotFoundError

# Get the address of Redis server from docker links named "redis".
REDIS_HOST = os.environ.get('REDIS_PORT_6379_TCP_ADDR', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT_6379_TCP_PORT', '6379'))

# VM instances should run a docker daemon using "-H tcp://0.0.0.0:2375" in DOCKER_OPTS.

_terminated = False

@asyncio.coroutine
def handle_api(loop, server, registry):
    while not _terminated:
        try:
            req_data = yield from server.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        req = decode(req_data[0])
        resp = Namespace()

        if req.action == ManagerRequestTypes.PING:

            resp.reply     = ManagerResponseTypes.PONG
            resp.kernel_id = ''
            resp.body      = req.body

        elif req.action == ManagerRequestTypes.CREATE:

            try:
                instance, kernel = yield from registry.create_kernel(spec=req.body.spec)
            except InstanceNotAvailableError as e:
                resp.reply     = ManagerResponseTypes.FAILURE
                resp.kernel_id = ''
                resp.body      = '\n'.join(map(str, e.args))
                server.write([encode(resp)])
                return

            yield from asyncio.sleep(0.2, loop=loop)
            tries = 0
            while tries < 5:
                success = yield from registry.ping_kernel(kernel.id)
                if success:
                    break
                else:
                    yield from asyncio.sleep(1, loop=loop)
                    tries += 1
            else:
                resp.reply     = ManagerResponseTypes.FAILURE
                resp.kernel_id = ''
                resp.body      = 'The created kernel did not respond!'
                server.write([encode(resp)])
                return

            # TODO: restore the user module state?

            resp.reply     = ManagerResponseTypes.SUCCESS
            resp.kernel_id = kernel.id
            resp.body      = {
                'agent_sock': kernel.agent_sock,
                'stdin_sock': None,
                'stdout_sock': kernel.stdout_sock,
                'stderr_sock': kernel.stderr_sock,
            }

        elif req.action == ManagerRequestTypes.DESTROY:

            try:
                yield from registry.destroy_kernel(req.kernel_id)
                resp.reply = ManagerResponseTypes.SUCCESS
                resp.kernel_id = req.kernel_id
                resp.body = ''
            except KernelNotFoundError:
                resp.reply = ManagerResponseTypes.INVALID_INPUT
                resp.kernel_id = ''
                resp.body = 'No such kernel.'

        elif req.action == ManagerRequestTypes.REFRESH:

            try:
                yield from registry.refresh_kernel(req.kernel_id)
                resp.reply = ManagerResponseTypes.SUCCESS
                resp.kernel_id = req.kernel_id
                resp.body = ''
            except KernelNotFoundError:
                resp.reply = ManagerResponseTypes.INVALID_INPUT
                resp.kernel_id = ''
                resp.body = 'No such kernel.'

        server.write([encode(resp)])

@asyncio.coroutine
def handle_timer(loop, registry, period=10.0):
    last_tick = loop.time()
    while not _terminated:
        yield from asyncio.sleep(1.0)
        if _terminated: break
        now = loop.time()
        if now - last_tick >= period:
            yield from registry.clean_old_kernels()
            last_tick = now


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

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()

    server = loop.run_until_complete(
        aiozmq.create_zmq_stream(zmq.REP, bind='tcp://*:5001', loop=loop))
    redis_conn_pool = loop.run_until_complete(
        aioredis.Pool.create(host=REDIS_HOST, port=REDIS_PORT))
    driver = create_driver(args.kernel_driver)
    my_ip = loop.run_until_complete(driver.get_internal_ip())
    manager_addr = 'tcp://{0}:{1}'.format(my_ip, 5001)
    registry = InstanceRegistry(redis_conn_pool, driver,
                                registry_id=args.reattach_registry_id,
                                kernel_timeout=args.kernel_timeout,
                                manager_addr=manager_addr,
                                loop=loop)
    loop.run_until_complete(registry.init())
    if args.kernel_driver == 'local':
        assert args.max_kernels > 0
        inst = loop.run_until_complete(registry.add_instance(max_kernels=args.max_kernels))
        assert inst is not None
        print('Added a local dummy instance.')

    print('Started serving... (driver: {0})'.format(args.kernel_driver))
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
        pending = asyncio.Task.all_tasks()
        for t in pending:
            try:
                loop.run_until_complete(asyncio.gather(t))
            except asyncio.CancelledError:
                pass
    finally:
        loop.close()
        print('Exit.')

if __name__ == '__main__':
    main()
