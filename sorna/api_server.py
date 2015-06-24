#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

from .proto.api_pb2 import ManagerRequest, ManagerResponse
from .proto.api_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE
from .proto.agent_pb2 import AgentRequest, AgentResponse
from .proto.agent_pb2 import HEARTBEAT, SOCKET_INFO
from . import kernel_agent
from .utils.protobuf import read_message, write_message
import argparse
import asyncio, aiozmq, zmq
from abc import ABCMeta, abstractmethod
import docker
from enum import Enum
import json
from namedlist import namedtuple, namedlist
import signal
import struct
import subprocess
import uuid

KernelDriverTypes = Enum('KernelDriverTypes', 'local docker')

Instance = namedlist('Instance', [
    ('ip', None),
    ('docker_port', 2375), # standard docker daemon port
    ('tag', ''),
    ('max_kernels', 2),
    ('cur_kernels', 0),
])
# VM instances should run a docker daemon using "-H tcp://0.0.0.0:2375" in DOCKER_OPTS.

Kernel = namedlist('Kernel', [
    ('instance', None),
    ('kernel_id', None),
    ('spec', 'python34'),  # later, extend this to multiple languages and setups
    ('agent_sock', None),
    ('stdin_sock', None),
    ('stdout_sock', None),
    ('stderr_sock', None),
    ('priv', None),
])

class KernelDriver(metaclass=ABCMeta):

    def __init__(self, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()

    @asyncio.coroutine
    @abstractmethod
    def find_avail_instance(self):
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def create_kernel(self, instance):
        '''
        Launches the kernel and return its ID.
        '''
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def destroy_kernel(self, kernel_id):
        raise NotImplementedError()

    @asyncio.coroutine
    def ping_kernel(self, kernel_id):
        kernel = kernel_registry[kernel_id]
        sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.agent_sock, loop=loop)
        req_id = str(uuid.uuid4())
        req = AgentRequest()
        req.req_type = HEARTBEAT
        req.body = req_id
        sock.write([req.SerializeToString()])
        try:
            resp_data = yield from asyncio.wait_for(sock.read(), timeout=2.0, loop=self.loop)
            resp = AgentResponse()
            resp.ParseFromString(resp_data[0])
            return (resp.body == req_id)
        except asyncio.TimeoutError:
            return False


class DockerKernelDriver(KernelDriver):

    @asyncio.coroutine
    def find_avail_instance(self):
        for instance in instance_registry:
            if instance.cur_kernels < instance.max_kernels:
                instance.cur_kernels += 1
                return instance
        return None

    @asyncio.coroutine
    def create_kernel(self, instance):
        cli = docker.Client(
            base_url='tcp://{0}:{1}'.format(instance.ip, instance.docker_port),
            timeout=5, version='auto'
        )
        # TODO: create the container image
        # TODO: change the command to "python3 -m sorna.kernel_agent"
        container = cli.create_container(image='lablup-python-kernel:latest',
                                         command='/usr/bin/python3')
        kernel = Kernel(instance=instance, kernel_id=container.id)
        kernel.priv = container.id
        kernel.kernel_id = '{0}:{1}'.format(instance.ip, kernel.priv)
        kernel.agent_sock = 'tcp://{0}:{1}'.format(instance.ip, 5002)
        # TODO: run the container and set the port mappings
        kernel_registry[kernel.kernel_id] = kernel
        return kernel_id

    @asyncio.coroutine
    def destroy_kernel(self, kernel_id):
        del kernel_registry[req.kernel_id]
        raise NotImplementedError()


class LocalKernelDriver(KernelDriver):

    @asyncio.coroutine
    def find_avail_instance(self):
        for instance in instance_registry.values():
            if instance.ip != '127.0.0.1':
                continue
            if instance.cur_kernels < instance.max_kernels:
                instance.cur_kernels += 1
                return instance
        return None

    @asyncio.coroutine
    def create_kernel(self, instance):
        unique_id = str(uuid.uuid4())
        kernel_id = '127.0.0.1:{0}'.format(unique_id)
        kernel = Kernel(instance=instance, kernel_id=unique_id)
        cmdargs = ('/usr/bin/python3', '-m', 'sorna.kernel_agent',
                   '--kernel-id', kernel_id)
        proc = yield from asyncio.create_subprocess_exec(*cmdargs, loop=loop)
        kernel.kernel_id = kernel_id
        kernel.agent_sock = 'tcp://{0}:{1}'.format(instance.ip, 5002)
        kernel.priv = proc
        kernel_registry[kernel_id] = kernel
        return kernel_id

    @asyncio.coroutine
    def destroy_kernel(self, kernel_id):
        kernel = kernel_registry[kernel_id]
        kernel.instance.cur_kernels -= 1
        assert(kernel.instance.cur_kernels >= 0)
        proc = kernel.priv
        proc.terminate()
        del kernel_registry[req.kernel_id]
        yield from proc.wait()


# Module states

kernel_driver = KernelDriverTypes.docker
instance_registry = {
    'test': Instance(ip='127.0.0.1')
}
kernel_registry = dict()


# Module functions

@asyncio.coroutine
def handle_api(loop, router):
    while True:
        client_id, _, req_data = yield from router.read()
        req = ManagerRequest()
        req.ParseFromString(req_data)
        resp = ManagerResponse()

        if kernel_driver == KernelDriverTypes.docker:
            driver = DockerKernelDriver(loop)
        elif kernel_driver == KernelDriverTypes.local:
            driver = LocalKernelDriver(loop)
        else:
            assert False, 'Should not reach here.'

        if req.action == PING:

            resp.reply     = PONG
            resp.kernel_id = ''
            resp.body      = req.body

        elif req.action == CREATE:

            instance = yield from driver.find_avail_instance()
            if instance is None:
                raise RuntimeError('No instance is available to launch a new kernel.')
            kernel_id = yield from driver.create_kernel(instance)

            yield from asyncio.sleep(0.2, loop=loop)
            tries = 0
            print('Checking if the kernel is up...')
            while tries < 5:
                success = yield from driver.ping_kernel(kernel_id)
                if success:
                    break
                else:
                    print('  retrying after 1 sec...')
                    yield from asyncio.sleep(1, loop=loop)
                    tries += 1
            else:
                resp.reply     = FAILURE
                resp.kernel_id = ''
                resp.body      = 'The created kernel did not respond!'
                router.write([client_id, b'', resp.SerializeToString()])
                return

            # TODO: restore the user module state?

            resp.reply     = SUCCESS
            resp.kernel_id = kernel_id
            resp.body      = json.dumps({ # TODO: implement
                'agent_socket': 'tcp://{0}:{1}'.format(instance.ip, 5002),
                'stdin_sock': '<not-implemented>',
                'stdout_sock': '<not-implemented>',
                'stderr_sock': '<not-implemented>',
            })

        elif req.action == DESTROY:

            if req.kernel_id in kernel_registry:
                yield from driver.destroy_kernel(req.kernel_id)
                resp.reply = SUCCESS
                resp.kernel_id = req.kernel_id
                resp.body = ''
            else:
                resp.reply = INVALID_INPUT
                resp.kernel_id = ''
                resp.body = 'No such kernel.'

        router.write([client_id, b'', resp.SerializeToString()])

def handle_exit():
    loop.stop()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--kernel-driver', default='docker', choices=('docker', 'local'))
    args = argparser.parse_args()

    kernel_driver = KernelDriverTypes[args.kernel_driver]

    loop = asyncio.get_event_loop()
    start_coro = aiozmq.create_zmq_stream(zmq.ROUTER, bind='tcp://0.0.0.0:5001', loop=loop)
    router = loop.run_until_complete(start_coro)
    print('Started serving...')
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    try:
        asyncio.async(handle_api(loop, router), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    router.close()
    loop.close()
    print('Exit.')
