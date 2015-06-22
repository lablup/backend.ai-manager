#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

from .proto.api_pb2 import InputMessage, OutputMessage
from .proto.api_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE
from .proto.agent_pb2 import AgentRequest, AgentResponse
from .proto.agent_pb2 import HEARTBEAT, SOCKET_INFO
from . import kernel_agent
from .utils.protobuf import read_message, write_message
import asyncio, aiozmq, zmq
from abc import ABCMeta, abstractmethod
import docker
from namedlist import namedtuple, namedlist
import signal
import struct
import subprocess
import uuid

kernel_driver = 'local'  # or 'docker'

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
    ('container_id', None),
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
    def create_kernel(self, instance):
        '''
        Launches the kernel and return its ID.
        '''
        pass

    @asyncio.coroutine
    @abstractmethod
    def destroy_kernel(self, kernel_id):
        pass

    @asyncio.coroutine
    @abstractmethod
    def ping_kernel(self, kernel_id):
        pass


class DockerKernelDriver(KernelDriver):

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
        kernel = Kernel(instance=instance, container=container.id)
        kernel_id = '{0}:{1}'.format(instance.ip, kernel.container)
        # TODO: run the container and set the port mappings
        return kernel_id

    @asyncio.coroutine
    def destroy_kernel(self, kernel_id):
        raise NotImplementedError()

    @asyncio.coroutine
    def ping_kernel(self, kernel_id):
        raise NotImplementedError()


class LocalKernelDriver(KernelDriver):

    @asyncio.coroutine
    def create_kernel(self, instance):
        unique_id = str(uuid.uuid4())
        kernel_id = '127.0.0.1:{0}'.format(unique_id)
        kernel = Kernel(instance=instance, container=unique_id)
        cmdargs = ('/usr/bin/python3', '-m', kernel_agent.__file__,
                   '--kernel-id', kernel_id)
        proc = yield from loop.create_subprocess_exec(*cmdargs)
        kernel.priv = proc
        return kernel_id

    @asyncio.coroutine
    def destroy_kernel(self, kernel_id):
        kernel = kernel_registry[kernel_id]
        kernel.instance.cur_kernels -= 1
        assert(kernel.instance.cur_kernels >= 0)
        proc = kernel.priv
        proc.terminate()
        yield from proc.wait()

    @asyncio.coroutine
    def ping_kernel(self, kernel_id):
        kernel = kernel_registry[kernel_id]
        dealer = yield from aiozmq.create_zmq_stream(zmq.DEALER, connect=kernel.agent_sock, loop=loop)
        req_id = str(uuid.uuid4())
        req = AgentRequest()
        req.req_type = HEARTBEAT
        req.body = req_id
        dealer.write([req])
        # TODO: handle timeout
        resp = yield from dealer.read()
        return (resp.body == req_id)

instance_registry = {
    'test': Instance(ip='127.0.0.1')
}
kernel_registry = dict()

def find_avail_instance():
    for instance in instance_registry:
        if kernel_driver == 'local' and instance.ip != '127.0.0.1':
            continue
        if instance.cur_kernels < instance.max_kernels:
            instance.cur_kernels += 1
            return instance
    return None

@asyncio.coroutine
def handle_api(loop, router):
    while True:
        client_id, input_data = yield from router.read()
        input_msg = InputMessage()
        input_msg.ParseFromString(input_data)
        output_msg = OutputMessage()

        if input_msg.action == PING:

            output_msg.reply     = PONG
            output_msg.kernel_id = ''
            output_msg.body      = input_msg.body

        elif input_msg.action == CREATE:

            instance = find_avail_instance()
            if instance is None:
                raise RuntimeError('No instance is available to launch a new kernel.')
            if kernel_driver == 'docker':
                driver = DockerKernelDriver(loop)
            elif kernel_driver == 'local':
                driver = LocalKernelDriver(loop)
            else:
                assert False, 'Should not reach here.'
            kernel_id = driver.create_kernel()
            kernel_registry[kernel_id] = kernel

            yield from asyncio.sleep(0.2, loop=loop)
            tries = 0
            while tries < 5:
                success = yield from driver.ping_kernel(kernel_id)
                if success:
                    break
                else:
                    yield from asyncio.sleep(1, loop=loop)
                    tries += 1
            else:
                output_msg.reply     = FAILURE
                output_msg.kernel_id = ''
                output_msg.body      = ''
                router.write([client_id, output_msg])
                return

            # TODO: restore the user module state?

            output_msg.reply     = SUCCESS
            output_msg.kernel_id = kernel_id
            output_msg.body      = json.loads({ # TODO: implement
                'stdin_sock': '',
                'stdout_sock': '',
                'stderr_sock': '',
            })

        elif input_msg.action == DESTROY:

            if kernel_driver == 'docker':
                driver = DockerKernelDriver(loop)
            elif kernel_driver == 'local':
                driver = LocalKernelDriver(loop)
            else:
                assert False, 'Should not reach here.'

            if input_msg.kernel_id in kernel_registry:
                yield from driver.destroy_kernel(input_msg.kernel_id)
                output_msg.reply = SUCCESS
                output_msg.kernel_id = input_msg.kernel_id
                output_msg.body = ''
            else:
                output_msg.reply = INVALID_INPUT
                output_msg.kernel_id = ''
                output_msg.body = 'No such kernel.'

        router.write([client_id, output_msg])

def handle_exit():
    loop.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    start_coro = aiozmq.create_zmq_stream(zmq.ROUTER, bind='tcp://0.0.0.0:5001', loop=loop)
    router = loop.run_until_complete(start_coro)
    print('Started serving...')
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_exit)
        asyncio.async(handle_api(loop, router), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    router.close()
    loop.close()
    print('Exit.')
