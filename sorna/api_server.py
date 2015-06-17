#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

from .proto.api_pb2 import InputMessage, OutputMessage, ActionType, ReplyType
from .proto.agent_pb2 import AgentRequest, AgentResponse
from .utils.protobuf import read_message, write_message
import asyncio, aiozmq, zmq
import docker
import signal
import struct
from namedlist import namedtuple, namedlist

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
    ('stdin_sock', None),
    ('stdout_sock', None),
    ('stderr_sock', None),
])

instance_registry = {
    'test': Instance(ip='127.0.0.1')
}
kernel_registry = dict()

def find_avail_instance():
    for instance in instance_registry:
        if instance.cur_kernels < instance.max_kernels:
            instance.cur_kernels += 1
            return instance
    return None

@asyncio.coroutine
def handle_api(reader, writer):
    while not reader.at_eof():
        input_msg = yield from read_message(InputMessage, reader)
        output_msg = OutputMessage()

        if input_msg.action == ActionType.PING:

            output_msg.reply     = ReplyType.PONG
            output_msg.kernel_id = 0
            output_msg.body      = ''

        elif input_msg.action == ActionType.CREATE:

            instance = find_avail_instance()
            if instance is None:
                raise RuntimeError('No instance is available to launch a new kernel.')
            cli = docker.Client(
                base_url='tcp://{0}:{1}'.format(instance.ip, instance.docker_port),
                timeout=5, version='auto'
            )
            # TODO: create the container image
            # TODO: change the command to "python3 -m sorna.kernel_agent"
            container = cli.create_container(image='lablup-python-kernel:latest',
                                             command='/usr/bin/python3')
            kernel = Kernel(instance=instance, container=container.id)
            kernel_key = '{0}:{1}'.format(instance.ip, kernel.container)
            kernel_registry[kernel_key] = kernel

            # TODO: run the container and set the port mappings

            # TODO: check heartbeats to see if the container is running correctly.
            while tries < 5:
                yield from asyncio.sleep(1)

            # TODO: restore the user module state?

            output_msg.reply     = ReplyType.KERNEL_INFO
            output_msg.kernel_id = kernel_key
            output_msg.body      = json.loads({ # TODO: implement
                'stdin_sock': '',
                'stdout_sock': '',
                'stderr_sock': '',
            })

        elif input_msg.action == ActionType.DESTROY:

            if input_msg.kernel_id in kernel_registry:
                kernel = kernel_registry[input_msg.kernel_id]
                kernel.instance.cur_kernels -= 1
                assert(kernel.instance.cur_kernels >= 0)

                # TODO: implement
            else:
                raise RuntimeError('No such kernel.')
        
        yield from write_message(output_msg, writer)

def handle_exit():
    loop.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    start_coro = asyncio.start_server(handle_api, '0.0.0.0', 5001, loop=loop)
    server = loop.run_until_complete(start_coro)
    print('Started serving...')
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_exit)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
    print('Exit.')
