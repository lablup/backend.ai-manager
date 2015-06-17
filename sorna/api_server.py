#! /usr/bin/env python3

'''
The Sorna API Server

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

from sorna.proto.api_pb2 import InputMessage, OutputMessage, ActionType, ReplyType
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
        input_len     = yield from reader.readexactly(2)
        input_msg_len = struct.unpack('>H', input_len)
        input_data    = yield from reader.readexactly(input_msg_len)
        input_msg     = InputMessage.ParseFromString(input_data)
        output_msg = OutputMessage()

        if input_msg.action == ActionType.PING:

            output_msg.reply     = ReplyType.PONG
            output_msg.kernel_id = 0
            output_msg.length    = 0

        elif input_msg.action == ActionType.CREATE:

            instance = find_avail_instance()
            if instance is None:
                raise RuntimeError('No instance is available to launch a new kernel.')
            cli = docker.Client(
                base_url='tcp://{0}:{1}'.format(instance.ip, instance.docker_port),
                timeout=5, version='auto'
            )
            container = cli.create_container(image='lablup-python-kernel:latest',
                                             command='/usr/bin/python3')
            kernel = Kernel(instance=instance, container=container.id)
            kernel_key = '{0}:{1}'.format(instance.ip, kernel.container)
            kernel_registry[kernel_key] = kernel

            # TODO: restore the user module state?
            # TODO: check if the container is running correctly.

            output_msg.reply     = ReplyType.KERNEL_INFO
            output_msg.kernel_id = kernel_key
            output_msg.content   = json.loads({ # TODO: implement
                'stdin_sock': '',
                'stdout_sock': '',
                'stderr_sock': '',
            })
            output_msg.length = len(output_msg.content)

        elif input_msg.action == ActionType.DESTROY:

            if input_msg.kernel_id in kernel_registry:
                kernel = kernel_registry[input_msg.kernel_id]
                kernel.instance.cur_kernels -= 1
                assert(kernel.instance.cur_kernels >= 0)

                # TODO: implement
            else:
                raise RuntimeError('No such kernel.')

        output = output_msg.SerializeToString()
        writer.write(struct.pack('>H', len(output)))
        writer.write(output)
        yield from writer.drain()

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
