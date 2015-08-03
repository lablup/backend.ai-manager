#! /usr/bin/env python3

'''
The Sorna Drivers

A driver defines a set of methods to deploy and destroy computing resources,
such as Docker containers on top of AWS or the local machine.
'''

from abc import ABCMeta, abstractmethod
import asyncio, aiozmq
from enum import Enum
from datetime import datetime
import uuid
import socket
import subprocess
from .structs import Kernel

__all__ = ['DriverTypes', 'BaseDriver', 'AWSDockerDriver', 'LocalDriver', 'create_driver']

DriverTypes = Enum('DriverTypes', 'local aws_docker')
AgentPortRange = tuple(range(5002, 5010))


class BaseDriver(metaclass=ABCMeta):
    '''
    The driver is a low-level interface to control computing resource infrastucture,
    such as Amazon AWS and local development environment.

    Drivers are agnostic to resource management polices; they just do what the
    registry and other components request.
    '''

    def __init__(self, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()

    @asyncio.coroutine
    @abstractmethod
    def launch_instance(self, spec=None):
        '''
        Prepare a VM instance and runs the sorna agent on it.

        :param spec: An object describing the type of instance and extra options sepcific to each driver. If None, the driver uses its default setting.

        :returns: A tuple of the instance ID and its IP address.
        '''
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def destroy_instance(self, inst_id):
        '''
        Destroy the launched instance.
        '''
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def create_kernel(self, instance, agent_port, manager_addr):
        '''
        Launch the kernel and return its ID.

        :param instance: An object containing information of the target instance.
        :param manager_addr: The address of the manager.
        :type instance: :class:`Instance <sorna.structs.Instance>`
        :type manager_Addr: :class:`str`

        :returns: A :class:`Kernel <sorna.structs.Kernel>` object with kernel details.
        '''
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def destroy_kernel(self, kernel):
        '''
        Destroy the kernel.

        :param kernel: An object containing information of the target kernel.
        :type kernel: :class:`Kernel <sorna.structs.Kernel>`
        '''
        raise NotImplementedError()

    @asyncio.coroutine
    @abstractmethod
    def get_internal_ip(self):
        '''
        Get the IP address used internally.
        This address is used to allow kernel agents to connect back to the manager.
        '''
        raise NotImplementedError()


class AWSDockerDriver(BaseDriver):
    '''
    This driver uses Amazon EC2 for instances and docker as the kernel container.
    '''

    @asyncio.coroutine
    def launch_instance(self, spec=None):
        if sepc is None:
            spec = 't2.micro'
        # TODO: use boto to launch EC2 instance
        raise NotImplementedError()

    @asyncio.coroutine
    def destroy_instance(self, inst_id):
        # TODO: use boto to destroy EC2 instance
        raise NotImplementedError()

    @asyncio.coroutine
    def create_kernel(self, instance, agent_port, manager_addr):
        cli = docker.Client(
            base_url='tcp://{0}:{1}'.format(instance.ip, instance.docker_port),
            timeout=5, version='auto'
        )
        # TODO: create the container image
        # TODO: change the command to "python3 -m sorna.kernel_agent"
        # TODO: pass agent_port
        container = cli.create_container(image='lablup-python-kernel:latest',
                                         command='/usr/bin/python3')
        kernel = Kernel(instance=instance, id=container.id)
        kernel.priv = container.id
        kernel.id = 'docker:{0}'.format(kernel.priv)
        kernel.agent_sock = 'tcp://{0}:{1}'.format(instance.ip, agent_port)
        # TODO: run the container and set the port mappings
        return kernel

    @asyncio.coroutine
    def destroy_kernel(self, kernel):
        # TODO: destroy the container
        raise NotImplementedError()

    @asyncio.coroutine
    def get_internal_ip(self):
        # TODO: read http://instance-data/latest/meta-data/local-ipv4
        raise NotImplementedError()


class LocalDriver(BaseDriver):
    '''
    This driver does not use remote hosts at all.
    The kernel container is simply a local process.
    '''

    def __init__(self, loop=None):
        super().__init__(loop=loop)
        self.agents = dict()

    @asyncio.coroutine
    def launch_instance(self, spec=None):
        # As this is the local machine, we do nothing!
        inst_id = uuid.uuid4().hex
        return inst_id, '127.0.0.1'

    @asyncio.coroutine
    def destroy_instance(self, inst_id):
        pass

    @asyncio.coroutine
    def create_kernel(self, instance, agent_port, manager_addr):
        kernel_id = 'local:{0}'.format(uuid.uuid4().hex)
        kernel = Kernel(id=kernel_id, instance=instance.id)
        # TODO: Pool the local port numbers in a more general way!
        #       The validity of this "temp socket" method depends on how the OS
        #       assigns the port numbers.
        #       (In particular, the OS must use different port number when
        #       creating the next socket.)
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_sock.bind(('', 0))
        new_port = temp_sock.getsockname()[1]
        temp_sock.close()
        cmdargs = ('/usr/bin/env', 'python3', '-m', 'sorna.agent',
                   '--kernel-id', kernel_id, '--agent-port', str(new_port),
                   '--manager-addr', manager_addr)
        proc = yield from asyncio.create_subprocess_exec(*cmdargs, loop=self.loop,
                                                         stdout=subprocess.DEVNULL,
                                                         stderr=subprocess.DEVNULL)
        kernel.agent_sock = 'tcp://{0}:{1}'.format(instance.ip, new_port)
        kernel.stdin_sock = ''
        kernel.stdout_sock = ''
        kernel.stderr_sock = ''
        kernel.created_at = datetime.now().isoformat()
        self.agents[kernel_id] = proc
        return kernel

    @asyncio.coroutine
    def destroy_kernel(self, kernel):
        proc = self.agents[kernel.id]
        proc.terminate()
        yield from proc.wait()
        del self.agents[kernel.id]

    @asyncio.coroutine
    def get_internal_ip(self):
        return '127.0.0.1'


_driver_type_to_class = {
    'local': LocalDriver,
    'aws_docker': AWSDockerDriver,
}

def create_driver(name):
    '''
    Create an instance of driver with the given driver name.
    '''
    cls = _driver_type_to_class[name]
    return cls()
