#! /usr/bin/env python3

import unittest
import subprocess, os, signal
import socket, time
import asyncio, zmq, aioredis
from sorna import defs, utils
from sorna.proto import Message, odict
from sorna.proto.msgtypes import ManagerRequestTypes, SornaResponseTypes
from .registry import *


class SornaInstanceRegistryTest(unittest.TestCase):
    '''
    Test the functionality of :class:`InstanceRegistry <sorna.instance.InstanceRegistry>`.
    This test suite requires a temporary Redis server and a set of docker link
    environment variables to it.
    '''

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        redis_host = os.environ.get('REDIS_PORT_6379_TCP_ADDR', '127.0.0.1')
        redis_port = int(os.environ.get('REDIS_PORT_6379_TCP_PORT', '6379'))
        async def create_redis_pool(dbid):
            return await asyncio.wait_for(aioredis.create_pool((redis_host, redis_port),
                                                               encoding='utf8',
                                                               db=dbid,
                                                               loop=self.loop),
                                          timeout=1.0, loop=self.loop)
        self.redis_kern = self.loop.run_until_complete(create_redis_pool(defs.SORNA_KERNEL_DB))
        self.redis_inst = self.loop.run_until_complete(create_redis_pool(defs.SORNA_INSTANCE_DB))
        self.redis_sess = self.loop.run_until_complete(create_redis_pool(defs.SORNA_SESSION_DB))
        my_ip = self.loop.run_until_complete(utils.get_instance_ip())
        manager_addr = 'tcp://{0}:{1}'.format(my_ip, 5001)
        self.registry = InstanceRegistry((redis_host, redis_port),
                                         manager_addr=manager_addr,
                                         loop=self.loop)
        async def init_registry():
            await self.registry.init()
        self.loop.run_until_complete(init_registry())

    def tearDown(self):
        async def clean_up():
            await self.registry.terminate()
            await self.redis_kern.clear()
            await self.redis_inst.clear()
            await self.redis_sess.clear()
        self.loop.run_until_complete(clean_up())
        # Progress the event loop so that the pending coroutines have chances to finish.
        # Otherwise, you will see a lot of ResourceWarnings about unclosed sockets.
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def test_create_kernel(self):
        async def go():
            # An instance has a capacity of a single kernel, and try to create kernel twice.
            instance, kernel = await self.registry.create_kernel(lang='python34')
            #print(instance, kernel)
            await self.registry.destroy_kernel(kernel)

        self.loop.run_until_complete(go())


class SornaManagerLocalIntegrationTest(unittest.TestCase):
    '''
    Test the manager using the full API interaction steps, including zmq-based communication.
    This test requires a temporary Redis server like :class:`SornaInstanceRegistryTest`.
    '''

    def setUp(self):
        self.kernel_ip = '127.0.0.1'
        self.kernel_driver = 'local'
        self.manager_port = 5001
        self.manager_addr = 'tcp://{0}:{1}'.format(self.kernel_ip, self.manager_port)

        # Establish a manager server in a separate process
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port_error = s.connect_ex((self.kernel_ip, self.manager_port))
        s.close()
        if self.port_error != 0:  # When the port is available
            cmd = ['python3', '-m', 'sorna.manager',
                   '--kernel-driver', self.kernel_driver,
                   '--max-kernels', '1']
            self.server = subprocess.Popen(cmd, start_new_session=True,
                                           stdout=subprocess.DEVNULL,
                                           stderr=subprocess.DEVNULL)

        # Connect to the manager server
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.manager_addr)

    def tearDown(self):
        if self.port_error != 0:  # Kill test server
            sid = os.getsid(self.server.pid)
            os.killpg(sid, signal.SIGTERM)
            exitcode = self.server.wait()
            #print('Manager process exited with code {0}'.format(exitcode))

    def test_ping(self):
        request = Message(
            ('action', ManagerRequestTypes.PING),
            ('body', 'test'),
        )
        self.socket.send(request.encode())

        self.assertNotEqual(self.socket.poll(1000), 0)
        response_data = self.socket.recv()
        response = Message.decode(response_data)

        self.assertEqual(response['reply'], SornaResponseTypes.PONG)
        self.assertEqual(request['body'], response['body'])

    def test_create_and_destroy_kernel(self):
        # Create the kernel.
        request = Message(
            ('action', ManagerRequestTypes.CREATE),
            ('user_id', 'test'),
        )
        self.socket.send(request.encode())

        self.assertNotEqual(self.socket.poll(1000), 0)
        response_data = self.socket.recv()
        response = Message.decode(response_data)

        self.assertEqual(response['reply'], SornaResponseTypes.SUCCESS)

        # Destroy the kernel.
        request = Message(
            ('action', ManagerRequestTypes.DESTROY),
            ('kernel_id', response['kernel_id']),
        )
        self.socket.send(request.encode())

        # Receive response
        self.assertNotEqual(self.socket.poll(1000), 0)
        response_data = self.socket.recv()
        response = Message.decode(response_data)

        # Assert the response is SUCCESS
        self.assertEqual(response['reply'], SornaResponseTypes.SUCCESS)
        #print(response)
