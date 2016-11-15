#! /usr/bin/env python3

import asyncio
import subprocess
import os, signal
import time
import unittest

import aioredis
import zmq

from sorna.proto import Message
from sorna.proto.msgtypes import ManagerRequestTypes, SornaResponseTypes
from sorna.utils import get_instance_ip
from sorna.exceptions import InstanceNotAvailableError
from .registry import InstanceRegistry


class SornaProcessManagerMixin:

    @staticmethod
    def run_agent(max_kernels=1):
        # Run a local agent
        cmd = ['python3', '-m', 'sorna.agent.server',
               '--manager-addr', 'tcp://127.0.0.1:5001',
               '--max-kernels', str(max_kernels)]
        agent_proc = subprocess.Popen(cmd, start_new_session=True,
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
        return agent_proc

    @staticmethod
    def kill_proc(proc):
        sid = os.getsid(proc.pid)
        os.killpg(sid, signal.SIGTERM)
        proc.wait()

    @staticmethod
    def run_manager():
        # Run a local server
        cmd = ['python3', '-m', 'sorna.manager.server']
        manager_proc = subprocess.Popen(cmd, start_new_session=True,
                                        stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL)
        return manager_proc

    def zmq_connect(self, addr):
        if not hasattr(self, 'zmq_ctx'):
            self.zmq_ctx = zmq.Context()
        sock = self.zmq_ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.SNDHWM, 50)
        sock.setsockopt(zmq.RCVHWM, 50)
        sock.connect(addr)
        return sock


class SornaRegistryTest(unittest.TestCase, SornaProcessManagerMixin):
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

        async def create_redis():
            return await asyncio.wait_for(
                aioredis.create_redis((redis_host, redis_port),
                                      encoding='utf8',
                                      loop=self.loop),
                timeout=1.0, loop=self.loop)

        self.redis = self.loop.run_until_complete(create_redis())
        my_ip = self.loop.run_until_complete(get_instance_ip())
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
            await self.redis.flushall()
            await self.redis.quit()
        self.loop.run_until_complete(clean_up())
        # Progress the event loop so that the pending coroutines have chances to finish.
        # Otherwise, you will see a lot of ResourceWarnings about unclosed sockets.
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def test_01_create_kernel_with_no_instance(self):
        async def go():
            with self.assertRaises(InstanceNotAvailableError):
                instance, kernel = await self.registry.create_kernel(lang='python34')
        self.loop.run_until_complete(go())

    def test_02_create_kernel(self):
        async def go():
            instance, kernel = await self.registry.create_kernel(lang='python34')
            assert kernel.instance == instance.id
            assert kernel.addr == instance.addr
            await self.registry.destroy_kernel(kernel)
        p = self.run_agent()
        time.sleep(1)
        self.loop.run_until_complete(go())
        time.sleep(1)
        self.kill_proc(p)
        time.sleep(1)


class SornaIntegrationTest(unittest.TestCase, SornaProcessManagerMixin):
    '''
    Test the manager using the full API interaction steps, including zmq-based communication.
    This test requires a temporary Redis server like :class:`SornaInstanceRegistryTest`.
    '''

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        redis_host = os.environ.get('REDIS_PORT_6379_TCP_ADDR', '127.0.0.1')
        redis_port = int(os.environ.get('REDIS_PORT_6379_TCP_PORT', '6379'))

        async def create_redis():
            return await asyncio.wait_for(
                aioredis.create_redis((redis_host, redis_port),
                                      encoding='utf8',
                                      loop=self.loop),
                timeout=1.0, loop=self.loop)

        self.redis = self.loop.run_until_complete(create_redis())

    def tearDown(self):
        async def clean_up():
            await self.redis.flushall()
            await self.redis.quit()

        self.loop.run_until_complete(clean_up())
        # Progress the event loop so that the pending coroutines have chances to finish.
        # Otherwise, you will see a lot of ResourceWarnings about unclosed sockets.
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def test_session_based_kernel_mgmt(self):
        mgr = self.run_manager()
        time.sleep(1)
        agent = self.run_agent(max_kernels=2)
        time.sleep(1)
        # Connect to the manager
        sock = self.zmq_connect('tcp://127.0.0.1:5001')

        # Create the kernel.
        request = Message(
            ('action', ManagerRequestTypes.GET_OR_CREATE),
            ('user_id', 'test'),
            ('entry_id', 'abcdef'),
            ('lang', 'python34'),
        )
        sock.send(request.encode())
        self.assertNotEqual(sock.poll(1000), 0)
        response_data = sock.recv()
        response = Message.decode(response_data)
        self.assertEqual(response['reply'], SornaResponseTypes.SUCCESS)
        kernel_id = response['kernel_id']

        # Get the kernel (the kernel should be same!)
        request = Message(
            ('action', ManagerRequestTypes.GET_OR_CREATE),
            ('user_id', 'test'),
            ('entry_id', 'abcdef'),
            ('lang', 'python34'),
        )
        sock.send(request.encode())
        self.assertNotEqual(sock.poll(1000), 0)
        response_data = sock.recv()
        response = Message.decode(response_data)
        self.assertEqual(kernel_id, response['kernel_id'])

        # Create another kernel
        request = Message(
            ('action', ManagerRequestTypes.GET_OR_CREATE),
            ('user_id', 'test'),
            ('entry_id', 'abcdeg'),
            ('lang', 'python34'),
        )
        sock.send(request.encode())
        self.assertNotEqual(sock.poll(1000), 0)
        response_data = sock.recv()
        response = Message.decode(response_data)
        self.assertNotEqual(kernel_id, response['kernel_id'])
        kernel2_id = response['kernel_id']

        # Destroy all kernels.
        request = Message(
            ('action', ManagerRequestTypes.DESTROY),
            ('kernel_id', kernel_id),
        )
        sock.send(request.encode())
        self.assertNotEqual(sock.poll(1000), 0)
        response_data = sock.recv()
        response = Message.decode(response_data)
        self.assertEqual(response['reply'], SornaResponseTypes.SUCCESS)

        request = Message(
            ('action', ManagerRequestTypes.DESTROY),
            ('kernel_id', kernel2_id),
        )
        sock.send(request.encode())
        self.assertNotEqual(sock.poll(1000), 0)
        response_data = sock.recv()
        response = Message.decode(response_data)
        self.assertEqual(response['reply'], SornaResponseTypes.SUCCESS)

        sock.close()
        self.kill_proc(mgr)
        time.sleep(1)
        self.kill_proc(agent)
        time.sleep(1)
