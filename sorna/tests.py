#! /usr/bin/env python3

import unittest
import subprocess, os, signal
import socket, time
import asyncio, zmq, asyncio_redis as aioredis
from .instance import *
from .driver import create_driver
from .proto import Namespace, encode, decode
from .proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes


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
        @asyncio.coroutine
        def create_conn_pool_coro():
            return (yield from asyncio.wait_for(aioredis.Pool.create(host=redis_host,
                                                                     port=redis_port,
                                                                     poolsize=1,
                                                                     loop=self.loop,
                                                                     auto_reconnect=False),
                                                timeout=5.0, loop=self.loop))
        self.pool = self.loop.run_until_complete(create_conn_pool_coro())
        self.pool_for_registry = self.loop.run_until_complete(create_conn_pool_coro())
        self.driver = create_driver('local')
        self.registry = InstanceRegistry(self.pool_for_registry,
                                         self.driver,
                                         kernel_timeout=1.0,
                                         loop=self.loop)
        @asyncio.coroutine
        def init_registry_coro():
            yield from self.registry.init()
        self.loop.run_until_complete(init_registry_coro())

    def tearDown(self):
        @asyncio.coroutine
        def terminate_registry_coro():
            yield from self.registry.terminate()
            #yield from self.pool_for_registry.flushdb()
        self.loop.run_until_complete(terminate_registry_coro())
        self.pool_for_registry.close()
        self.pool.close()
        # Progress the event loop so that the pending coroutines have chances to finish.
        # Otherwise, you will see a lot of ResourceWarnings about unclosed sockets.
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def test_init(self):
        @asyncio.coroutine
        def go():
            cursor = yield from self.pool.sscan('instance_registries')
            return (yield from cursor.fetchall())
        stored_ids = self.loop.run_until_complete(go())
        self.assertIn(self.registry._id, stored_ids)

    def test_add_instance(self):
        @asyncio.coroutine
        def go():
            return (yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test'))
        instance = self.loop.run_until_complete(go())
        self.assertIsNotNone(instance)
        self.assertEqual(instance.tag, 'test')

    def test_add_instance_and_working(self):
        # TODO: integration test: try to access the instance using the driver!
        pass

    def test_get_instance(self):
        @asyncio.coroutine
        def go():
            with self.assertRaises(InstanceNotFoundError):
                yield from self.registry.get_instance('non-existent-instance-id')
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')
            instance2 = yield from self.registry.get_instance(instance.id)
            self.assertEqual(instance, instance2)
        instance = self.loop.run_until_complete(go())

    def test_get_instance_diver_patterns(self):
        @asyncio.coroutine
        def go():
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')
            # Only the instance ID
            instance2 = yield from self.registry.get_instance(instance.id)
            self.assertEqual(instance, instance2)
            # The instance ID prefixed by the registry ID
            instance2 = yield from self.registry.get_instance(
                self.registry._mangle_inst_prefix(instance.id))
            self.assertEqual(instance, instance2)
            # The instance ID prefixed by the registry ID and suffixed by a sub-key name
            instance2 = yield from self.registry.get_instance(
                self.registry._mangle_inst_prefix(instance.id) + '.meta')
            self.assertEqual(instance, instance2)
        instance = self.loop.run_until_complete(go())

    def test_reattach_registry(self):
        @asyncio.coroutine
        def go():
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')
            new_registry = InstanceRegistry(self.pool_for_registry, self.driver, self.registry._id)
            instance2 = yield from new_registry.get_instance(instance.id)
            self.assertEqual(instance, instance2)
        self.loop.run_until_complete(go())

    def test_remove_instance(self):
        @asyncio.coroutine
        def go():
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')
            yield from self.registry.remove_instance(instance.id)
            with self.assertRaises(InstanceNotFoundError):
                yield from self.registry.get_instance(instance.id)
        self.loop.run_until_complete(go())

    def test_remove_instance_with_running_kernels(self):
        # Running kernels must be destroyed along with the instance.
        pass

    def test_create_kernel(self):
        @asyncio.coroutine
        def go():
            # A single front-end server creates a kernel when there is no instances.
            with self.assertRaises(InstanceNotAvailableError):
                yield from self.registry.create_kernel()

            # An instance has a capacity of a single kernel, and try to create kernel twice.
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')
            _, kernel = yield from self.registry.create_kernel()
            with self.assertRaises(InstanceNotAvailableError):
                yield from self.registry.create_kernel()
            yield from self.registry.destroy_kernel(kernel)
            yield from self.registry.remove_instance(instance)

            # An instance has a capacity of two kernels, and try to fill it.
            instance = yield from self.registry.add_instance(spec=None, max_kernels=2, tag='test')
            _, kernel1 = yield from self.registry.create_kernel()
            _, kernel2 = yield from self.registry.create_kernel()
            doppelganger1 = yield from self.registry.get_kernel(kernel1.id)
            doppelganger2 = yield from self.registry.get_kernel(kernel2.id)
            self.assertEqual(kernel1.id, doppelganger1.id)
            self.assertEqual(kernel2.id, doppelganger2.id)
            yield from self.registry.destroy_kernel(kernel1)
            yield from self.registry.destroy_kernel(kernel2)
            yield from self.registry.remove_instance(instance)
        self.loop.run_until_complete(go())

    def test_create_kernel_race_condition(self):
        # Two front-end servers create kernels in an interleaved manner.
        pass

    def test_destroy_kernel(self):
        pass

    def test_destroy_kernel_race_condition(self):
        pass

    def test_clean_up_kernel(self):
        @asyncio.coroutine
        def go():
            instance = yield from self.registry.add_instance(spec=None, max_kernels=1, tag='test')

            # Test cleaning immediately (timeout = 0)
            parent_instance, kernel = yield from self.registry.create_kernel()
            self.assertEqual(parent_instance.id, instance.id)
            kernel2 = yield from self.registry.get_kernel(kernel.id)
            self.assertEqual(kernel2, kernel)
            yield from self.registry.clean_old_kernels(timeout=0)
            with self.assertRaises(KernelNotFoundError):
                yield from self.registry.get_kernel(kernel.id)

            # Test cleaning on timeout
            parent_instance, kernel = yield from self.registry.create_kernel()
            self.assertEqual(parent_instance.id, instance.id)
            kernel2 = yield from self.registry.get_kernel(kernel.id)
            self.assertEqual(kernel2, kernel)
            yield from self.registry.clean_old_kernels(timeout=1.0) # fallback to set last_used
            yield from asyncio.sleep(1.2)
            yield from self.registry.clean_old_kernels(timeout=1.0)
            with self.assertRaises(KernelNotFoundError):
                yield from self.registry.get_kernel(kernel.id)
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
            cmd = ['python3', '-m', 'sorna.manager', '--kernel-driver', self.kernel_driver]
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
        request = Namespace()
        request.action = ManagerRequestTypes.PING
        request.body = 'test'
        self.socket.send(encode(request))

        response_data = self.socket.recv()
        response = decode(response_data)

        self.assertEqual(response.reply, ManagerResponseTypes.PONG)
        self.assertEqual(request.body, response.body)

    def test_create_and_destroy_kernel(self):
        # Create the kernel.
        request = Namespace()
        request.action = ManagerRequestTypes.CREATE
        request.user_id = 'test'
        request.body = {
            'spec': 'python34',
        }
        self.socket.send(encode(request))

        response_data = self.socket.recv()
        response = decode(response_data)

        self.assertEqual(response.reply, ManagerResponseTypes.SUCCESS)

        # Destroy the kernel.
        request.action = ManagerRequestTypes.DESTROY
        request.kernel_id = response.kernel_id
        self.socket.send(encode(request))

        # Receive response
        response_data = self.socket.recv()
        response = decode(response_data)

        # Assert the response is SUCCESS
        self.assertEqual(response.reply, ManagerResponseTypes.SUCCESS)
        #print(response)
