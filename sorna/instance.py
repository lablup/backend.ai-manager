#! /usr/bin/env python3

import asyncio, asyncio_redis as aioredis
import zmq, aiozmq
from datetime import datetime
import re
from urllib.parse import urlparse
import uuid
from .proto import Namespace, encode, decode
from .proto.msgtypes import AgentRequestTypes
from .driver import BaseDriver
from .structs import Instance, Kernel

class InstanceNotAvailableError(RuntimeError):
    pass

class KernelNotFoundError(RuntimeError):
    pass

class InstanceRegistry:
    '''
    Provide a high-level API to create, destroy, and query the computation
    kernels.  It uses :class:`BaseDriver <sorna.driver.BaseDriver>` subclasses to control low-level
    details of resource management and sorna agents.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    '''

    # To atomically check and get an available instance,
    # we wrap that operation as a Redis script.
    fetch_avail_inst_luafunc = '''
    -- KEYS[1]: the key name of available instance set
    -- ARGV[1]: registry id
    -- ARGV[2]: instance id
    local inst_key = ARGV[1] .. ".inst." .. ARGV[2]
    local inst_num = redis.call("HGET", inst_key, "num_kernels")
    local inst_max = redis.call("HGET", inst_key, "max_kernels")
    if inst_num == inst_max - 1 then
      redis.call("SREM", KEYS[1], ARGV[2])
    else
      redis.call("SADD", KEYS[1], ARGV[2])
    end
    redis.call("HINCRBY", inst_key, "num_kernels", 1)
    '''

    def __init__(self, redis_conn, kernel_driver, loop=None):
        assert isinstance(redis_conn, aioredis.Pool)
        assert isinstance(kernel_driver, BaseDriver)
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._conn = redis_conn
        self._driver = kernel_driver
        self._id = str(uuid.uuid4())

    @asyncio.coroutine
    def init(self):
        yield from self._conn.sadd('instance_registries', [self._id])
        yield from self._conn.set('{0}.meta.created'.format(self._id), datetime.now().isoformat())
        self.fetch_avail_inst_script = yield from self._conn.register_script(self.fetch_avail_inst_luafunc)

    @asyncio.coroutine
    def terminate(self):
        # TODO: destroy all instances
        # TODO: destroy all tracking kernels
        yield from self._conn.srem('instance_registries', [self._id])
        yield from self._conn.delete(['{0}.meta.created'.format(self._id)])

    def _mangle_inst_prefix(self, inst_id):
        return '{0}.inst.{1}'.format(self._id, inst_id)

    def _mangle_kernel_prefix(self, kern_id):
        return '{0}.kern.{1}'.format(self._id, kern_id)

    @staticmethod
    def fut2ret(f):
        return f.result() if isinstance(f, asyncio.Future) else f

    @asyncio.coroutine
    def get_instance(self, inst_id):
        assert isinstance(inst_id, str)
        key = self._mangle_inst_prefix(inst_id)
        fields = yield from self._conn.hmget(key, [
            'ip',
            'spec',
            'docker_port',
            'max_kernels',
            'num_kernels',
            'tag',
        ])
        fields = map(self.fut2ret, fields)
        return Instance(inst_id, *fields)

    @asyncio.coroutine
    def get_kernel(self, kern_id):
        assert isinstance(kern_id, str)
        key = self._mangle_kernel_prefix(kern_id)
        fields = yield from self._conn.hmget(key, [
            'instance',
            'spec',
            'agent_sock',
            'stdin_sock',
            'stdout_sock',
            'stderr_sock',
            'created_at',
            'tag',
        ])
        fields = map(self.fut2ret, fields)
        return Kernel(kern_id, *fields)

    @asyncio.coroutine
    def add_instance(self, spec=None, max_kernels=1, tag=None):
        assert max_kernels < 100 and max_kernels > 0
        inst_id, inst_ip = yield from self._driver.launch_instance(spec)
        key = self._mangle_inst_prefix(inst_id)
        ret = False
        while not ret:
            tnx = yield from self._conn.multi(watch=[key])
            fut = yield from tnx.hmset(key, {
                'ip': inst_ip,
                'docker_port': '2375',
                'max_kernels': str(max_kernels),
                'num_kernels': '0',
                'tag': tag if tag else '',
            })
            fut2 = yield from tnx.lpush(key + '.agent_ports', list(map(str, range(6000, 6100))))
            yield from tnx.exec()
            ret = all((yield from asyncio.gather(fut, fut2)))
        avail_key = '{0}.avail_instances'.format(self._id)
        yield from self._conn.sadd(avail_key, [inst_id])
        print('add_instance / avail_key = {}'.format(avail_key))
        return (yield from self.get_instance(inst_id))

    @asyncio.coroutine
    def remove_instance(self, instance_id, destroy=True):
        # TODO: kill all kernels running on it
        # TODO: remove from avail_instances
        # TODO: destory the instance using driver
        raise NotImplementedError()

    @asyncio.coroutine
    def create_kernel(self, spec='python34'):
        found = False
        avail_key = '{0}.avail_instances'.format(self._id)
        inst_id = yield from self._conn.spop(avail_key)
        if inst_id is None:
            # TODO: automatically add instances to some extent
            raise InstanceNotAvailableError()
        else:
            yield from self.fetch_avail_inst_script.run(keys=[avail_key], args=[self._id, inst_id])

            instance = yield from self.get_instance(inst_id)
            inst_key = self._mangle_inst_prefix(inst_id)
            assigned_agent_port = yield from self._conn.lpop(inst_key + '.agent_ports')

            kernel = yield from self._driver.create_kernel(instance, assigned_agent_port)
            kern_key = self._mangle_kernel_prefix(kernel.id)
            yield from self.get_socket_info(kernel)
            yield from self._conn.hmset(kern_key, {
                'id': kernel.id,
                'instance': instance.id,
                'spec': kernel.spec,
                'agent_sock': kernel.agent_sock,
                'stdin_sock': kernel.stdin_sock,
                'stdout_sock': kernel.stdout_sock,
                'stderr_sock': kernel.stderr_sock,
                'created_at': datetime.now().isoformat(),
            })
            return instance, kernel

    @asyncio.coroutine
    def destroy_kernel(self, kernel):
        if not isinstance(kernel, Kernel): kernel = yield from self.get_kernel(kernel)
        yield from self._driver.destroy_kernel(kernel)
        kern_key = self._mangle_kernel_prefix(kernel.id)
        yield from self._conn.delete([kern_key])
        inst_key = self._mangle_inst_prefix(kernel.instance)
        yield from self._conn.hincrby(inst_key, 'num_kernels', -1)
        p = urlparse(kernel.agent_sock)
        yield from self._conn.lpush(inst_key + '.agent_ports', list(map(str, [p.port])))
        avail_key = '{0}.avail_instances'.format(self._id)
        yield from self._conn.sadd(avail_key, [kernel.instance])

    @asyncio.coroutine
    def ping_kernel(self, kernel):
        if not isinstance(kernel, Kernel): kernel = yield from self.get_kernel(kernel)
        sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.agent_sock,
                                                   loop=self._loop)
        req_id = str(uuid.uuid4())
        req = Namespace()
        req.req_type = AgentRequestTypes.HEARTBEAT
        req.body = req_id
        sock.write([encode(req)])
        try:
            resp_data = yield from asyncio.wait_for(sock.read(), timeout=2.0,
                                                    loop=self._loop)
            resp = decode(resp_data[0])
            return (resp.body == req_id)
        except asyncio.TimeoutError:
            return False
        finally:
            sock.close()

    @asyncio.coroutine
    def get_socket_info(self, kernel):
        if not isinstance(kernel, Kernel): kernel = yield from self.get_kernel(kernel)
        sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.agent_sock,
                                                   loop=self._loop)
        req = Namespace()
        req.req_type = AgentRequestTypes.SOCKET_INFO
        req.body = ''
        sock.write([encode(req)])
        resp_data = yield from sock.read()
        resp = decode(resp_data[0])
        kernel.stdin_sock = resp.body.stdin
        kernel.stdout_sock = resp.body.stdout
        kernel.stderr_sock = resp.body.stderr
        sock.close()
