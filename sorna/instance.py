#! /usr/bin/env python3

import asyncio, asyncio_redis as aioredis
import zmq, aiozmq
from datetime import datetime, timedelta
import dateutil.parser
import functools
import re
from urllib.parse import urlparse
import uuid
import time
from .proto import Namespace, encode, decode
from .proto.msgtypes import AgentRequestTypes
from .driver import BaseDriver
from .structs import Instance, Kernel

__all__ = ['InstanceRegistry', 'InstanceNotFoundError', 'KernelNotFoundError',
           'InstanceNotAvailableError']


def _auto_get_kernel(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not isinstance(args[0], Kernel):
            arg0 = yield from self.get_kernel(args[0])
            yield from func(self, arg0, *args[1:], **kwargs)
        return func(self, *args, **kwargs)
    return wrapper

def _auto_get_instance(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not isinstance(args[0], Instance):
            arg0 = yield from self.get_instance(args[0])
            yield from func(self, arg0, *args[1:], **kwargs)
        return func(self, *args, **kwargs)
    return wrapper

def _s(obj):
    if obj is None: return ''
    return str(obj)



class InstanceNotAvailableError(RuntimeError):
    pass


class InstanceNotFoundError(RuntimeError):
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
    local inst_key = ARGV[1] .. ".inst." .. ARGV[2] .. ".meta"
    local inst_num = tonumber(redis.call("HGET", inst_key, "num_kernels"))
    local inst_max = tonumber(redis.call("HGET", inst_key, "max_kernels"))
    if inst_num < inst_max - 1 then
      redis.call("SADD", KEYS[1], ARGV[2])
    end
    redis.call("HINCRBY", inst_key, "num_kernels", 1)
    '''

    def __init__(self, redis_conn, kernel_driver, registry_id=None, kernel_timeout=600, manager_addr=None, loop=None):
        assert isinstance(redis_conn, aioredis.Pool)
        assert isinstance(kernel_driver, BaseDriver)
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._conn = redis_conn
        self._driver = kernel_driver
        self._id = uuid.uuid4().hex if registry_id is None else registry_id
        self._kernel_timeout = kernel_timeout
        self._manager_addr = manager_addr

    @asyncio.coroutine
    def init(self):
        yield from self._conn.sadd('instance_registries', [self._id])
        yield from self._conn.set('{0}.meta.created'.format(self._id), datetime.now().isoformat())
        self.fetch_avail_inst_script = yield from self._conn.register_script(self.fetch_avail_inst_luafunc)

    @asyncio.coroutine
    def terminate(self):
        # Remove all instances. (This will also terminate running kernels on them.)
        cursor = yield from self._conn.scan('{0}.inst.*.meta'.format(self._id))
        while True:
            cursor.count = 100
            inst_key = yield from cursor.fetchone()
            if inst_key is None: break
            yield from self.remove_instance(inst_key)

        # Clean up registry information.
        yield from self._conn.delete(['{0}.avail_instances'.format(self._id)])
        yield from self._conn.srem('instance_registries', [self._id])
        yield from self._conn.delete(['{0}.meta.created'.format(self._id)])

    def _mangle_inst_prefix(self, inst_id):
        return '{0}.inst.{1}'.format(self._id, inst_id)

    def _mangle_kernel_prefix(self, kern_id):
        return '{0}.kern.{1}'.format(self._id, kern_id)

    @staticmethod
    def fut2ret(f):
        return f.result() if isinstance(f, asyncio.Future) else f

    _re_inst_id = re.compile(r'^((?P<reg_id>[:\w]+)\.inst\.)?(?P<inst_id>[:\w]+)(\.[-\w]+)?$')
    _re_kern_id = re.compile(r'^((?P<reg_id>[:\w]+)\.kern\.)?(?P<kern_id>[:\w]+)(\.[-\w]+)?$')

    @asyncio.coroutine
    def get_instance(self, inst_id):
        assert isinstance(inst_id, str)
        m = self._re_inst_id.search(inst_id)
        if m is None:
            raise InstanceNotFoundError(inst_id)
        inst_key = self._mangle_inst_prefix(m.group('inst_id')) + '.meta'
        if not (yield from self._conn.exists(inst_key)):
            raise InstanceNotFoundError(inst_id)
        fields = yield from self._conn.hmget(inst_key, [
            'ip',
            'spec',
            'docker_port',
            'max_kernels',
            'num_kernels',
            'tag',
        ])
        fields = map(self.fut2ret, fields)
        return Instance(m.group('inst_id'), *fields)

    @asyncio.coroutine
    def get_kernel(self, kern_id):
        assert isinstance(kern_id, str)
        m = self._re_kern_id.search(kern_id)
        if m is None:
            raise KernelNotFoundError(kern_id)
        kern_key = self._mangle_kernel_prefix(m.group('kern_id')) + '.meta'
        if not (yield from self._conn.exists(kern_key)):
            raise KernelNotFoundError(kern_id)
        fields = yield from self._conn.hmget(kern_key, [
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
        return Kernel(m.group('kern_id'), *fields)

    @asyncio.coroutine
    def add_instance(self, spec=None, max_kernels=1, tag=None):
        assert max_kernels < 100 and max_kernels > 0
        inst_id, inst_ip = yield from self._driver.launch_instance(spec)
        inst_kp = self._mangle_inst_prefix(inst_id)
        ret = False
        while not ret:
            tnx = yield from self._conn.multi(watch=[inst_kp + '.meta'])
            fut = yield from tnx.hmset(inst_kp + '.meta', {
                'ip': inst_ip,
                'docker_port': '2375',
                'max_kernels': str(max_kernels),
                'num_kernels': '0',
                'tag': _s(tag),
            })
            fut2 = yield from tnx.lpush(inst_kp + '.agent_ports', list(map(str, range(6000, 6100))))
            yield from tnx.exec()
            ret = all((yield from asyncio.gather(fut, fut2)))
        avail_key = '{0}.avail_instances'.format(self._id)
        if max_kernels > 0:
            yield from self._conn.sadd(avail_key, [inst_id])
        return (yield from self.get_instance(inst_id))

    @asyncio.coroutine
    @_auto_get_instance
    def remove_instance(self, instance, destroy=True):
        inst_kp = self._mangle_inst_prefix(instance.id)
        yield from self.clean_old_kernels(inst_id=instance.id, timeout=0)

        # Remove from available instances and delete Redis tracking keys
        avail_key = '{0}.avail_instances'.format(self._id)
        yield from self._conn.srem(avail_key, [instance.id])
        yield from self._conn.delete([inst_kp + '.meta', inst_kp + '.agent_ports'])

        # Destroy the instance
        yield from self._driver.destroy_instance(instance.id)

    @asyncio.coroutine
    def create_kernel(self, spec='python34'):
        avail_key = '{0}.avail_instances'.format(self._id)
        avail_count = yield from self._conn.scard(avail_key)
        if avail_count == 0:
            raise InstanceNotAvailableError('Could not find available instance.')
        inst_id = yield from self._conn.spop(avail_key)
        if inst_id is None:
            # TODO: automatically add instances to some extent
            raise InstanceNotAvailableError('Could not find available instance.')
        else:
            try:
                yield from self.fetch_avail_inst_script.run(keys=[avail_key],
                                                            args=[self._id, inst_id])
            except aioredis.exceptions.ScriptKilledError as e:
                print(e.message)
                raise InstanceNotAvailableError('Failed to fetch available instance.') from e

            instance = yield from self.get_instance(inst_id)
            inst_kp = self._mangle_inst_prefix(inst_id)
            assigned_agent_port = yield from self._conn.lpop(inst_kp + '.agent_ports')

            kernel = yield from self._driver.create_kernel(instance,
                                                           assigned_agent_port,
                                                           self._manager_addr)
            kern_kp = self._mangle_kernel_prefix(kernel.id)
            yield from self.fill_socket_info(kernel)
            yield from self._conn.hmset(kern_kp + '.meta', {
                'id': kernel.id,
                'instance': instance.id,
                'spec': _s(kernel.spec),
                'agent_sock': _s(kernel.agent_sock),
                'stdin_sock': _s(kernel.stdin_sock),
                'stdout_sock': _s(kernel.stdout_sock),
                'stderr_sock': _s(kernel.stderr_sock),
                'created_at': kernel.created_at,
            })
            return instance, kernel

    @asyncio.coroutine
    @_auto_get_kernel
    def destroy_kernel(self, kernel):
        yield from self._driver.destroy_kernel(kernel)
        kern_kp = self._mangle_kernel_prefix(kernel.id)
        yield from self._conn.delete([kern_kp + '.meta'])
        inst_kp = self._mangle_inst_prefix(kernel.instance)
        yield from self._conn.hincrby(inst_kp + '.meta', 'num_kernels', -1)
        p = urlparse(kernel.agent_sock)
        yield from self._conn.lpush(inst_kp + '.agent_ports', list(map(str, [p.port])))
        avail_key = '{0}.avail_instances'.format(self._id)
        yield from self._conn.sadd(avail_key, [kernel.instance])

    @asyncio.coroutine
    @_auto_get_kernel
    def ping_kernel(self, kernel):
        sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.agent_sock,
                                                   loop=self._loop)
        req_id = uuid.uuid4().hex
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
    def fill_socket_info(self, kernel):
        sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.agent_sock,
                                                   loop=self._loop)
        req = Namespace()
        req.req_type = AgentRequestTypes.SOCKET_INFO
        req.kernel_id = kernel.id
        req.body = ''
        sock.write([encode(req)])
        resp_data = yield from sock.read()
        resp = decode(resp_data[0])
        kernel.stdin_sock = resp.body.stdin
        kernel.stdout_sock = resp.body.stdout
        kernel.stderr_sock = resp.body.stderr
        sock.close()

    @asyncio.coroutine
    @_auto_get_kernel
    def refresh_kernel(self, kernel):
        kern_kp = self._mangle_kernel_prefix(kernel.id)
        yield from self._conn.hset(kern_kp + '.meta', 'last_used', datetime.now().isoformat())

    @asyncio.coroutine
    def clean_old_kernels(self, inst_id=None, timeout=None):
        if timeout is None:
            timeout = self._kernel_timeout

        cursor = yield from self._conn.scan('{0}.kern.*.meta'.format(self._id))
        while True:
            cursor.count = 100
            kern_key = yield from cursor.fetchone()

            if kern_key is None:
                break
            if inst_id is not None:
                parent_inst_id = yield from self._conn.hget(kern_key, 'instance')
                if parent_inst_id != inst_id:
                    continue

            last_used = yield from self._conn.hget(kern_key, 'last_used')
            # NOTE: last_used field is set by neumann.
            if last_used is None:
                # If not set, just assume it is just used now.
                # TODO: log this situation
                if timeout == 0:
                    yield from self.destroy_kernel(kern_key)
                else:
                    yield from self._conn.hset(kern_key, 'last_used', datetime.now().isoformat())
            else:
                last_used = dateutil.parser.parse(last_used)
                now = datetime.now()
                if timeout == 0 or now - last_used > timedelta(seconds=timeout):
                    yield from self.destroy_kernel(kern_key)
