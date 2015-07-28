#! /usr/bin/env python3

import asyncio, asyncio_redis
from datetime import datetime
from namedlist import namedlist
import re
import uuid

class InstanceNotAvailableError(RuntimeError):
    pass

Instance = namedlist('Instance', [
    ('id', None),
    ('ip', None),
    ('docker_port', 2375), # standard docker daemon port
    ('tag', ''),
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


class InstanceRegistry:

    re_inst_id = re.compile(r'inst\.([-\w]+)\.')

    def __init__(self, redis_conn, kernel_driver):
        assert isinstance(redis_conn, asyncio_redis.Pool)
        self._conn = redis_conn
        self._driver = kernel_driver
        self._id = str(uuid.uuid4())

    @asyncio.coroutine
    def init(self):
        yield from self._conn.sadd('instance_registries', [self._id])
        yield from self._conn.set('{0}.meta.created'.format(self._id), datetime.now().isoformat())

    @asyncio.coroutine
    def terminate(self):
        # TODO: destroy all tracking kernels
        yield from self._conn.srem('instance_registries', [self._id])
        yield from self._conn.delete(['{0}.meta.created'.format(self._id)])

    @asyncio.coroutine
    def add_instance(self, instance_id, addr, port_range, max_kernels, tag=None):
        assert len(port_range) == max_kernels
        k_prefix = '{0}.inst.{1}'.format(self._id, instance_id)
        k_addr = k_prefix + '.addr'
        k_num = k_prefix + '.num_kernels'
        k_ports = k_prefix + '.ports'
        ret = False
        while not ret:
            tnx = yield from self._conn.multi(watch=[k_addr, k_num, k_ports])
            fut1 = yield from tnx.set(k_addr, addr)
            fut2 = yield from tnx.set(k_num, max_kernels)
            fut3 = yield from tnx.lpush(k_ports, port_range)
            yield from tnx.exec()
            ret = yield from fut3
        if tag:
            yield from self._conn.set(k_prefix + '.tag', tag)

    @asyncio.coroutine
    def create_kernel(self, spec='python34'):
        keys = yield from self._conn.scan('{0}.inst.*.num_kernels'.format(self._id))
        found = False
        for key_fut in keys:
            key = yield from key_fut
            ret = False
            while not ret:
                yield from self._conn.watch([key])
                tnx = yield from self._conn.multi()
                if self._conn.get(key) == 0:
                    tnx.unwatch()
                    ret = True
                else:
                    fut = yield from tnx.decr(key)
                    yield from tnx.exec()
                    ret = yield from fut
                    found = True
            if found:
                inst_id = self.re_inst_id.search(key).group(1)
                break

        if found:
            k_prefix = '{0}.inst.{1}'.format(self._id, inst_id)
            ip, tag = yield from self._conn.mget([k_prefix + '.addr', k_prefix + '.tag'])
            assigned_agent_port = yield from self._conn.lpop(k_prefix + '.ports')
            # TODO: create the kernel and fill the Kernel info.
            return Instance(id=inst_id, ip=ip, tag=tag), Kernel()
        else:
            raise InstanceNotAvailableError()
