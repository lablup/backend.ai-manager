#! /usr/bin/env python3

import asyncio
from datetime import datetime
from dateutil.tz import tzutc
import functools
from itertools import chain
import logging

import zmq, aiozmq
import aioredis

from sorna import defs
from sorna.exceptions import \
    InstanceNotAvailableError, \
    InstanceNotFoundError, KernelNotFoundError, \
    KernelCreationFailedError, KernelDestructionFailedError
from sorna.proto import Message
from sorna.proto.msgtypes import AgentRequestTypes, SornaResponseTypes
from .structs import Instance, Kernel

__all__ = ['InstanceRegistry', 'InstanceNotFoundError']

# A shortcut for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_r = lambda fmt, reg_id, *args, **kwargs: \
    'registry[{}]: '.format(reg_id) + fmt.format(*args, **kwargs)

log = logging.getLogger(__name__)


def auto_get_kernel(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not isinstance(args[0], Kernel):
            arg0 = await self.get_kernel(args[0])
            return await func(self, arg0, *args[1:], **kwargs)
        return await func(self, *args, **kwargs)
    return wrapper


def auto_get_instance(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not isinstance(args[0], Instance):
            arg0 = await self.get_instance(args[0])
            return await func(self, arg0, *args[1:], **kwargs)
        return await func(self, *args, **kwargs)
    return wrapper


def _s(obj):
    if obj is None:
        return ''
    return str(obj)


class InstanceRegistry:
    '''
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    '''

    def __init__(self, redis_addr, kernel_timeout=600, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.redis_addr = redis_addr
        self.kernel_timeout = kernel_timeout
        self.redis_inst = None
        self.lifecycle_lock = asyncio.Lock()

    async def init(self):
        self.redis_kern = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=defs.SORNA_KERNEL_DB,
                                                     loop=self.loop)
        self.redis_inst = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=defs.SORNA_INSTANCE_DB,
                                                     loop=self.loop)
        self.redis_sess = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=defs.SORNA_SESSION_DB,
                                                     loop=self.loop)
        log.info('ready.')

    async def terminate(self):
        # Clean up all sessions.
        async with self.redis_sess.get() as rs:
            await rs.flushdb()
        self.redis_kern.close()
        self.redis_inst.close()
        self.redis_sess.close()
        await self.redis_kern.wait_closed()
        await self.redis_inst.wait_closed()
        await self.redis_sess.wait_closed()
        log.info('terminated.')

    async def get_instance(self, inst_id):
        async with self.redis_inst.get() as ri:
            fields = await ri.hgetall(inst_id)
            if not fields:
                raise InstanceNotFoundError(inst_id)
            return Instance(**fields)

    async def get_kernel(self, kern_id):
        async with self.redis_kern.get() as rk:
            fields = await rk.hgetall(kern_id)
            if fields:
                # Check if stale.
                try:
                    async with self.redis_inst.get() as ri:
                        kern_set_key = fields['instance'] + '.kernels'
                        if not (await ri.sismember(kern_set_key, kern_id)):
                            raise KernelNotFoundError(kern_id)
                        if not (await ri.exists(fields['instance'])):
                            raise KernelNotFoundError(kern_id)
                        return Kernel(**fields)
                except KernelNotFoundError:
                    await rk.delete(kern_id)
                    raise
            else:
                raise KernelNotFoundError(kern_id)

    async def get_kernel_from_session(self, client_sess_token, lang):
        async with self.redis_sess.get() as rs:
            sess_key = '{0}:{1}'.format(client_sess_token, lang)
            kern_id = await rs.get(sess_key)
            if kern_id:
                return (await self.get_kernel(kern_id))
            else:
                raise KernelNotFoundError()

    async def get_or_create_kernel(self, client_sess_token, lang, spec=None):
        async with self.lifecycle_lock:
            try:
                kern = await self.get_kernel_from_session(client_sess_token, lang)
            except KernelNotFoundError:
                # Create a new kernel.
                async with self.redis_sess.get() as rs:
                    _, kern = await self.create_kernel(lang, spec)
                    sess_key = '{0}:{1}'.format(client_sess_token, lang)
                    await rs.set(sess_key, kern.id)
        assert kern is not None
        return kern

    async def create_kernel(self, lang, spec=None):
        _spec = {
            'cpu_shares': 1024,
        }
        if spec:
            _spec.update(spec)
        if not self.lifecycle_lock.locked:
            await self.lifecycle_lock
        else:
            log.info(_f('create_kernel with spec: {!r}', _spec))

            # Find available instance.
            inst_id = None
            async with self.redis_inst.get() as ri:
                found_available = False
                # FIXME: monkey-patch for deep-learning support
                if 'tensorflow' in lang or 'caffe' in lang:
                    inst_id = 'i-indominus'
                    if (await ri.exists(inst_id)):
                        max_kernels = int(await ri.hget(inst_id, 'max_kernels'))
                        num_kernels = int(await ri.hget(inst_id, 'num_kernels'))
                        if num_kernels < max_kernels:
                            await ri.hincrby(inst_id, 'num_kernels', 1)
                            found_available = True
                else:
                    async for inst_id in ri.iscan(match='i-*'):
                        if inst_id.endswith('.kernels'):
                            continue
                        max_kernels = int(await ri.hget(inst_id, 'max_kernels'))
                        num_kernels = int(await ri.hget(inst_id, 'num_kernels'))
                        if num_kernels < max_kernels:
                            await ri.hincrby(inst_id, 'num_kernels', 1)
                            # This will temporarily increase num_kernels,
                            # and it will be "fixed" by the agent when it sends
                            # the next heartbeat.
                            found_available = True
                            break
                if not found_available:
                    # TODO: automatically add instances to some extent
                    log.error('instance not available.')
                    raise InstanceNotAvailableError('Could not find available instance.')
            assert inst_id is not None

            # Create kernel by invoking the agent on the instance.
            log.info(_f('grabbed instance {}', inst_id))
            kern_id = None
            stdin_port = None
            stdout_port = None
            instance = await self.get_instance(inst_id)
            conn = await aiozmq.create_zmq_stream(zmq.REQ, connect=instance.addr,
                                                  loop=self.loop)
            conn.transport.setsockopt(zmq.SNDHWM, 50)
            request = Message()
            request['action'] = AgentRequestTypes.CREATE_KERNEL
            request['lang'] = lang
            conn.write([request.encode()])
            try:
                resp_data = await asyncio.wait_for(conn.read(), timeout=3)
            except asyncio.TimeoutError:
                log.error('failed to create kernel; TIMEOUT: agent did not respond.')
                raise KernelCreationFailedError('TIMEOUT', 'agent did not respond')
            else:
                response = Message.decode(resp_data[0])
                if response['reply'] == SornaResponseTypes.SUCCESS:
                    kern_id = response['kernel_id']
                    stdin_port = response['stdin_port']
                    stdout_port = response['stdout_port']
                else:
                    err_name = SornaResponseTypes(response['reply']).name
                    err_cause = response['cause']
                    log.error(_f('failed to create kernel; {}: {}',
                                 err_name, err_cause))
                    raise KernelCreationFailedError(err_name, err_cause)
            finally:
                conn.close()
            assert kern_id is not None

        log.info(_f('created kernel {} on instance {}', kern_id, inst_id))
        async with self.redis_kern.get() as rk, \
                   self.redis_inst.get() as ri:
            kernel_info = {
                'id': kern_id,
                'instance': instance.id,
                'lang': lang,
                # all kernels in an agent shares the same agent address
                # (the agent multiplexes requests to different kernels)
                'addr': instance.addr,
                'stdin_port': stdin_port,
                'stdout_port': stdout_port,
                'created_at': datetime.now(tzutc()).isoformat(),
            }
            kv_list = chain.from_iterable((k, v) for k, v in kernel_info.items())
            await rk.hmset(kern_id, *kv_list)
            await ri.sadd(instance.id + '.kernels', kern_id)
            kernel = Kernel(**kernel_info)
        return instance, kernel

    @auto_get_kernel
    async def destroy_kernel(self, kernel):
        log.info(_f('destroy_kernel ({})', kernel.id))
        async with self.lifecycle_lock:
            conn = await aiozmq.create_zmq_stream(zmq.REQ, connect=kernel.addr,
                                                  loop=self.loop)
            conn.transport.setsockopt(zmq.SNDHWM, 50)
            request = Message()
            request['action'] = AgentRequestTypes.DESTROY_KERNEL
            request['kernel_id'] = kernel.id
            conn.write([request.encode()])
            try:
                resp_data = await asyncio.wait_for(conn.read(), timeout=3)
            except asyncio.TimeoutError:
                log.error('failed to destroy kernel; TIMEOUT: agent did not respond.')
                raise KernelDestructionFailedError('TIMEOUT', 'agent did not respond')
            else:
                response = Message.decode(resp_data[0])
                if response['reply'] != SornaResponseTypes.SUCCESS:
                    err_name = SornaResponseTypes(response['reply']).name
                    err_cause = response['cause']
                    log.error(_f('failed to destroy kernel; {}: {}',
                                 err_name, err_cause))
                    raise KernelDestructionFailedError(err_name, err_cause)
            finally:
                conn.close()
            assert response['reply'] == SornaResponseTypes.SUCCESS
            # decrementing num_kernels and removing kernel ID from running kernels
            # are already done by the agent.
