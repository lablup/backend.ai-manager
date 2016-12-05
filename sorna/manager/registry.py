import asyncio
from datetime import datetime
from dateutil.tz import tzutc
import functools
import logging
import operator

import aiozmq, aiozmq.rpc
import aioredis
from async_timeout import timeout as _timeout
import zmq

from sorna.defs import SORNA_KERNEL_DB, SORNA_INSTANCE_DB, \
                       SORNA_SESSION_DB
from sorna.utils import dict2kvlist
from sorna.exceptions import \
    InstanceNotAvailable, InstanceNotFound, KernelNotFound, \
    KernelCreationFailed, KernelDestructionFailed, \
    KernelExecutionFailed, KernelRestartFailed
from .structs import Instance, Kernel

__all__ = ['InstanceRegistry', 'InstanceNotFound']

# A shortcut for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_r = lambda fmt, reg_id, *args, **kwargs: \
    'registry[{}]: '.format(reg_id) + fmt.format(*args, **kwargs)

log = logging.getLogger('sorna.manager.registry')


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

    def __init__(self, redis_addr, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.redis_addr = redis_addr
        self.redis_kern = None
        self.redis_inst = None
        self.redis_sess = None
        self.lifecycle_lock = asyncio.Lock()

    async def init(self):
        self.redis_kern = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=SORNA_KERNEL_DB,
                                                     loop=self.loop)
        self.redis_inst = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=SORNA_INSTANCE_DB,
                                                     loop=self.loop)
        self.redis_sess = await aioredis.create_pool(self.redis_addr,
                                                     encoding='utf8',
                                                     db=SORNA_SESSION_DB,
                                                     loop=self.loop)
        log.debug('ready.')

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
        log.debug('terminated.')

    async def get_instance(self, inst_id):
        async with self.redis_inst.get() as ri:
            fields = await ri.hgetall(inst_id)
            if not fields:
                raise InstanceNotFound(inst_id)
            return Instance(**fields)

    async def get_kernel(self, kern_id):
        async with self.redis_kern.get() as rk:
            fields = await rk.hgetall(kern_id)
            if fields:
                # Check if stale.
                try:
                    async with self.redis_inst.get() as ri:
                        if 'instance' not in fields:
                            # Received last heartbeats but the agent has restarted meanwhile.
                            raise KernelNotFound
                        if not (await ri.exists('shadow:' + fields['instance'])):
                            # The agent heartbeat is timed out.
                            raise KernelNotFound
                        kern_set_key = fields['instance'] + '.kernels'
                        if not (await ri.sismember(kern_set_key, kern_id)):
                            # The agent is running but it has no such kernel.
                            raise KernelNotFound
                        return Kernel(**fields)
                except KernelNotFound:
                    await rk.delete(kern_id)
                    raise
            else:
                raise KernelNotFound

    async def get_kernel_from_session(self, client_sess_token, lang):
        async with self.redis_sess.get() as rs:
            sess_key = '{0}:{1}'.format(client_sess_token, lang)
            kern_id = await rs.get(sess_key)
            if kern_id:
                return (await self.get_kernel(kern_id))
            else:
                raise KernelNotFound

    async def get_or_create_kernel(self, client_sess_token, lang, spec=None):
        try:
            kern = await self.get_kernel_from_session(client_sess_token, lang)
        except KernelNotFound:
            # Create a new kernel.
            async with self.redis_sess.get() as rs:
                _, kern = await self.create_kernel(lang, spec)
                sess_key = '{0}:{1}'.format(client_sess_token, lang)
                await rs.set(sess_key, kern.id)
        assert kern is not None
        return kern

    async def create_kernel(self, lang, spec=None):
        inst_id = None
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri:
            # Find available instance.
            # FIXME: monkey-patch for deep-learning support
            if 'tensorflow' in lang or 'caffe' in lang:
                inst_id = 'i-indominus'
                if (await ri.exists(inst_id)):
                    max_kernels = int(await ri.hget(inst_id, 'max_kernels'))
                    num_kernels = int(await ri.hget(inst_id, 'num_kernels'))
                    if not (num_kernels < max_kernels):
                        inst_id = None
            else:
                # Scan all agent instances with free kernel slots.
                # We scan shadow keys first to check only alive instances,
                # and then fetch details from normal keys.
                inst_loads = []
                async for shadow_id in ri.iscan(match='shadow:i-*'):
                    inst_id = shadow_id[7:]  # strip "shadow:" prefix
                    if inst_id.endswith('.kernels') or inst_id == 'i-indominus':
                        continue
                    max_kernels = int(await ri.hget(inst_id, 'max_kernels'))
                    num_kernels = int(await ri.hget(inst_id, 'num_kernels'))
                    if num_kernels < max_kernels:
                        inst_loads.append((inst_id, num_kernels))
                if inst_loads:
                    # Choose a least-loaded agent instance.
                    inst_id = min(inst_loads, key=operator.itemgetter(1))[0]
                else:
                    inst_id = None

            if inst_id is not None:
                await ri.hincrby(inst_id, 'num_kernels', 1)

        if inst_id is None:
            raise InstanceNotAvailable

        # Create kernel by invoking the agent on the instance.
        kern_id = None
        stdin_port = None
        stdout_port = None
        instance = await self.get_instance(inst_id)

        agent = await aiozmq.rpc.connect_rpc(connect=instance.addr)
        agent.transport.setsockopt(zmq.LINGER, 50)
        try:
            with _timeout(10):
                kern_id, stdin_port, stdout_port = \
                    await agent.call.create_kernel(lang, {})
        except asyncio.TimeoutError:
            raise KernelCreationFailed('TIMEOUT')
        except Exception as e:
            log.exception('create_kernel')
            msg = ', '.join(map(str, e.args))
            raise KernelCreationFailed('FAILURE', msg)
        finally:
            agent.close()
        assert kern_id is not None

        log.debug(_f('create_kernel() -> {} on {}', kern_id, inst_id))
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
            await rk.hmset(kern_id, *dict2kvlist(kernel_info))
            await ri.sadd(instance.id + '.kernels', kern_id)
            kernel = Kernel(**kernel_info)
        return instance, kernel

    @auto_get_kernel
    async def destroy_kernel(self, kernel):
        log.debug(_f('destroy_kernel({})', kernel.id))
        agent = await aiozmq.rpc.connect_rpc(connect=kernel.addr)
        agent.transport.setsockopt(zmq.LINGER, 50)
        try:
            with _timeout(10):
                await agent.call.destroy_kernel(kernel.id)
        except asyncio.TimeoutError:
            raise KernelDestructionFailed('TIMEOUT')
        except Exception as e:
            log.exception('destroy_kernel')
            msg = ', '.join(map(str, e.args))
            raise KernelDestructionFailed('FAILURE', msg)
        finally:
            agent.close()

    @auto_get_kernel
    async def restart_kernel(self, kernel):
        log.debug(_f('restart_kernel({})', kernel.id))
        agent = await aiozmq.rpc.connect_rpc(connect=kernel.addr)
        agent.transport.setsockopt(zmq.LINGER, 50)
        try:
            with _timeout(10):
                await agent.call.restart_kernel(kernel.id)
        except asyncio.TimeoutError:
            raise KernelRestartFailed('TIMEOUT')
        except Exception as e:
            log.exception('restart_kernel')
            msg = ', '.join(map(str, e.args))
            raise KernelRestartFailed('FAILURE', msg)
        finally:
            agent.close()

    @auto_get_kernel
    async def update_kernel(self, kernel, updated_fields):
        log.debug(_f('update_kernel({})', kernel.id))
        async with self.redis_kern.get() as rk:
            await rk.hmset(kernel.id, *dict2kvlist(updated_fields))

    @auto_get_kernel
    async def execute_snippet(self, kernel, code_id, code):
        log.debug(_f('execute_snippet({}, ...)', kernel.id))
        agent = await aiozmq.rpc.connect_rpc(connect=kernel.addr)
        agent.transport.setsockopt(zmq.LINGER, 50)
        try:
            with _timeout(200):  # must be longer than kernel exec_timeout
                result = await agent.call.execute_code('0', kernel.id,
                                                        code_id, code, {})
        except asyncio.TimeoutError:
            raise KernelExecutionFailed('TIMEOUT')
        except Exception as e:
            log.exception('execute_code')
            msg = ', '.join(map(str, e.args))
            raise KernelExecutionFailed('FAILURE', msg)
        finally:
            agent.close()
        return result

    async def handle_heartbeat(self, inst_id, inst_info, kern_stats, interval):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:
            ri_pipe = ri.pipeline()
            rk_pipe = rk.pipeline()
            ri_pipe.hmset(inst_id, *dict2kvlist(inst_info))
            my_kernels_key = '{}.kernels'.format(inst_id)
            for kern_id, stats in kern_stats.items():
                ri_pipe.sadd(my_kernels_key, kern_id)
                rk_pipe.hmset(kern_id, *dict2kvlist(stats))

            # Create a "shadow" key that actually expires.
            # This allows access to agent information upon expiration events.
            ri_pipe.set('shadow:' + inst_id, '')
            ri_pipe.expire('shadow:' + inst_id, float(interval * 2))
            try:
                await ri_pipe.execute()
                await rk_pipe.execute()
            except asyncio.CancelledError:
                pass
            except:
                log.exception('heartbeat error')

    async def revive_instance(self, inst_id, inst_addr):
        agent = await aiozmq.rpc.connect_rpc(connect=inst_addr)
        agent.transport.setsockopt(zmq.LINGER, 50)
        try:
            with _timeout(10):
                await agent.call.reset()
        except asyncio.TimeoutError:
            log.warning('revive_instance timeout')
        finally:
            agent.close()
        await self.reset_instance(inst_id)

    async def reset_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri:
            # Clear the running kernels set.
            await ri.hset(inst_id, 'num_kernels', 0)
            await ri.delete(inst_id + '.kernels')

    async def clean_kernel(self, kern_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:
            inst_id = await rk.hget(kern_id, 'instance')
            await ri.hincrby(inst_id, 'num_kernels', -1)
            await ri.srem(inst_id + '.kernels', kern_id)
            await rk.delete(kern_id)

    async def clean_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:
            kern_ids = await ri.smembers(inst_id + '.kernels')
            pipe = ri.pipeline()
            pipe.delete(inst_id)
            pipe.delete(inst_id + '.kernels')
            await pipe.execute()
            if kern_ids:
                await rk.delete(*kern_ids)
            return kern_ids
