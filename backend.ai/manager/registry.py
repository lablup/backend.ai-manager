import asyncio
from datetime import datetime
from dateutil.tz import tzutc
import functools
import logging
import operator

import aiozmq, aiozmq.rpc
from aiozmq.rpc.base import GenericError, NotFoundError, ParametersError
import aioredis
from async_timeout import timeout as _timeout
import zmq

from sorna.utils import dict2kvlist
from ..gateway.exceptions import (
    InstanceNotAvailable, InstanceNotFound, KernelNotFound,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from ..gateway.defs import (SORNA_KERNEL_DB, SORNA_INSTANCE_DB,
                            SORNA_SESSION_DB)
from .structs import Instance, Kernel

__all__ = ['InstanceRegistry', 'InstanceNotFound']

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


class RPCContext:

    preserved_exceptions = (
        NotFoundError,
        ParametersError,
        asyncio.TimeoutError,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    )

    def __init__(self, addr, timeout=10):
        self.addr = addr
        self.timeout = timeout
        self.server = None
        self.call = None

    async def __aenter__(self):
        self.server = await aiozmq.rpc.connect_rpc(
            connect=self.addr, error_table={
                'concurrent.futures._base.TimeoutError': asyncio.TimeoutError,
            })
        self.server.transport.setsockopt(zmq.LINGER, 50)
        self.call = self.server.call
        self.t = _timeout(self.timeout)
        self.t.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        recv_exc = exc_type is not None
        raise_pending = self.t.__exit__(exc_type, exc, tb)
        self.server.close()
        self.server = None
        self.call = None
        if recv_exc:
            if issubclass(exc_type, GenericError):
                e = AgentError(exc.args[0], exc.args[1])
                raise e.with_traceback(tb)
            elif issubclass(exc_type, self.preserved_exceptions):
                pass
            else:
                e = AgentError(exc_type, exc.args)
                raise e.with_traceback(tb)
        return recv_exc and raise_pending


class InstanceRegistry:
    '''
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    '''

    def __init__(self, redis_addr, gpu_instances=None, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.redis_addr = redis_addr
        self.redis_kern = None
        self.redis_inst = None
        self.redis_sess = None
        self.lifecycle_lock = asyncio.Lock()
        # FIXME: money-patched
        self.gpu_instances = gpu_instances if gpu_instances else set()

    async def init(self):
        self.redis_kern = await aioredis.create_pool(self.redis_addr.as_sockaddr(),
                                                     minsize=5,
                                                     encoding='utf8',
                                                     db=SORNA_KERNEL_DB,
                                                     loop=self.loop)
        self.redis_inst = await aioredis.create_pool(self.redis_addr.as_sockaddr(),
                                                     minsize=5,
                                                     encoding='utf8',
                                                     db=SORNA_INSTANCE_DB,
                                                     loop=self.loop)
        self.redis_sess = await aioredis.create_pool(self.redis_addr.as_sockaddr(),
                                                     minsize=5,
                                                     encoding='utf8',
                                                     db=SORNA_SESSION_DB,
                                                     loop=self.loop)
        log.debug('ready.')

    async def terminate(self):
        # Clean up all sessions.
        try:
            async with self.redis_sess.get() as rs:
                await rs.flushdb()
        except ConnectionRefusedError:
            pass
        self.redis_kern.close()
        self.redis_inst.close()
        self.redis_sess.close()
        await self.redis_kern.wait_closed()
        await self.redis_inst.wait_closed()
        await self.redis_sess.wait_closed()
        log.debug('terminated.')

    async def get_instance(self, inst_id, field=None):
        async with self.redis_inst.get() as ri:
            if field:
                value = await ri.hget(inst_id, field)
                if value is None:
                    raise InstanceNotFound(inst_id)
                return value
            else:
                fields = await ri.hgetall(inst_id)
                if not fields:
                    raise InstanceNotFound(inst_id)
                return Instance(**fields)

    async def enumerate_instances(self, check_shadow=True):
        async with self.redis_inst.get() as ri:
            if check_shadow:
                # check only "active" shadow instances.
                async for shadow_id in ri.iscan(match='shadow:i-*'):
                    inst_id = shadow_id[7:]  # strip "shadow:" prefix
                    if inst_id.endswith('.kernels'):
                        continue
                    yield inst_id
            else:
                async for inst_id in ri.iscan(match='i-*'):
                    if inst_id.endswith('.kernels'):
                        continue
                    yield inst_id

    @auto_get_instance
    async def update_instance(self, inst, updated_fields):
        async with self.redis_inst.get() as ri:
            await ri.hmset(inst.id, *dict2kvlist(updated_fields))

    async def get_kernel(self, kern_id, field=None, allow_stale=False):
        '''
        Retreive the kernel information as Kernel object.
        If ``field`` is given, it extracts only the raw value of the given field, without
        wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel owner instance.
        '''
        if field:
            async with self.redis_kern.get() as rk:
                value = await rk.hget(kern_id, field)
                # TODO: stale check?
                if value is None:
                    raise KernelNotFound
                return value
        else:
            async with self.redis_kern.get() as rk:
                fields = await rk.hgetall(kern_id)
            if not fields:
                raise KernelNotFound
            try:
                if not allow_stale:
                    if 'instance' not in fields:
                        # Received last heartbeats but the agent has restarted meanwhile.
                        raise KernelNotFound
                    async with self.redis_inst.get() as ri:
                        if not (await ri.exists(f"shadow:{fields['instance']}")):
                            # The agent heartbeat is timed out.
                            raise KernelNotFound
                        if not (await ri.sismember(f"{fields['instance']}.kernels", kern_id)):
                            # The agent is running but it has no such kernel.
                            raise KernelNotFound
                kern = Kernel(**fields)
                kern.apply_type()
                return kern
            except KernelNotFound:
                async with self.redis_kern.get() as rk:
                    await rk.delete(kern_id)
                raise

    async def get_kernels(self, kern_ids, field=None, allow_stale=False):
        '''
        Batched version of :meth:`get_kernel() <InstanceRegistry.get_kernel>`.
        The order of the returend array is same to the order of ``kern_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising KernelNotFound exception.
        '''
        if field:
            async with self.redis_kern.get() as rk:
                pipe = rk.pipeline()
                for kern_id in kern_ids:
                    pipe.hget(kern_id, field)
                values = await pipe.execute()
                return values
        else:
            async with self.redis_kern.get() as rk:
                pipe = rk.pipeline()
                for kern_id in kern_ids:
                    pipe.hgetall(kern_id)
                results = await pipe.execute()
            final_results = []
            for result in results:
                if not result:
                    final_results.append(None)
                    continue
                try:
                    if not allow_stale:
                        if 'instance' not in result:
                            # Received last heartbeats but the agent has restarted meanwhile.
                            raise KernelNotFound
                        async with self.redis_inst.get() as ri:
                            if not (await ri.exists(f"shadow:{result['instance']}")):
                                # The agent heartbeat is timed out.
                                raise KernelNotFound
                            if not (await ri.sismember(f"{result['instance']}.kernels", kern_id)):
                                # The agent is running but it has no such kernel.
                                raise KernelNotFound
                    kern = Kernel(**result)
                    kern.apply_type()
                    final_results.append(kern)
                except KernelNotFound:
                    async with self.redis_kern.get() as rk:
                        await rk.delete(kern_id)
                    final_results.append(None)
            return final_results

    async def get_kernel_from_session(self, client_sess_token, lang):
        async with self.redis_sess.get() as rs:
            sess_key = '{0}:{1}'.format(client_sess_token, lang)
            kern_id = await rs.get(sess_key)
            if kern_id:
                return (await self.get_kernel(kern_id))
            else:
                raise KernelNotFound

    async def get_or_create_kernel(self, client_sess_token, lang, owner_access_key,
                                   limits=None, mounts=None):
        assert owner_access_key
        try:
            kern = await self.get_kernel_from_session(client_sess_token, lang)
            created = False
        except KernelNotFound:
            # Create a new kernel.
            async with self.redis_sess.get() as rs:
                _, kern = await self.create_kernel(lang, owner_access_key,
                                                   limits=limits, mounts=mounts)
                sess_key = '{0}:{1}'.format(client_sess_token, lang)
                await rs.set(sess_key, kern.id)
                created = True
        assert kern is not None
        return kern, created

    async def create_kernel(self, lang, owner_access_key, limits=None, mounts=None):
        inst_id = None
        limits = limits or {}
        mounts = mounts or []

        # FIXME: monkey-patch for deep-learning support
        # Let it choose a GPU instance if the requested kernel session is a deep-learning type.
        gpu_requested = 'tensorflow' in lang or 'caffe' in lang or 'torch' in lang

        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri:  # noqa

            inst_loads = []

            # Find an agent instance having free kernel slots and least occupied slots.
            async for inst_id in self.enumerate_instances():
                if ((gpu_requested and inst_id not in self.gpu_instances) or
                    (not gpu_requested and inst_id in self.gpu_instances)):
                    # Skip if not desired type of instances.
                    continue
                try:
                    max_kernels = int(await ri.hget(inst_id, 'max_kernels'))
                    num_kernels = int(await ri.hget(inst_id, 'num_kernels'))
                    if num_kernels < max_kernels:
                        inst_loads.append((inst_id, num_kernels))
                except (TypeError, ValueError):
                    inst_id = None

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

        try:
            async with RPCContext(instance.addr, 10) as rpc:
                kern_id, stdin_port, stdout_port = \
                    await rpc.call.create_kernel(lang, limits, mounts)
        except asyncio.TimeoutError:
            raise KernelCreationFailed('TIMEOUT')
        except AgentError as e:
            log.exception('create_kernel: agent-side error')
            raise KernelCreationFailed('FAILURE', e)
        except:
            log.exception('create_kernel: unexpected error')
            raise
        assert kern_id is not None

        log.debug(f'create_kernel() -> {kern_id} on {inst_id}')
        async with self.redis_kern.get() as rk, \
                   self.redis_inst.get() as ri:  # noqa
            kernel_info = {
                'id': kern_id,
                'instance': instance.id,
                'access_key': owner_access_key,
                'lang': lang,
                # all kernels in an agent shares the same agent address
                # (the agent multiplexes requests to different kernels)
                'addr': instance.addr,
                'stdin_port': stdin_port,
                'stdout_port': stdout_port,
                'created_at': datetime.now(tzutc()).isoformat(),
            }
            await rk.hmset(kern_id, *dict2kvlist(kernel_info))
            await ri.sadd(f'{instance.id}.kernels', kern_id)
            kernel = Kernel(**kernel_info)
        return instance, kernel

    @auto_get_kernel
    async def destroy_kernel(self, kernel):
        log.debug(f'destroy_kernel({kernel.id})')
        try:
            async with RPCContext(kernel.addr, 10) as rpc:
                await rpc.call.destroy_kernel(kernel.id)
        except asyncio.TimeoutError:
            raise KernelDestructionFailed('TIMEOUT')
        except asyncio.CancelledError:
            raise
        except AgentError as e:
            log.exception('destroy_kernel: agent-side error')
            raise KernelDestructionFailed('FAILURE', e)
        except:
            log.exception('destroy_kernel: unexpected error')
            raise

    @auto_get_kernel
    async def restart_kernel(self, kernel):
        log.debug(f'restart_kernel({kernel.id})')
        try:
            async with RPCContext(kernel.addr, 30) as rpc:
                stdin_port, stdout_port = await rpc.call.restart_kernel(kernel.id)
                async with self.redis_kern.get() as rk:
                    rk_pipe = rk.pipeline()
                    rk_pipe.hset(kernel.id, 'stdin_port', stdin_port)
                    rk_pipe.hset(kernel.id, 'stdout_port', stdout_port)
                    await rk_pipe.execute()
        except asyncio.TimeoutError:
            raise KernelRestartFailed('TIMEOUT')
        except asyncio.CancelledError:
            raise
        except AgentError as e:
            log.exception('restart_kernel: agent-side error')
            raise KernelRestartFailed('FAILURE', e)
        except:
            log.exception('restart_kernel: unexpected error')
            raise

    @auto_get_kernel
    async def update_kernel(self, kernel, updated_fields):
        log.debug(f'update_kernel({kernel.id})')
        async with self.redis_kern.get() as rk:
            await rk.hmset(kernel.id, *dict2kvlist(updated_fields))

    @auto_get_kernel
    async def execute_snippet(self, kernel, api_version, mode, code, opts):
        log.debug(f'execute_snippet:v{api_version}({kernel.id}, {mode}')
        try:
            async with RPCContext(kernel.addr, 200) as rpc:  # must be longer than kernel exec_timeout
                result = await rpc.call.execute_code(api_version, kernel.id,
                                                     mode, code, opts)
        except asyncio.TimeoutError:
            raise KernelExecutionFailed('TIMEOUT')
        except asyncio.CancelledError:
            raise
        except AgentError as e:
            log.exception('execute_code: agent-side error')
            raise KernelExecutionFailed('FAILURE', e)
        except:
            log.exception('execute_code: unexpected error')
            raise
        return result

    @auto_get_kernel
    async def upload_file(self, kernel, filename, filedata):
        log.debug(f'upload_file({kernel.id}, {filename})')
        try:
            async with RPCContext(kernel.addr, 10000) as rpc:
                result = await rpc.call.upload_file(kernel.id, filename, filedata)
        except asyncio.TimeoutError:
            raise KernelExecutionFailed('TIMEOUT')
        except asyncio.CancelledError:
            raise
        except AgentError as e:
            log.exception('execute_code: agent-side error')
            raise KernelExecutionFailed('FAILURE', e)
        except:
            log.exception('execute_code: unexpected error')
            raise
        return result

    @auto_get_kernel
    async def increment_kernel_usage(self, kernel):
        log.debug(f'increment_kernel_usage({kernel.id})')
        async with self.redis_kern.get() as rk:
            await rk.hincrby(kernel.id, 'num_queries', 1)

    async def get_kernels_in_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri:  # noqa
            kern_ids = await ri.smembers(f'{inst_id}.kernels')
            if kern_ids is None:
                return []
            return kern_ids

    async def handle_stats(self, inst_id, kern_stats, interval):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:  # noqa

            rk_pipe = rk.pipeline()
            for kern_id in kern_stats.keys():
                rk_pipe.exists(kern_id)
            kernel_existence = await rk_pipe.execute()

            ri_pipe = ri.pipeline()
            rk_pipe = rk.pipeline()
            for (kern_id, stats), alive in zip(kern_stats.items(), kernel_existence):
                if alive:
                    ri_pipe.sadd(f'{inst_id}.kernels', kern_id)
                    rk_pipe.hmset(kern_id, *dict2kvlist(stats))
            await ri_pipe.execute()
            await rk_pipe.execute()

    async def handle_heartbeat(self, inst_id, inst_info, running_kernels, interval):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:  # noqa

            rk_pipe = rk.pipeline()
            for kern_id in running_kernels:
                rk_pipe.exists(kern_id)
            kernel_existence = await rk_pipe.execute()

            ri_pipe = ri.pipeline()
            rk_pipe = rk.pipeline()
            for kern_id, alive in zip(running_kernels, kernel_existence):
                if alive:
                    ri_pipe.sadd(f'{inst_id}.kernels', kern_id)
                    rk_pipe.hset(kern_id, 'instance', inst_id)
            # Create a "shadow" key that actually expires.
            # This allows access to agent information upon expiration events.
            ri_pipe.hmset(inst_id, *dict2kvlist(inst_info))
            ri_pipe.set(f'shadow:{inst_id}', '')
            ri_pipe.expire(f'shadow:{inst_id}', float(interval * 3.3))
            await ri_pipe.execute()
            await rk_pipe.execute()

    async def revive_instance(self, inst_id, inst_addr):
        try:
            async with RPCContext(inst_addr, 10) as rpc:
                await rpc.call.reset()
        except asyncio.TimeoutError:
            log.warning('revive_instance timeout')
        except AgentError as e:
            log.exception('revive_instance: agent-side error')
        except:
            log.exception('revive_instance: unexpected error')
        await self.reset_instance(inst_id)

    async def reset_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri:  # noqa
            # Clear the running kernels set.
            ri_pipe = ri.pipeline()
            ri_pipe.hset(inst_id, 'status', 'running')
            ri_pipe.hset(inst_id, 'num_kernels', 0)
            ri_pipe.delete(f'{inst_id}.kernels')
            await ri_pipe.execute()

    async def forget_kernel(self, kern_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:  # noqa
            rk_pipe = rk.pipeline()
            rk_pipe.hget(kern_id, 'instance')
            rk_pipe.delete(kern_id)
            results = await rk_pipe.execute()
            inst_id = results[0]
            if inst_id:
                ri_pipe = ri.pipeline()
                ri_pipe.hincrby(inst_id, 'num_kernels', -1)
                ri_pipe.srem(f'{inst_id}.kernels', kern_id)
                await ri_pipe.execute()

    async def forget_all_kernels_in_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:  # noqa
            ri_pipe = ri.pipeline()
            ri_pipe.smembers(f'{inst_id}.kernels')
            ri_pipe.hset(inst_id, 'num_kernels', 0)
            ri_pipe.delete(f'{inst_id}.kernels')
            results = await ri_pipe.execute()
            kern_ids = results[0]
            rk_pipe = rk.pipeline()
            if kern_ids:
                for kern_id in kern_ids:
                    rk_pipe.delete(kern_id)
            await rk_pipe.execute()

    async def forget_instance(self, inst_id):
        async with self.lifecycle_lock, \
                   self.redis_inst.get() as ri, \
                   self.redis_kern.get() as rk:  # noqa
            ri_pipe = ri.pipeline()
            ri_pipe.smembers(f'{inst_id}.kernels')
            # Delete shadow key immediately to prevent bogus agent-lost events.
            ri_pipe.delete(f'shadow:{inst_id}')
            ri_pipe.delete(inst_id)
            ri_pipe.delete(f'{inst_id}.kernels')
            results = await ri_pipe.execute()
            kern_ids = results[0]
            if kern_ids:
                await rk.delete(*kern_ids)
            return kern_ids
