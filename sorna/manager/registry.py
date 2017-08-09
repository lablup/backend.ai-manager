import asyncio
from contextlib import contextmanager
from datetime import datetime
from dateutil.tz import tzutc
import functools
import logging
import operator

import aiozmq, aiozmq.rpc
from aiozmq.rpc.base import GenericError, NotFoundError, ParametersError
import aioredis
import aiotools
from async_timeout import timeout as _timeout
import sqlalchemy as sa
import zmq

from sorna.common.utils import dict2kvlist
from ..gateway.exceptions import (
    InstanceNotAvailable, InstanceNotFound, KernelNotFound,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from ..gateway.defs import (SORNA_KERNEL_DB, SORNA_INSTANCE_DB,
                            SORNA_SESSION_DB)
from .models import agents, kernels, ResourceSlot, AgentStatus, KernelStatus
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


# TODO: rename to AgentRegistry
class InstanceRegistry:
    '''
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    '''

    def __init__(self, dbpool, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.dbpool = dbpool

    async def init(self):
        log.debug('ready.')

    async def terminate(self):
        pass

    async def get_instance(self, inst_id, field=None):
        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.select(['id', field] if field else None)
                       .select_from(agents)
                       .where(agents.c.id == inst_id))
            row = await conn.fetchrow(query)
            if not row:
                raise InstanceNotFound(inst_id)
            return row

        #async with self.redis_inst.get() as ri:
        #    if field:
        #        value = await ri.hget(inst_id, field)
        #        if value is None:
        #            raise InstanceNotFound(inst_id)
        #        return value
        #    else:
        #        fields = await ri.hgetall(inst_id)
        #        if not fields:
        #            raise InstanceNotFound(inst_id)
        #        return Instance(**fields)

    async def enumerate_instances(self, check_shadow=True):
        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.select_from(agents))
            for row in await conn.fetch(query):
                yield row

        #async with self.redis_inst.get() as ri:
        #    if check_shadow:
        #        # check only "active" shadow instances.
        #        async for shadow_id in ri.iscan(match='shadow:i-*'):
        #            inst_id = shadow_id[7:]  # strip "shadow:" prefix
        #            if inst_id.endswith('.kernels'):
        #                continue
        #            yield inst_id
        #    else:
        #        async for inst_id in ri.iscan(match='i-*'):
        #            if inst_id.endswith('.kernels'):
        #                continue
        #            yield inst_id

    async def update_instance(self, inst_id, updated_fields):
        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.update(agents)
                       .values(**updated_fields)
                       .where(agents.c.id == inst.instance_id))
            await conn.execute(query)

        # async with self.redis_inst.get() as ri:
        #     await ri.hmset(inst.id, *dict2kvlist(updated_fields))

    @aiotools.actxmgr
    async def handle_kernel_exception(self, op, sess_id,
                                error_callback=None,
                                cancellation_callback=None):
        op_exc = {
            'create_kernel': KernelCreationFailed,
            'restart_kernel': KernelRestartFailed,
            'destroy_kernel': KernelDestructionFailed,
            'execute': KernelExecutionFailed,
            'upload_file': KernelExecutionFailed,
        }
        exc_class = op_exc[op]
        try:
            yield
        except asyncio.TimeoutError:
            await self.set_kernel_status(sess_id, KernlStatus.ERROR,
                                         status_info=f'Operation timeout ({op})')
            if error_callback:
                await error_callback()
            raise exc_class('TIMEOUT')
        except asyncio.CancelledError:
            if cancellation_callback:
                await cancellation_callback()
            raise
        except AgentError as e:
            log.exception(f'{op}: agent-side error')
            await self.set_kernel_status(sess_id, KernlStatus.ERROR,
                                         status_info='Agent error')
            if error_callback:
                await error_callback()
            raise exc_class('FAILURE', e)
        except:
            log.exception(f'{op}: unexpected error')
            # TODO: raven.captureException()
            await self.set_kernel_status(sess_id, KernlStatus.ERROR,
                                         status_info='Unexpected error')
            if error_callback:
                await error_callback()
            raise

    async def get_kernel(self, sess_id, field=None, allow_stale=False):
        '''
        Retreive the kernel information as Kernel object.
        If ``field`` is given, it extracts only the raw value of the given field, without
        wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel owner instance.
        '''
        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.select(['sess_id', field] if field else None)
                       .select_from(kernels)
                       .where(kernels.c.sess_id == sess_id))
            if not allow_stale:
                query = query.where(kernels.c.agent.status == AgentStatus.ALIVE)
            row = await conn.fetchrow(query)
            if not row:
                raise KernelNotFound
            return row

    async def get_kernels(self, kern_ids, field=None, allow_stale=False):
        '''
        Batched version of :meth:`get_kernel() <InstanceRegistry.get_kernel>`.
        The order of the returend array is same to the order of ``kern_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising KernelNotFound exception.
        '''

        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.select(kernels)
                       .where(kernel.c.sess_id in kern_ids))
            if not allow_stale:
                query = query.where(kernels.c.agent.status == AgentStatus.ALIVE)
            for row in await conn.fetch(query):
                yield row

    async def set_kernel_status(self, sess_id, status, **extra_fields):
        data = {
            'status': status,
        }
        data.update(extra_fields)
        async with self.dbpool.acquire() as conn, conn.transaction():
            query = (sa.update(kernels)
                       .values(data)
                       .where(kernels.c.sess_id == sess_id))

    async def get_or_create_kernel(self, client_sess_token, lang, owner_access_key,
                                   limits=None, mounts=None):
        assert owner_access_key
        try:
            kern = await self.get_kernel(client_sess_token)
            created = False
        except KernelNotFound:
            kern = await self.create_kernel(client_sess_token, lang, owner_access_key,
                                            limits=limits, mounts=mounts)
            created = True
        assert kern is not None
        return kern, created

    def get_kernel_slot(self, lang):
        # TODO: implement
        return ResourceSlot(
            None, 1, 1, 1
        )

    async def create_kernel(self, sess_id, lang, owner_access_key, limits=None, mounts=None):
        inst_id = None
        limits = limits or {}
        mounts = mounts or []

        required_slot = self.get_kernel_slot(lang)

        async with self.dbpool.acquire() as conn, conn.transaction():

            avail_slots = []
            query = (sa.select([agents], for_update=True))
            for row in await conn.fetch(query):
                sdiff = ResourceSlot(
                    row['id'],
                    row['mem_slots'] - row['used_mem_slots'] - required_slot.mem,
                    row['cpu_slots'] - row['used_cpu_slots'] - required_slot.cpu,
                    row['gpu_slots'] - row['used_gpu_slots'] - required_slot.gpu
                )
                avail_slots.append(sdiff)

            # check minimum requirement
            avail_slots = [s for s in avail_slots
                           if s.mem > 0 and s.cpu > 0 and s.gpu > 0]

            # load-balance
            if avail_slots:
                inst_id = (max(avail_slots, key=lambda s: s.mem + s.cpu + s.gpu)).id
            else:
                raise InstanceNotAvailable

            # reserve slots
            query = (sa.update(agents)
                       .values(agents.c.used_mem_slots = agents.c.used_mem_slots + required_slot.mem,
                               agents.c.used_cpu_slots = agents.c.used_cpu_slots + required_slot.cpu,
                               agents.c.used_gpu_slots = agents.c.used_gpu_slots + required_slot.gpu)
                       .where(agents.c.id == inst_id))  # noqa
            await conn.execute(query)

        # Create kernel by invoking the agent on the instance.
        stdin_port = None
        stdout_port = None

        async with self.dbpool.acquire() as conn, conn.transaction():

            async def revert_slot_reservation():
                query = (sa.update(agents)
                           .values(agents.c.used_mem_slots = agents.c.used_mem_slots - required_slot.mem,
                                   agents.c.used_cpu_slots = agents.c.used_cpu_slots - required_slot.cpu,
                                   agents.c.used_gpu_slots = agents.c.used_gpu_slots - required_slot.gpu)
                           .where(agents.c.id == inst_id))  # noqa
                await conn.execute(query)

            query = (sa.select([agents.c.addr]).where(agents.c.id == inst_id))
            agent_addr = await conn.fetchval(query)

            async with self.handle_kernel_exception('create_kernel', sess_id,
                                                    revert_slot_reservation):
                async with RPCContext(agent_addr, 10) as rpc:
                    kernel_info = await rpc.call.create_kernel(lang, limits, mounts)

                # TODO: handle stdin_port, stdout_port

                log.debug(f'create_kernel("{sess_id}") -> created on {inst_id}')
                kernel = {
                    'sess_id': sess_id,
                    'agent': inst_id,
                    'access_key': owner_access_key,
                    'lang': lang,
                    'agent_addr': agent_addr,
                    'container_id': kernel_info['container_id'],
                    'allocated_cores': kernel_info['allocated_cores'],
                }
                query = (kernels.insert().values(**kernel))
                await conn.execute(query)
                return kernel

    async def destroy_kernel(self, sess_id):
        log.debug(f"destroy_kernel({sess_id})")
        kernel = await self.get_kernel(sess_id, 'agent_addr')
        async with self.handle_kernel_exception('destroy_kernel', sess_id):
            await self.set_kernel_status(sess_id, KernlStatus.TERMINATING)
            async with RPCContext(kernel['agent_addr'], 10) as rpc:
                await rpc.call.destroy_kernel(kernel['id'])

    async def restart_kernel(self, sess_id):
        log.debug(f'restart_kernel({sess_id})')
        kernel = await self.get_kernel(sess_id, 'agent_addr')

        async with self.handle_kernel_exception('restart_kernel', sess_id):
            await self.set_kernel_status(sess_id, KernlStatus.RESTARTING)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                kernel_info = await rpc.call.restart_kernel(sess_id)
            # TODO: what if prev status was "building" or others?
            await self.set_kernel_status(sess_id, KernlStatus.RUNNING,
                                         stdin_port=kernel_info['stdin_port'],
                                         stdout_port=kernel_info['stdout_port'])

    async def execute(self, sess_id, api_version, mode, code, opts):
        log.debug(f'execute:v{api_version}({sess_id}, {mode}')
        async with self.handle_kernel_exception('execute', sess_id):
            async with RPCContext(kernel.addr, 200) as rpc:  # must be longer than kernel exec_timeout
                result = await rpc.call.execute(api_version, kernel.id,
                                                mode, code, opts)
                return result

    async def upload_file(self, sess_id, filename, filedata):
        log.debug(f'upload_file({sess_id}, {filename})')
        async with self.handle_kernel_exception('upload_file', sess_id):
            async with RPCContext(kernel.addr, 10000) as rpc:
                result = await rpc.call.upload_file(kernel.id, filename, filedata)
                return result

    async def update_kernel(self, sess_id, updated_fields):
        log.debug(f'update_kernel({sess_id})')
        async with self.redis_kern.get() as rk:
            await rk.hmset(kernel.id, *dict2kvlist(updated_fields))

    async def increment_kernel_usage(self, sess_id):
        log.debug(f'increment_kernel_usage({sess_id})')
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
