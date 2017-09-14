import asyncio
from contextlib import contextmanager
from datetime import datetime
from dateutil.tz import tzutc
import functools
import logging
import operator
import sys
from typing import Union
import uuid

import aiozmq, aiozmq.rpc
from aiozmq.rpc.base import GenericError, NotFoundError, ParametersError
import aiotools
from async_timeout import timeout as _timeout
import sqlalchemy as sa
import zmq

from sorna.common.utils import dict2kvlist
from ..gateway.exceptions import (SornaError,
    InstanceNotAvailable, InstanceNotFound, KernelNotFound,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from .models import agents, kernels, keypairs, ResourceSlot, AgentStatus, KernelStatus
from .structs import Instance, Kernel
from ..gateway.utils import catch_unexpected

__all__ = ['InstanceRegistry', 'InstanceNotFound']

log = logging.getLogger('sorna.manager.registry')


@aiotools.actxmgr
async def RPCContext(addr, timeout=10):
    preserved_exceptions = (
        NotFoundError,
        ParametersError,
        asyncio.TimeoutError,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    )
    server = None
    try:
        server = await aiozmq.rpc.connect_rpc(
            connect=addr, error_table={
                'concurrent.futures._base.TimeoutError': asyncio.TimeoutError,
            })
        server.transport.setsockopt(zmq.LINGER, 50)
        with _timeout(timeout):
            yield server
    except:
        exc_type, exc, tb = sys.exc_info()
        if issubclass(exc_type, GenericError):
            e = AgentError(exc.args[0], exc.args[1])
            raise e.with_traceback(tb)
        elif issubclass(exc_type, preserved_exceptions):
            raise
        else:
            e = AgentError(exc_type, exc.args)
            raise e.with_traceback(tb)
    finally:
        if server:
            server.close()


@aiotools.actxmgr
async def reenter_txn(pool, conn):
    if conn is None:
        async with pool.acquire() as conn, conn.begin():
            yield conn
    else:
        yield conn


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
        async with self.dbpool.acquire() as conn:
            query = (sa.select(['id', field] if field else None)
                       .select_from(agents)
                       .where(agents.c.id == inst_id))
            result = await conn.execute(query)
            row = await result.first()
            if not row:
                raise InstanceNotFound(inst_id)
            return row

    async def enumerate_instances(self, check_shadow=True):
        async with self.dbpool.acquire() as conn:
            query = (sa.select_from(agents))
            async for row in conn.execute(query):
                yield row

    async def update_instance(self, inst_id, updated_fields):
        async with self.dbpool.acquire() as conn:
            query = (sa.update(agents)
                       .values(**updated_fields)
                       .where(agents.c.id == inst_id))
            await conn.execute(query)

    @aiotools.actxmgr
    async def handle_kernel_exception(self, op, sess_id,
                                error_callback=None,
                                cancellation_callback=None,
                                set_error=False):
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
            if set_error:
                await self.set_kernel_status(sess_id, KernelStatus.ERROR,
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
            if set_error:
                await self.set_kernel_status(sess_id, KernelStatus.ERROR,
                                             status_info='Agent error')
            if error_callback:
                await error_callback()
            raise exc_class('FAILURE', e)
        except SornaError:
            # silently re-raise to make them handled by gateway http handlers
            raise
        except:
            log.exception(f'{op}: other error')
            # TODO: raven.captureException()
            if set_error:
                await self.set_kernel_status(sess_id, KernelStatus.ERROR,
                                             status_info='Other error')
            if error_callback:
                await error_callback()
            raise

    async def get_kernel(self, kern_id: uuid.UUID, field=None, allow_stale=False):
        '''
        Retreive the kernel information from the given kernel ID.
        This ID is unique for all individual agent-spawned containers.

        If ``field`` is given, it extracts only the raw value of the given field, without
        wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel owner instance.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.access_key]
        if isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with self.dbpool.acquire() as conn:
            if allow_stale:
                query = (sa.select(cols)
                           .select_from(kernels)
                           .where(kernels.c.id == kern_id)
                           .limit(1).offset(0))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.id == kern_id) &
                                  (kernels.c.status.in_([KernelStatus.BUILDING,
                                                         KernelStatus.RUNNING])) &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent))
                           .limit(1).offset(0))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise KernelNotFound
            return row

    async def get_kernel_session(self, sess_id: str, field=None, allow_stale=False):
        '''
        Retreive the kernel information from the session ID (client-side session token).
        If the kernel is composed of multiple containers, it returns the address of the master
        container.

        If ``field`` is given, it extracts only the raw value of the given field, without
        wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel owner instance.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.access_key]
        if isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with self.dbpool.acquire() as conn:
            if allow_stale:
                query = (sa.select(cols)
                           .select_from(kernels)
                           .where((kernels.c.sess_id == sess_id) &
                                  (kernels.c.role == 'master'))
                           .limit(1).offset(0))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.sess_id == sess_id) &
                                  (kernels.c.role == 'master') &
                                  (kernels.c.status.in_([KernelStatus.BUILDING,
                                                         KernelStatus.RUNNING])) &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent))
                           .limit(1).offset(0))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise KernelNotFound
            return row

    async def get_kernels(self, kern_ids, field=None, allow_stale=False):
        '''
        Batched version of :meth:`get_kernel() <InstanceRegistry.get_kernel>`.
        The order of the returend array is same to the order of ``kern_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising KernelNotFound exception.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.access_key]
        if isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with self.dbpool.acquire() as conn:
            if allow_stale:
                query = (sa.select(cols)
                           .select_from(kernels)
                           .where((kernels.c.sess_id.in_(kern_ids)) &
                                  (kernels.c.role == 'master')))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.sess_id.in_(kern_ids)) &
                                  (kernels.c.role == 'master') &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent)))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return rows

    async def set_kernel_status(self, sess_id, status, db_connection=None, **extra_fields):
        data = {
            'status': status,
        }
        data.update(extra_fields)
        async with reenter_txn(self.dbpool, db_connection) as conn:
            query = (sa.update(kernels)
                       .values(data)
                       .where(kernels.c.sess_id == sess_id))
            await conn.execute(query)

    @catch_unexpected(log)
    async def get_or_create_kernel(self, client_sess_token, lang, owner_access_key,
                                   limits=None, mounts=None, conn=None):
        assert owner_access_key
        try:
            kern = await self.get_kernel_session(client_sess_token)
            created = False
        except KernelNotFound:
            kern = await self.create_kernel(client_sess_token, lang, owner_access_key,
                                            limits=limits, mounts=mounts, conn=conn)
            created = True
        assert kern is not None
        return kern, created

    def get_kernel_slot(self, lang):
        # TODO: implement
        return ResourceSlot(
            None, 1, 1, 0
        )

    async def create_kernel(self, sess_id, lang, owner_access_key, limits=None, mounts=None, conn=None):
        agent_id = None
        limits = limits or {}
        mounts = mounts or []
        required_slot = self.get_kernel_slot(lang)
        created_info = None

        async with reenter_txn(self.dbpool, conn) as conn:

            # scan available slots from alive agents
            avail_slots = []
            query = (sa.select([agents], for_update=True)
                       .where(agents.c.status == AgentStatus.ALIVE))
            async for row in conn.execute(query):
                sdiff = ResourceSlot(
                    row['id'],
                    row['mem_slots'] - row['used_mem_slots'],
                    row['cpu_slots'] - row['used_cpu_slots'],
                    row['gpu_slots'] - row['used_gpu_slots'],
                )
                avail_slots.append(sdiff)

            # check minimum requirement
            avail_slots = [s for s in avail_slots
                           if s.mem >= required_slot.mem and
                              s.cpu >= required_slot.cpu and
                              s.gpu >= required_slot.gpu]

            # load-balance
            if avail_slots:
                agent_id = (max(avail_slots, key=lambda s: s.mem + s.cpu + s.gpu)).id
            else:
                raise InstanceNotAvailable

            # reserve slots
            mem_col = agents.c.used_mem_slots
            cpu_col = agents.c.used_cpu_slots
            gpu_col = agents.c.used_gpu_slots
            query = (sa.update(agents)
                       .values({
                           'used_mem_slots': mem_col + required_slot.mem,
                           'used_cpu_slots': cpu_col + required_slot.cpu,
                           'used_gpu_slots': gpu_col + required_slot.gpu,
                       })
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            assert result.rowcount == 1

            # Create kernel by invoking the agent on the instance.
            query = (sa.select([agents.c.addr])
                       .where(agents.c.id == agent_id))
            agent_addr = await conn.scalar(query)
            assert agent_addr is not None

            # Prepare kernel.
            kernel_id = uuid.uuid4()
            query = kernels.insert().values({
                'id': kernel_id,
                'status': KernelStatus.PREPARING,
                'sess_id': sess_id,
                'role': 'master',
                'agent': agent_id,
                'agent_addr': agent_addr,
                'access_key': owner_access_key,
                'lang': lang,
                'mem_slot': required_slot.mem,
                'cpu_slot': required_slot.cpu,
                'gpu_slot': required_slot.gpu,
                'cpu_set': [],
                'gpu_set': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            result = await conn.execute(query)
            assert result.rowcount == 1

            async with self.handle_kernel_exception('create_kernel', sess_id):
                async with RPCContext(agent_addr, 3) as rpc:
                    config = {
                        'lang': lang,
                        # TODO: apply limits/mounts/vfolders
                    }
                    created_info = await rpc.call.create_kernel(str(kernel_id), config)
                if created_info is None:
                    raise KernelCreationFailed('ooops')
                log.debug(f'create_kernel("{sess_id}") -> '
                          f'created on {agent_id}\n{created_info!r}')
                assert str(kernel_id) == created_info['id']
                kernel_access_info = {
                    'id': kernel_id,
                    'sess_id': sess_id,
                    'agent': agent_id,
                    'agent_addr': agent_addr,
                }
                query = (kernels.update()
                                .values({
                                    'status': KernelStatus.RUNNING,
                                    'container_id': created_info['container_id'],
                                    'cpu_set': list(created_info['cpu_set']),
                                    'gpu_set': list(created_info['gpu_set']),
                                    'repl_in_port': created_info['repl_in_port'],
                                    'repl_out_port': created_info['repl_out_port'],
                                    'stdin_port': created_info['stdin_port'],
                                    'stdout_port': created_info['stdout_port'],
                                })
                                .where(kernels.c.id == kernel_id))
                result = await conn.execute(query)
                assert result.rowcount == 1
                return kernel_access_info


    async def destroy_kernel(self, sess_id):
        log.debug(f"destroy_kernel({sess_id})")
        async with self.handle_kernel_exception('destroy_kernel', sess_id,
                                                set_error=True):
            kernel = await self.get_kernel_session(sess_id)
            await self.set_kernel_status(sess_id, KernelStatus.TERMINATING)
            async with RPCContext(kernel['agent_addr'], 10) as rpc:
                await rpc.call.destroy_kernel(str(kernel['id']))

    async def restart_kernel(self, sess_id):
        log.debug(f'restart_kernel({sess_id})')
        async with self.handle_kernel_exception('restart_kernel', sess_id,
                                                set_error=True):
            extra_cols = (kernels.c.lang, kernels.c.cpu_set, kernels.c.gpu_set)
            kernel = await self.get_kernel_session(sess_id, extra_cols)
            await self.set_kernel_status(sess_id, KernelStatus.RESTARTING)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                prev_config = {
                    'lang': kernel['lang'],
                    'cpu_set': kernel['cpu_set'],
                    'gpu_set': kernel['gpu_set'],
                }
                kernel_info = await rpc.call.restart_kernel(str(kernel['id']), prev_config)
            # TODO: what if prev status was "building" or others?
            await self.set_kernel_status(sess_id, KernelStatus.RUNNING,
                                         container_id=kernel_info['container_id'],
                                         cpu_set=list(kernel_info['cpu_set']),
                                         gpu_set=list(kernel_info['gpu_set']),
                                         repl_in_port=kernel_info['repl_in_port'],
                                         repl_out_port=kernel_info['repl_out_port'],
                                         stdin_port=kernel_info['stdin_port'],
                                         stdout_port=kernel_info['stdout_port'])

    async def execute(self, sess_id, api_version, mode, code, opts):
        log.debug(f'execute:v{api_version}({sess_id}, {mode})')
        async with self.handle_kernel_exception('execute', sess_id):
            kernel = await self.get_kernel_session(sess_id)
            # must be longer than kernel exec_timeout
            # TODO: cope with agent restarts with save-loaded runner states
            # TODO: cope with changes of container public ports after agent restarts
            async with RPCContext(kernel['agent_addr'], 200) as rpc:
                exec_coro = rpc.call.execute(api_version, str(kernel['id']),
                                             mode, code, opts)
                if exec_coro is None:
                    raise RuntimeError('execute cancelled')
                try:
                    return await exec_coro
                except TypeError as e:
                    log.exception('typeerror????')

    async def upload_file(self, sess_id, filename, filedata):
        log.debug(f'upload_file({sess_id}, {filename})')
        async with self.handle_kernel_exception('upload_file', sess_id):
            kernel = await self.get_kernel_session(sess_id)
            async with RPCContext(kernel['agent_addr'], 10000) as rpc:
                result = await rpc.call.upload_file(str(kernel['id']), filename, filedata)
                return result

    async def update_kernel(self, sess_id, updated_fields):
        log.debug(f'update_kernel({sess_id})')
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.update(kernels)
                       .values(updated_fields)
                       .where((kernels.c.sess_id == sess_id) &
                              (kernels.c.role == 'master')))
            await conn.execute(query)

    async def increment_session_usage(self, sess_id, conn=None):
        log.debug(f'increment_session_usage({sess_id})')
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.update(kernels)
                       .values(num_queries=kernels.c.num_queries + 1)
                       .where((kernels.c.sess_id == sess_id) &
                              (kernels.c.role == 'master')))
            await conn.execute(query)

    async def get_kernels_in_instance(self, inst_id):
        async with self.dbpool.acquire() as conn:
            query = (sa.select([kernels.c.sess_id])
                       .select_from(kernels)
                       .where(kernels.c.agent == inst_id))
            result = await conn.execute(query)
            rows = await result.fetchall()
            if not rows:
                return tuple()
            return rows

    async def handle_stats(self, inst_id, kern_stats, interval):
        pass
        #async with self.lifecycle_lock, \
        #           self.redis_inst.get() as ri, \
        #           self.redis_kern.get() as rk:  # noqa

        #    rk_pipe = rk.pipeline()
        #    for kern_id in kern_stats.keys():
        #        rk_pipe.exists(kern_id)
        #    kernel_existence = await rk_pipe.execute()

        #    ri_pipe = ri.pipeline()
        #    rk_pipe = rk.pipeline()
        #    for (kern_id, stats), alive in zip(kern_stats.items(), kernel_existence):
        #        if alive:
        #            ri_pipe.sadd(f'{inst_id}.kernels', kern_id)
        #            rk_pipe.hmset(kern_id, *dict2kvlist(stats))
        #    await ri_pipe.execute()
        #    await rk_pipe.execute()

    async def handle_heartbeat(self, agent_id, agent_info):
        async with self.dbpool.acquire() as conn:
            # TODO: check why sa.column('status') does not work
            query = (sa.select([agents.c.status], for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            prev_status = await conn.scalar(query)
            if prev_status is None:
                # new agent detected!
                log.info(f'agent {agent_id} joined!')
                query = agents.insert().values({
                    'id': agent_id,
                    'status': AgentStatus.ALIVE,
                    'mem_slots': agent_info['mem_slots'],
                    'cpu_slots': agent_info['cpu_slots'],
                    'gpu_slots': agent_info['gpu_slots'],
                    'used_mem_slots': 0,
                    'used_cpu_slots': 0,
                    'used_gpu_slots': 0,
                    'addr': agent_info['addr'],
                    'first_contact': datetime.now(tzutc()),
                    'lost_at': None,
                })
                result = await conn.execute(query)
                assert result.rowcount == 1
            elif prev_status == AgentStatus.ALIVE:
                pass
            elif prev_status in (AgentStatus.LOST, AgentStatus.TERMINATED):
                log.warning(f'agent {agent_id} revived!')
                query = (sa.update(agents)
                           .values({
                               'status': AgentStatus.ALIVE,
                               'lost_at': None,
                           })
                           .where(agents.c.id == agent_id))
                await conn.execute(query)
            else:
                log.error(f'should not reach here! {type(prev_status)}')

    async def mark_agent_terminated(self, agent_id, status, conn=None):
        # TODO: interpret kern_id to sess_id
        #for kern_id in (await app['registry'].get_kernels_in_instance(agent_id)):
        #    for handler in app['stream_pty_handlers'][kern_id].copy():
        #        handler.cancel()
        #        await handler
        # TODO: define behavior when agent reuse running instances upon revive
        #await app['registry'].forget_all_kernels_in_instance(agent_id)
        async with reenter_txn(self.dbpool, conn) as conn:

            query = (sa.select([agents.c.status])
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            prev_status = await result.scalar()
            if prev_status in (None, AgentStatus.LOST, AgentStatus.TERMINATED):
                return

            if status == AgentStatus.LOST:
                log.warning(f'agent {agent_id} heartbeat timeout detected.')
            elif status == AgentStatus.TERMINATED:
                log.warning(f'agent {agent_id} has terminated.')
            query = (sa.update(agents)
                       .values({
                           'status': status,
                           'lost_at': datetime.now(tzutc()),
                       })
                       .where(agents.c.id == agent_id))
            await conn.execute(query)

    async def mark_kernel_terminated(self, kernel_id, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            # check if already terminated
            query = (sa.select([kernels.c.status])
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            prev_status = await result.scalar()
            if prev_status in (None, KernelStatus.TERMINATED):
                return

            # change the status to TERMINATED
            # (we don't delete the row for later logging and billing)
            query = (sa.update(kernels)
                       .values({
                           'status': KernelStatus.TERMINATED,
                           'terminated_at': datetime.now(tzutc()),
                       })
                       .where(kernels.c.id == kernel_id))
            await conn.execute(query)

            # release resource slots
            query = (sa.select([sa.column('agent'),
                                sa.column('mem_slot'),
                                sa.column('cpu_slot'),
                                sa.column('gpu_slot')])
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            kernel = await result.first()
            if kernel is None:
                return
            mem_col = agents.c.used_mem_slots
            cpu_col = agents.c.used_cpu_slots
            gpu_col = agents.c.used_gpu_slots
            query = (sa.update(agents)
                       .values({
                           'used_mem_slots': mem_col - kernel['mem_slot'],
                           'used_cpu_slots': cpu_col - kernel['cpu_slot'],
                           'used_gpu_slots': gpu_col - kernel['gpu_slot'],
                       })
                       .where(agents.c.id == kernel['agent']))
            await conn.execute(query)

            '''
            if kern_stat:
                # if last stats available, use it.
                log.info(f'update_session_usage: {kern.id}, last-stat: {kern_stat}')
                values = {
                    'access_key_id': kern.access_key,
                    'kernel_type': kern.lang,
                    'kernel_id': kern.id,
                    'started_at': kern.created_at,
                    'terminated_at': datetime.now(tzutc()),
                    'cpu_used': kern_stat['cpu_used'],
                    'mem_used': kern_stat['mem_max_bytes'] // 1024,
                    'io_used': (kern_stat['io_read_bytes'] + kern_stat['io_write_bytes']) // 1024,
                    'net_used': (kern_stat['net_rx_bytes'] + kern_stat['net_tx_bytes']) // 1024,
                }
                query = usage.insert().values(**values)
            else:
                # otherwise, get the latest stats from the registry.
                log.info(f'update_session_usage: {kern.id}, registry-stat')
                values = {
                    'access_key_id': kern.access_key,
                    'kernel_type': kern.lang,
                    'kernel_id': kern.id,
                    'started_at': kern.created_at,
                    'terminated_at': datetime.now(tzutc()),
                    'cpu_used': kern.cpu_used,
                    'mem_used': kern.mem_max_bytes // 1024,
                    'io_used': (kern.io_read_bytes + kern.io_write_bytes) // 1024,
                    'net_used': (kern.net_rx_bytes + kern.net_tx_bytes) // 1024,
                }
                query = usage.insert().values(**values)
            await conn.execute(query)
            '''

    async def mark_session_terminated(self, sess_id):
        async with self.dbpool.acquire() as conn:

            # restore concurrency usage of the owner access-key
            query = (sa.select([sa.column('id'),
                                sa.column('access_key')])
                       .select_from(kernels)
                       .where(kernels.c.sess_id == sess_id))
            result = await conn.execute(query)
            all_kernels = await result.fetchall()
            num_kernels = len(all_kernels)
            if num_kernels > 0:
                access_key = all_kernels[0]['access_key']
                query = (sa.update(keypairs)
                           .values({
                               'concurrency_used': (keypairs.c.concurrency_used -
                                                    num_kernels),
                           })
                           .where(keypairs.c.access_key == access_key))
                await conn.execute(query)

    async def forget_instance(self, inst_id):
        async with self.dbpool.acquire() as conn:
            query = (sa.update(agents)
                       .values(status=AgentStatus.TERMINATED,
                               lost_at=datetime.now(tzutc()))
                       .where(agents.c.id == inst_id))
            await conn.execute(query)
