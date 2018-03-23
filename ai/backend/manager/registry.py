import asyncio
from datetime import datetime
import logging
import sys
import uuid

import aiozmq, aiozmq.rpc
from aiozmq.rpc.base import GenericError, NotFoundError, ParametersError
import aiotools
from async_timeout import timeout as _timeout
from dateutil.tz import tzutc
import snappy
import sqlalchemy as sa
from yarl import URL
import zmq

from ai.backend.common import msgpack
from ..gateway.exceptions import (
    BackendError,
    InstanceNotAvailable, InstanceNotFound,
    KernelNotFound, KernelAlreadyExists,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from .models import (
    agents, kernels, keypairs,
    ResourceSlot, AgentStatus, KernelStatus
)
from ..gateway.utils import Infinity

__all__ = ['AgentRegistry', 'InstanceNotFound']

log = logging.getLogger('ai.backend.manager.registry')


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
    except Exception:
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
            await server.wait_closed()


@aiotools.actxmgr
async def reenter_txn(pool, conn):
    if conn is None:
        async with pool.acquire() as conn, conn.begin():
            yield conn
    else:
        async with conn.begin_nested():
            yield conn


class AgentRegistry:
    '''
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    '''

    def __init__(self, config_server, dbpool,
                 redis_stat, redis_live, redis_image,
                 loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.config_server = config_server
        self.dbpool = dbpool
        self.redis_stat = redis_stat
        self.redis_live = redis_live
        self.redis_image = redis_image

    async def init(self):
        pass

    async def shutdown(self):
        pass

    async def get_instance(self, inst_id, field=None):
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (sa.select(['id', field] if field else None)
                       .select_from(agents)
                       .where(agents.c.id == inst_id))
            result = await conn.execute(query)
            row = await result.first()
            if not row:
                raise InstanceNotFound(inst_id)
            return row

    async def enumerate_instances(self, check_shadow=True):
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (sa.select('*').select_from(agents))
            if check_shadow:
                query = query.where(agents.c.status == AgentStatus.ALIVE)
            async for row in conn.execute(query):
                yield row

    async def update_instance(self, inst_id, updated_fields):
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (sa.update(agents)
                       .values(**updated_fields)
                       .where(agents.c.id == inst_id))
            await conn.execute(query)

    @aiotools.actxmgr
    async def handle_kernel_exception(
            self, op, sess_id, access_key,
            error_callback=None,
            cancellation_callback=None,
            set_error=False):
        op_exc = {
            'create_session': KernelCreationFailed,
            'restart_session': KernelRestartFailed,
            'destroy_session': KernelDestructionFailed,
            'execute': KernelExecutionFailed,
            'upload_file': KernelExecutionFailed,
            'get_logs': KernelExecutionFailed,
        }
        exc_class = op_exc[op]
        try:
            yield
        except asyncio.TimeoutError:
            if set_error:
                await self.set_session_status(
                    sess_id, access_key, KernelStatus.ERROR,
                    status_info=f'Operation timeout ({op})')
            if error_callback:
                await error_callback()
            raise exc_class('TIMEOUT')
        except asyncio.CancelledError:
            if cancellation_callback:
                await cancellation_callback()
            raise
        except AgentError as e:
            if not issubclass(e.args[0], AssertionError):
                log.exception(f'{op}: agent-side error')
            # TODO: wrap some assertion errors as "invalid requests"
            if set_error:
                await self.set_session_status(sess_id, access_key,
                                              KernelStatus.ERROR,
                                              status_info='Agent error')
            if error_callback:
                await error_callback()
            raise exc_class('FAILURE', e)
        except BackendError:
            # silently re-raise to make them handled by gateway http handlers
            raise
        except:
            log.exception(f'{op}: other error')
            # TODO: raven.captureException()
            if set_error:
                await self.set_session_status(sess_id, access_key,
                                              KernelStatus.ERROR,
                                              status_info='Other error')
            if error_callback:
                await error_callback()
            raise

    async def get_kernel(self, kern_id: uuid.UUID, field=None, allow_stale=False,
                         db_connection=None):
        '''
        Retreive the kernel information from the given kernel ID.
        This ID is unique for all individual agent-spawned containers.

        If ``field`` is given, it extracts only the raw value of the given
        field, without wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel
        owner instance.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key]
        if field == '*':
            cols = '*'
        elif isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.dbpool, db_connection) as conn:
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

    async def get_session(self, sess_id: str, access_key: str, *,
                          field=None,
                          allow_stale=False,
                          for_update=False,
                          db_connection=None):
        '''
        Retreive the kernel information from the session ID (client-side
        session token).  If the kernel is composed of multiple containers, it
        returns the address of the master container.

        If ``field`` is given, it extracts only the raw value of the given
        field, without wrapping it as Kernel object.  If ``allow_stale`` is
        true, it skips checking validity of the kernel owner instance.
        '''

        cols = [kernels.c.id, kernels.c.sess_id, kernels.c.access_key,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.lang]
        if field == '*':
            cols = '*'
        elif isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.dbpool, db_connection) as conn:
            if allow_stale:
                query = (sa.select(cols, for_update=for_update)
                           .select_from(kernels)
                           .where((kernels.c.sess_id == sess_id) &
                                  (kernels.c.access_key == access_key) &
                                  (kernels.c.role == 'master'))
                           .limit(1).offset(0))
            else:
                query = (sa.select(cols, for_update=for_update)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.sess_id == sess_id) &
                                  (kernels.c.access_key == access_key) &
                                  (kernels.c.role == 'master') &
                                  (kernels.c.status != KernelStatus.TERMINATED) &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent))
                           .limit(1).offset(0))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise KernelNotFound
            return row

    async def get_sessions(self, sess_ids, field=None, allow_stale=False,
                           db_connection=None):
        '''
        Batched version of :meth:`get_session() <AgentRegistry.get_session>`.
        The order of the returend array is same to the order of ``sess_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising KernelNotFound exception.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key]
        if isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.dbpool, db_connection) as conn:
            if allow_stale:
                query = (sa.select(cols)
                           .select_from(kernels)
                           .where((kernels.c.sess_id.in_(sess_ids)) &
                                  (kernels.c.role == 'master')))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.sess_id.in_(sess_ids)) &
                                  (kernels.c.role == 'master') &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent)))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return rows

    async def set_session_status(self, sess_id, access_key, status,
                                 db_connection=None, **extra_fields):
        data = {
            'status': status,
        }
        data.update(extra_fields)
        async with reenter_txn(self.dbpool, db_connection) as conn:
            query = (sa.update(kernels)
                       .values(data)
                       .where((kernels.c.sess_id == sess_id) &
                              # TODO: include slave workers?
                              (kernels.c.status != KernelStatus.TERMINATED) &
                              (kernels.c.access_key == access_key)))
            await conn.execute(query)

    async def get_or_create_session(self, sess_id, access_key,
                                    lang, creation_config,
                                    conn=None):
        try:
            kern = await self.get_session(sess_id, access_key)
            canonical_lang = await self.config_server.resolve_image_name(lang)
            kernel_lang = tuple(kern.lang.split(':'))
            if canonical_lang != kernel_lang:
                raise KernelAlreadyExists
            created = False
        except KernelNotFound:
            kern = await self.create_session(
                sess_id, access_key,
                lang, creation_config,
                conn=conn)
            created = True
        assert kern is not None
        return kern, created

    async def create_session(self, sess_id, access_key,
                             lang, creation_config,
                             conn=None):
        agent_id = None
        created_info = None
        mounts = creation_config.get('mounts') or []
        environ = creation_config.get('environ') or {}

        name, tag = await self.config_server.resolve_image_name(lang)
        required_slot = await self.config_server.get_image_required_slots(name, tag)
        required_slot = ResourceSlot(
            id=None,
            mem=min(
                required_slot.mem,
                int(creation_config.get('instanceMemory') or Infinity),
            ),
            cpu=min(
                required_slot.cpu,
                float(creation_config.get('instanceCores') or Infinity),
            ),
            gpu=min(
                required_slot.gpu,
                float(creation_config.get('instanceGPUs') or Infinity),
            ),
        )
        lang = f'{name}:{tag}'
        runnable_agents = frozenset(await self.redis_image.smembers(lang))

        async with reenter_txn(self.dbpool, conn) as conn:

            # scan available slots from alive agents
            avail_slots = []
            query = (sa.select([agents], for_update=True)
                       .where(agents.c.status == AgentStatus.ALIVE))

            async for row in conn.execute(query):
                if row['id'] not in runnable_agents:
                    continue
                sdiff = ResourceSlot(
                    id=row['id'],
                    mem=row['mem_slots'] - row['used_mem_slots'],
                    cpu=row['cpu_slots'] - row['used_cpu_slots'],
                    gpu=row['gpu_slots'] - row['used_gpu_slots'],
                )
                avail_slots.append(sdiff)

            # check minimum requirement
            avail_slots = [s for s in avail_slots
                           if s.mem >= required_slot.mem and
                              s.cpu >= required_slot.cpu and
                              s.gpu >= required_slot.gpu]

            # load-balance
            if avail_slots:
                agent_id = (max(avail_slots, key=lambda s: (s.gpu, s.cpu, s.mem))).id
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
                'access_key': access_key,
                'lang': lang,
                'mem_slot': required_slot.mem,
                'cpu_slot': required_slot.cpu,
                'gpu_slot': required_slot.gpu,
                'environ': [f'{k}={v}' for k, v in environ.items()],
                'cpu_set': [],
                'gpu_set': [],
                'kernel_host': None,
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            result = await conn.execute(query)
            assert result.rowcount == 1

            async with self.handle_kernel_exception(
                    'create_session', sess_id, access_key):
                async with RPCContext(agent_addr, 3) as rpc:
                    config = {
                        'lang': lang,
                        'limits': {
                            'mem_slot': required_slot.mem,
                            'cpu_slot': required_slot.cpu,
                            'gpu_slot': required_slot.gpu,
                        },
                        'mounts': mounts,
                        'environ': environ,
                    }
                    created_info = await rpc.call.create_kernel(str(kernel_id),
                                                                config)
                if created_info is None:
                    raise KernelCreationFailed('ooops')
                log.debug(f'create_session("{sess_id}", "{access_key}") -> '
                          f'created on {agent_id}\n{created_info!r}')
                assert str(kernel_id) == created_info['id']
                agent_host = URL(agent_addr).host
                kernel_host = created_info.get('kernel_host', agent_host)
                kernel_access_info = {
                    'id': kernel_id,
                    'sess_id': sess_id,
                    'agent': agent_id,
                    'agent_addr': agent_addr,
                    'kernel_host': kernel_host,
                }
                query = (kernels.update()
                                .values({
                                    'status': KernelStatus.RUNNING,
                                    'container_id': created_info['container_id'],
                                    'cpu_set': list(created_info['cpu_set']),
                                    'gpu_set': list(created_info['gpu_set']),
                                    'kernel_host': kernel_host,
                                    'repl_in_port': created_info['repl_in_port'],
                                    'repl_out_port': created_info['repl_out_port'],
                                    'stdin_port': created_info['stdin_port'],
                                    'stdout_port': created_info['stdout_port'],
                                })
                                .where(kernels.c.id == kernel_id))
                result = await conn.execute(query)
                assert result.rowcount == 1
                return kernel_access_info

    async def destroy_session(self, sess_id, access_key):
        async with self.handle_kernel_exception(
                'destroy_session', sess_id, access_key, set_error=True):
            try:
                async with self.dbpool.acquire() as conn, conn.begin():
                    kernel = await self.get_session(sess_id, access_key,
                                                    for_update=True,
                                                    db_connection=conn)
                    await self.set_session_status(sess_id, access_key,
                                                  KernelStatus.TERMINATING,
                                                  db_connection=conn)
            except KernelNotFound:
                return
            async with RPCContext(kernel['agent_addr'], 10) as rpc:
                return await rpc.call.destroy_kernel(str(kernel['id']))

    async def restart_session(self, sess_id, access_key):
        async with self.handle_kernel_exception(
                'restart_session', sess_id, access_key, set_error=True):
            extra_cols = (
                kernels.c.lang,
                kernels.c.mem_slot,
                kernels.c.cpu_slot,
                kernels.c.gpu_slot,
                kernels.c.environ,
                kernels.c.cpu_set,
                kernels.c.gpu_set,
            )
            async with self.dbpool.acquire() as conn, conn.begin():
                kernel = await self.get_session(sess_id, access_key,
                                                field=extra_cols,
                                                for_update=True,
                                                db_connection=conn)
                await self.set_session_status(sess_id, access_key,
                                              KernelStatus.RESTARTING,
                                              db_connection=conn)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                # TODO: read from vfolders attachment table
                mounts = []
                limits = {
                    'cpu_slot': kernel['cpu_slot'],
                    'gpu_slot': kernel['gpu_slot'],
                    'mem_slot': kernel['mem_slot'],
                }
                environ = {
                     k: v for k, v in
                     map(lambda s: s.split('=', 1), kernel['environ'])
                }
                new_config = {
                    'lang': kernel['lang'],
                    'mounts': mounts,
                    'limits': limits,
                    'environ': environ,
                    'cpu_set': kernel['cpu_set'],
                    'gpu_set': kernel['gpu_set'],
                }
                kernel_info = await rpc.call.restart_kernel(str(kernel['id']),
                                                            new_config)
                # TODO: what if prev status was "building" or others?
                await self.set_session_status(
                    sess_id, access_key,
                    KernelStatus.RUNNING,
                    container_id=kernel_info['container_id'],
                    cpu_set=list(kernel_info['cpu_set']),
                    gpu_set=list(kernel_info['gpu_set']),
                    repl_in_port=kernel_info['repl_in_port'],
                    repl_out_port=kernel_info['repl_out_port'],
                    stdin_port=kernel_info['stdin_port'],
                    stdout_port=kernel_info['stdout_port'],
                )

    async def execute(self, sess_id, access_key,
                      api_version, run_id, mode, code, opts):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            # The agent aggregates at most 2 seconds of outputs
            # if the kernel runs for a long time.
            async with RPCContext(kernel['agent_addr'], 300) as rpc:
                coro = rpc.call.execute(api_version, str(kernel['id']),
                                        run_id, mode, code, opts)
                if coro is None:
                    log.warning('execute cancelled')
                    return None
                return await coro

    async def interrupt_session(self, sess_id, access_key):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 5) as rpc:
                coro = rpc.call.interrupt_kernel(str(kernel['id']))
                if coro is None:
                    log.warning('interrupt cancelled')
                    return None
                return await coro

    async def get_completions(self, sess_id, access_key, mode, text, opts):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 5) as rpc:
                coro = rpc.call.get_completions(str(kernel['id']), mode, text, opts)
                if coro is None:
                    log.warning('get_completions cancelled')
                    return None
                return await coro

    async def upload_file(self, sess_id, access_key, filename, payload):
        async with self.handle_kernel_exception('upload_file', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 180) as rpc:
                coro = rpc.call.upload_file(str(kernel['id']), filename, payload)
                if coro is None:
                    log.warning('upload_file cancelled')
                    return None
                return await coro

    async def get_logs(self, sess_id, access_key):
        async with self.handle_kernel_exception('get_logs', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 5) as rpc:
                coro = rpc.call.get_logs(str(kernel['id']))
                if coro is None:
                    log.warning('get_logs cancelled')
                    return None
                return await coro

    async def update_session(self, sess_id, access_key, updated_fields, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.update(kernels)
                       .values(updated_fields)
                       .where((kernels.c.sess_id == sess_id) &
                              (kernels.c.access_key == access_key) &
                              (kernels.c.status != KernelStatus.TERMINATED) &
                              (kernels.c.role == 'master')))
            await conn.execute(query)

    async def increment_session_usage(self, sess_id, access_key, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.update(kernels)
                       .values(num_queries=kernels.c.num_queries + 1)
                       .where((kernels.c.sess_id == sess_id) &
                              (kernels.c.access_key == access_key) &
                              (kernels.c.status != KernelStatus.TERMINATED) &
                              (kernels.c.role == 'master')))
            await conn.execute(query)

    async def get_sessions_in_instance(self, inst_id, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.select([kernels.c.sess_id])
                       .select_from(kernels)
                       .where((kernels.c.agent == inst_id) &
                              (kernels.c.role == 'master') &
                              (kernels.c.status != KernelStatus.TERMINATED)))
            result = await conn.execute(query)
            rows = await result.fetchall()
            if not rows:
                return tuple()
            return rows

    async def handle_heartbeat(self, agent_id, agent_info):

        now = datetime.now(tzutc())

        # Update "last seen" timestamp for liveness tracking
        await self.redis_live.hset('last_seen', agent_id, now.timestamp())

        # Check and update status of the agent record in DB
        async with self.dbpool.acquire() as conn, conn.begin():
            # TODO: check why sa.column('status') does not work
            query = (sa.select([agents.c.status,
                                agents.c.mem_slots,
                                agents.c.cpu_slots,
                                agents.c.gpu_slots],
                               for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            row = await result.first()
            ob_factors = await self.config_server.get_overbook_factors()
            reported_mem_slots = int(agent_info['mem_slots'] * ob_factors['mem'])
            reported_cpu_slots = int(agent_info['cpu_slots'] * ob_factors['cpu'])
            reported_gpu_slots = int(agent_info['gpu_slots'] * ob_factors['gpu'])
            if row is None or row.status is None:
                # new agent detected!
                log.info(f'agent {agent_id} joined!')
                query = agents.insert().values({
                    'id': agent_id,
                    'status': AgentStatus.ALIVE,
                    'region': agent_info['region'],
                    'mem_slots': reported_mem_slots,
                    'cpu_slots': reported_cpu_slots,
                    'gpu_slots': reported_gpu_slots,
                    'used_mem_slots': 0,
                    'used_cpu_slots': 0,
                    'used_gpu_slots': 0,
                    'addr': agent_info['addr'],
                    'first_contact': now,
                    'lost_at': None,
                })
                result = await conn.execute(query)
                assert result.rowcount == 1
            elif row.status == AgentStatus.ALIVE:
                changed_cols = {}
                if row.mem_slots != reported_mem_slots:
                    changed_cols['mem_slots'] = reported_mem_slots
                if row.cpu_slots != reported_cpu_slots:
                    changed_cols['cpu_slots'] = reported_cpu_slots
                if row.gpu_slots != reported_gpu_slots:
                    changed_cols['gpu_slots'] = reported_gpu_slots
                if changed_cols:
                    query = (sa.update(agents)
                               .values(changed_cols)
                               .where(agents.c.id == agent_id))
                    await conn.execute(query)
            elif row.status in (AgentStatus.LOST, AgentStatus.TERMINATED):
                log.warning(f'agent {agent_id} revived!')
                query = (sa.update(agents)
                           .values({
                               'status': AgentStatus.ALIVE,
                               'lost_at': None,
                               'mem_slots': reported_mem_slots,
                               'cpu_slots': reported_cpu_slots,
                               'gpu_slots': reported_gpu_slots,
                           })
                           .where(agents.c.id == agent_id))
                await conn.execute(query)
            else:
                log.error(f'should not reach here! {type(row.status)}')

        # Update the mapping of kernel images to agents.
        images = msgpack.unpackb(snappy.decompress(agent_info['images']))
        pipe = self.redis_image.pipeline()
        for image in images:
            pipe.sadd(image[0], agent_id)
        await pipe.execute()

    async def mark_agent_terminated(self, agent_id, status, conn=None):
        # TODO: interpret kern_id to sess_id
        # for kern_id in (await app['registry'].get_kernels_in_instance(agent_id)):
        #     for handler in app['stream_pty_handlers'][kern_id].copy():
        #         handler.cancel()
        #         await handler
        #  TODO: define behavior when agent reuse running instances upon revive
        # await app['registry'].forget_all_kernels_in_instance(agent_id)

        await self.redis_live.hdel('last_seen', agent_id)

        pipe = self.redis_image.pipeline()
        async for imgname in self.redis_image.iscan():
            pipe.srem(imgname, agent_id)
        await pipe.execute()

        async with reenter_txn(self.dbpool, conn) as conn:

            query = (sa.select([agents.c.status], for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            prev_status = await result.scalar()
            if prev_status in (None, AgentStatus.LOST, AgentStatus.TERMINATED):
                return

            if status == AgentStatus.LOST:
                log.warning(f'agent {agent_id} heartbeat timeout detected.')
            elif status == AgentStatus.TERMINATED:
                log.info(f'agent {agent_id} has terminated.')
            query = (sa.update(agents)
                       .values({
                           'status': status,
                           'lost_at': datetime.now(tzutc()),
                       })
                       .where(agents.c.id == agent_id))
            await conn.execute(query)

    async def mark_kernel_terminated(self, kernel_id, conn=None):
        '''
        Mark the kernel (individual worker) terminated and release
        the resource slots occupied by it.
        '''
        async with reenter_txn(self.dbpool, conn) as conn:
            # check if already terminated
            query = (sa.select([kernels.c.status], for_update=True)
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            prev_status = await result.scalar()
            if prev_status in (None, KernelStatus.TERMINATED):
                return

            # change the status to TERMINATED
            # (we don't delete the row for later logging and billing)
            kern_data = {
                'status': KernelStatus.TERMINATED,
                'terminated_at': datetime.now(tzutc()),
            }
            kern_stat = await self.redis_stat.hgetall(kernel_id)
            if kern_stat is not None and 'cpu_used' in kern_stat:
                kern_data.update({
                    'cpu_used': int(float(kern_stat['cpu_used'])),
                    'mem_max_bytes': int(kern_stat['mem_max_bytes']),
                    'net_rx_bytes': int(kern_stat['net_rx_bytes']),
                    'net_tx_bytes': int(kern_stat['net_tx_bytes']),
                    'io_read_bytes': int(kern_stat['io_read_bytes']),
                    'io_write_bytes': int(kern_stat['io_write_bytes']),
                    'io_max_scratch_size': int(kern_stat['io_max_scratch_size']),
                })
            query = (sa.update(kernels)
                       .values(kern_data)
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

    async def mark_session_terminated(self, sess_id, access_key):
        '''
        Mark the compute session terminated and restore the concurrency limit
        of the owner access key.  Releasing resource limits is handled by
        func:`mark_kernel_terminated`.
        '''
        async with self.dbpool.acquire() as conn, conn.begin():
            # concurrency is per session.
            query = (sa.update(keypairs)
                       .values({
                           'concurrency_used': (
                               keypairs.c.concurrency_used - 1),
                       })
                       .where(keypairs.c.access_key == access_key))
            await conn.execute(query)

    async def forget_instance(self, inst_id):
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (sa.update(agents)
                       .values(status=AgentStatus.TERMINATED,
                               lost_at=datetime.now(tzutc()))
                       .where(agents.c.id == inst_id))
            await conn.execute(query)
