import asyncio
from decimal import Decimal
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
from ai.backend.common.logging import BraceStyleAdapter
from ..gateway.exceptions import (
    BackendError, InstanceNotFound,
    KernelNotFound, KernelAlreadyExists,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from ..gateway.utils import reenter_txn
from .models import (
    agents, kernels, keypairs,
    ResourceSlot, AgentStatus, KernelStatus,
    scaling_groups, ScalingGroup, SessionCreationRequest)

__all__ = ['AgentRegistry', 'InstanceNotFound']

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.registry'))


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
        elif issubclass(exc_type, TypeError):
            if exc.args[0] == "'NoneType' object is not iterable":
                log.warning('The agent has cancelled the operation '
                            'or the kernel has terminated too quickly.')
                # In this case, you may need to use "--debug-skip-container-deletion"
                # CLI option in the agent and check out the container logs via
                # "docker logs" command to see what actually happened.
            else:
                e = AgentError(exc_type, exc.args)
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

    async def get_scaling_group(self, agent_id=None, scaling_group=None, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            if agent_id:
                j = sa.join(scaling_groups, agents,
                            scaling_groups.c.name == agents.c.scaling_group,
                            isouter=True)
                query = (sa.select([scaling_groups.c.name])
                           .select_from(j)
                           .where(agents.c.id == agent_id))
            else:
                query = (sa.select([scaling_groups.c.name])
                           .select_from(scaling_groups)
                           .where(scaling_groups.c.name == scaling_group))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise ValueError('Scaling group not found')
            # TODO: consider columns of the row (e.g. which scaling driver to use)
            return ScalingGroup(row.name, self)

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
            'download_file': KernelExecutionFailed,
            'list_files': KernelExecutionFailed,
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
                log.exception('{0}: agent-side error', op)
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
            log.exception('{0}: other error', op)
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
                                    conn=None, tag=None, scaling_group=None):
        try:
            kern = await self.get_session(sess_id, access_key)
            canonical_lang = await self.config_server.resolve_image_name(lang)
            kernel_lang = tuple(kern.lang.split(':'))
            if canonical_lang != kernel_lang:
                raise KernelAlreadyExists
            created = False
        except KernelNotFound:
        #     kern = await self.create_session(
        #         sess_id, access_key,
        #         lang, creation_config,
        #         conn=conn, session_tag=tag, scaling_group=scaling_group)
        #     created = True
        # assert kern is not None
        # return kern, created
            kern = await self.enqueue_session(
                sess_id, access_key,
                lang, creation_config,
                conn=conn, session_tag=tag, scaling_group=scaling_group)
            created = True
        assert kern is not None
        return kern, created

    async def enqueue_session(self, sess_id, access_key,
                              lang, creation_config,
                              conn=None, session_tag=None, scaling_group=None):
        if scaling_group is None:
            scaling_group = 'default'
        scaling_group = await self.get_scaling_group(scaling_group=scaling_group,
                                                     conn=conn)

        # Parse input and create SessionCreateRequest.
        if '/' in lang:
            tokens = lang.split('/')
            docker_registry = '/'.join(tokens[:-1])
            lang = tokens[-1]
        else:
            docker_registry = await self.config_server.get_docker_registry()
        name, tag = await self.config_server.resolve_image_name(lang)
        lang = f'{name}:{tag}'

        kernel_id = uuid.uuid4()

        request = SessionCreationRequest(
            sess_id=sess_id,
            kernel_id=kernel_id,
            access_key=access_key,
            docker_registry=docker_registry,
            lang=lang,
            session_tag=session_tag,
            creation_config=creation_config,
        )
        kernel_access_info = await scaling_group.register_request(request, conn=conn)
        await scaling_group.schedule(conn=conn)
        return kernel_access_info

    async def create_session(self, agent_id, kernel_id, conn=None):
        # sess_id = request.sess_id
        # kernel_id = request.kernel_id
        # creation_config = request.creation_config
        #
        # mounts = creation_config.get('mounts') or []
        # environ = creation_config.get('environ') or {}

        async with reenter_txn(self.dbpool, conn) as conn:

            # TODO: move below to JobScheduler::schedule()
            # # scan available slots from alive agents
            # avail_slots = []
            # query = (sa.select([agents], for_update=True)
            #            .where(agents.c.status == AgentStatus.ALIVE))
            #
            # async for row in conn.execute(query):
            #     if row['id'] not in runnable_agents:
            #         continue
            #     sdiff = ResourceSlot(
            #         id=row['id'],
            #         mem=row['mem_slots'] - row['used_mem_slots'],
            #         cpu=row['cpu_slots'] - row['used_cpu_slots'],
            #         gpu=row['gpu_slots'] - row['used_gpu_slots'],
            #     )
            #     avail_slots.append(sdiff)
            #
            # # check minimum requirement
            # avail_slots = [s for s in avail_slots
            #                if s.mem >= (required_shares.mem * 1024) and
            #                   s.cpu >= required_shares.cpu and
            #                   s.gpu >= required_shares.gpu]
            #
            # # load-balance
            # if avail_slots:
            #     agent_id = (max(avail_slots, key=lambda s: (s.gpu, s.cpu, s.mem))).id
            # else:
            #     raise InstanceNotAvailable
            #
            # # reserve slots
            # mem_col = agents.c.used_mem_slots
            # cpu_col = agents.c.used_cpu_slots
            # gpu_col = agents.c.used_gpu_slots
            # query = (sa.update(agents)
            #            .values({
            #                'used_mem_slots': mem_col + required_shares.mem * 1024,
            #                'used_cpu_slots': cpu_col + required_shares.cpu,
            #                'used_gpu_slots': gpu_col + required_shares.gpu,
            #            })
            #            .where(agents.c.id == agent_id))
            # result = await conn.execute(query)
            # assert result.rowcount == 1

            # Create kernel by invoking the agent on the instance.
            query = (sa.select('*')
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            row = await result.first()
            sess_id = row.sess_id
            access_key = row.access_key
            required_shares = ResourceSlot(
                mem=row.mem_slot,
                cpu=row.cpu_slot,
                gpu=row.gpu_slot,
            )
            lang = row.lang
            agent_addr = row.agent_addr
            mounts = row.mounts
            environ = {k: v
                       for k, v in map(lambda x: x.split('='), row.environ)}
            assert agent_addr is not None

            # Update kernel status.
            query = (kernels.update()
                            .values({
                                'status': KernelStatus.PREPARING,
                                'agent': agent_id,
                                'agent_addr': agent_addr,
                            })
                            .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            assert result.rowcount == 1

            async with self.handle_kernel_exception(
                    'create_session', sess_id, access_key):
                async with RPCContext(agent_addr, 3) as rpc:
                    config = {
                        'lang': lang,
                        'limits': {
                            # units: share
                            'mem_slot': str(required_shares.mem),
                            'cpu_slot': str(required_shares.cpu),
                            'gpu_slot': str(required_shares.gpu),
                        },
                        'mounts': mounts,
                        'environ': environ,
                    }
                    created_info = await rpc.call.create_kernel(str(kernel_id),
                                                                config)
                if created_info is None:
                    raise KernelCreationFailed('ooops')
                log.debug('create_session("{0}", "{1}") -> created on {2}\n{3!r}',
                          sess_id, access_key, agent_id, created_info)
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
                                    'cpu_set': [],  # TODO: revamp with resource_spec
                                    'gpu_set': [],  # TODO: revamp with resource_spec
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
                    # units: share
                    'cpu_slot': kernel['cpu_slot'],
                    'gpu_slot': kernel['gpu_slot'],
                    'mem_slot': kernel['mem_slot'] / 1024,
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

    async def download_file(self, sess_id, access_key, filepath):
        async with self.handle_kernel_exception('download_file', sess_id,
                                                access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 180) as rpc:
                coro = rpc.call.download_file(str(kernel['id']), filepath)
                if coro is None:
                    log.warning('download_file cancelled')
                    return None
                return await coro

    async def list_files(self, sess_id, access_key, path):
        async with self.handle_kernel_exception('list_files', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 5) as rpc:
                coro = rpc.call.list_files(str(kernel['id']), path)
                if coro is None:
                    log.warning('list_files cancelled')
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

    async def kill_all_sessions_in_agent(self, agent_addr):
        async with RPCContext(agent_addr, 5) as rpc:
            coro = rpc.call.clean_all_kernels('manager-freeze-force-kill')
            if coro is None:
                log.warning('kill_all_sessions_in_agent cancelled')
                return None
            return await coro

    async def kill_all_sessions(self, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.select([agents.c.addr])
                       .where(agents.c.status == AgentStatus.ALIVE))
            result = await conn.execute(query)
            alive_agent_addrs = [row.addr for row in result]
            log.debug(str(alive_agent_addrs))
            tasks = [self.kill_all_sessions_in_agent(agent_addr)
                     for agent_addr in alive_agent_addrs]
            await asyncio.gather(*tasks)

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
                                agents.c.gpu_slots,
                                agents.c.scaling_group],
                               for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            row = await result.first()
            ob_factors = await self.config_server.get_overbook_factors()
            reported_mem_slots = int(Decimal(agent_info['mem_slots']) *
                                     Decimal(ob_factors['mem']))
            reported_cpu_slots = float(Decimal(agent_info['cpu_slots']) *
                                       Decimal(ob_factors['cpu']))
            reported_gpu_slots = float(Decimal(agent_info['gpu_slots']) *
                                       Decimal(ob_factors['gpu']))
            try:
                scaling_group = agent_info.get('scaling_group', None)
                assert scaling_group is not None
                query = (sa.select('*')
                           .select_from(scaling_groups)
                           .where(scaling_groups.c.name == scaling_group))
                result = await conn.execute(query)
                assert await result.first()
            except AssertionError:
                log.warning('agent with invalid scaling group sends heartbeat '
                            '(agent id: {0}, scaling group: {1})',
                            agent_id, scaling_group)
                return
            agent_created = False
            if row is None or row.status is None:
                # new agent detected!
                agent_created = True
                log.info('agent {0} joined! (scaling group: {1})',
                         agent_id, scaling_group)
                query = agents.insert().values({
                    'id': agent_id,
                    'status': AgentStatus.ALIVE,
                    'region': agent_info['region'],
                    'scaling_group': scaling_group,
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
                if scaling_group != row.scaling_group:
                    # Scaling group of an agent cannot be changed when it is running,
                    # so updated scaling group implies the agent has been terminated
                    # and then started and assigned to different scaling group.
                    agent_created = True
                    log.warning('agent {0} revived! (scaling group: {1})',
                                agent_id, scaling_group)
                    changed_cols['scaling_group'] = scaling_group
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
                agent_created = True
                log.warning('agent {0} revived! (scaling_group: {1})',
                            agent_id, scaling_group)
                query = (sa.update(agents)
                           .values({
                               'status': AgentStatus.ALIVE,
                               'region': agent_info['region'],
                               'scaling_group': scaling_group,
                               'addr': agent_info['addr'],
                               'lost_at': None,
                               'mem_slots': reported_mem_slots,
                               'cpu_slots': reported_cpu_slots,
                               'gpu_slots': reported_gpu_slots,
                           })
                           .where(agents.c.id == agent_id))
                await conn.execute(query)
            else:
                log.error('should not reach here! {0}', type(row.status))

            if agent_created:
                scaling_group = await self.get_scaling_group(
                    scaling_group=scaling_group, conn=conn)
                await scaling_group.schedule(conn=conn)

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
                log.warning('agent {0} heartbeat timeout detected.', agent_id)
            elif status == AgentStatus.TERMINATED:
                log.info('agent {0} has terminated.', agent_id)
            query = (sa.update(agents)
                       .values({
                           'status': status,
                           'lost_at': datetime.now(tzutc()),
                       })
                       .where(agents.c.id == agent_id))
            await conn.execute(query)

            scaling_group = await self.get_scaling_group(
                agent_id=agent_id, conn=conn)
            await scaling_group.schedule(conn=conn)

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
            # units: absolute
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
            # units: absolute
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

            if kernel['agent'] and prev_status == KernelStatus.RESIZING:
                scaling_group = await self.get_scaling_group(
                    agent_id=kernel['agent'], conn=conn)
                await scaling_group.schedule(conn=conn)

    async def mark_session_terminated(self, sess_id, access_key, conn=None):
        '''
        Mark the compute session terminated and restore the concurrency limit
        of the owner access key.  Releasing resource limits is handled by
        func:`mark_kernel_terminated`.
        '''
        async with reenter_txn(self.dbpool, conn) as conn:
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
