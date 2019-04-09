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
from ai.backend.common.docker import get_registry_info, get_known_registries
from ai.backend.common.types import ImageRef, ResourceSlot
from ai.backend.common.logging import BraceStyleAdapter
from ..gateway.exceptions import (
    BackendError, InvalidAPIParameters,
    InstanceNotAvailable, InstanceNotFound,
    KernelNotFound, KernelAlreadyExists,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    AgentError)
from .models import (
    agents, kernels, keypairs,
    AgentStatus, KernelStatus,
)

__all__ = ['AgentRegistry', 'InstanceNotFound']

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.registry'))

agent_peers = {}


@aiotools.actxmgr
async def RPCContext(addr, timeout=None):
    preserved_exceptions = (
        NotFoundError,
        ParametersError,
        asyncio.TimeoutError,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    )
    global agent_peers
    peer = agent_peers.get(addr, None)
    if peer is None:
        peer = await aiozmq.rpc.connect_rpc(
            connect=addr, error_table={
                'concurrent.futures._base.TimeoutError': asyncio.TimeoutError,
            })
        peer.transport.setsockopt(zmq.LINGER, 1000)
        agent_peers[addr] = peer
    try:
        with _timeout(timeout):
            yield peer
    except preserved_exceptions:
        raise
    except Exception:
        exc_type, exc, tb = sys.exc_info()
        if issubclass(exc_type, GenericError):
            e = AgentError(exc.args[0], exc.args[1], exc_repr=exc.args[2])
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
        else:
            e = AgentError(exc_type, exc.args)
            raise e.with_traceback(tb)


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
        global agent_peers
        closing_tasks = []
        for addr, peer in agent_peers.items():
            peer.close()
            closing_tasks.append(peer.wait_closed())
        await asyncio.gather(*closing_tasks)

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
            'refresh_session': KernelExecutionFailed,
        }
        exc_class = op_exc[op]
        # NOTE: Error logging is done outside of this actxmanager.
        try:
            yield
        except asyncio.TimeoutError:
            if set_error:
                await self.set_session_status(
                    sess_id, access_key, KernelStatus.ERROR,
                    status_info=f'Operation timeout ({op})')
            if error_callback:
                await error_callback()
            raise exc_class('TIMEOUT') from None
        except asyncio.CancelledError:
            if cancellation_callback:
                await cancellation_callback()
            raise
        except AgentError as e:
            # TODO: wrap some assertion errors as "invalid requests"
            if set_error:
                await self.set_session_status(sess_id, access_key,
                                              KernelStatus.ERROR,
                                              status_info='Agent error')
            if error_callback:
                await error_callback()
            raise exc_class('FAILURE', e) from None
        except BackendError:
            # silently re-raise to make them handled by gateway http handlers
            raise
        except:
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
                kernels.c.agent_addr, kernels.c.kernel_host,
                kernels.c.image, kernels.c.registry,
                kernels.c.service_ports]
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
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key,
                kernels.c.service_ports]
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
                                    image, creation_config,
                                    resource_policy,
                                    tag=None):
        requested_image_ref = \
            await ImageRef.resolve_alias(image, self.config_server.etcd)
        try:
            kern = await self.get_session(sess_id, access_key)
            running_image_ref = ImageRef(kern['image'], [kern['registry']])
            if running_image_ref != requested_image_ref:
                raise KernelAlreadyExists
            create = False
        except KernelNotFound:
            create = True
        if create:
            kern = await self.create_session(
                sess_id, access_key,
                requested_image_ref, creation_config,
                resource_policy,
                session_tag=tag)
        assert kern is not None
        return kern, create

    async def create_session(self, sess_id: str, access_key: str,
                             image_ref: ImageRef,
                             creation_config: dict,
                             resource_policy: dict,
                             session_tag=None):
        agent_id = None
        created_info = None
        mounts = creation_config.get('mounts') or []
        environ = creation_config.get('environ') or {}

        # TODO: merge into a single call
        image_info = await self.config_server.inspect_image(image_ref)
        image_min_slots, image_max_slots = \
            await self.config_server.get_image_slot_ranges(image_ref)
        known_slot_types = \
            await self.config_server.get_resource_slots()

        # ==== BEGIN: ENQUEUING PART ====
        # This part will be moved to the job-enqueue handler.

        # Sanitize user input: does it have resource config?
        if 'resources' in creation_config:
            # Sanitize user input: does it have "known" resource slots only?
            for slot_key, slot_value in creation_config['resources'].items():
                if slot_key not in known_slot_types:
                    raise InvalidAPIParameters(
                        f'Unknown requested resource slot: {slot_key}')
            try:
                requested_slots = ResourceSlot.from_user_input(
                    creation_config['resources'], known_slot_types)
            except ValueError:
                log.exception('request_slots & image_slots calculation error')
                # happens when requested_slots have more keys
                # than the image-defined slots
                # (e.g., image does not support accelerators
                #  requested by the client)
                raise InvalidAPIParameters(
                    'Your resource request has resource type(s) '
                    'not supported by the image.')

            # If the resource is not specified, fill them with image minimums.
            for k, v in requested_slots.items():
                if v is None or v == 0:
                    requested_slots[k] = image_min_slots[k]
        else:
            # Handle the legacy clients (prior to v19.03)
            # We support CPU/memory conversion, but to use accelerators users
            # must update their clients because the slots names are not provided
            # by the accelerator plugins.
            cpu = creation_config.get('instanceCores')
            if cpu is None:  # the key is there but may be null.
                cpu = image_min_slots['cpu']
            mem = creation_config.get('instanceMemory')
            if mem is None:  # the key is there but may be null.
                mem = image_min_slots['mem']
            else:
                # In legacy clients, memory is normalized to GiB.
                mem = str(mem) + 'g'
            requested_slots = ResourceSlot.from_user_input({
                'cpu': cpu,
                'mem': mem,
            }, known_slot_types)
            gpu = creation_config.get('instanceGPUs')
            if gpu is not None:
                raise InvalidAPIParameters('Client upgrade required '
                                           'to use GPUs (v19.03+).')
            tpu = creation_config.get('instanceTPUs')
            if tpu is not None:
                raise InvalidAPIParameters('Client upgrade required '
                                           'to use TPUs (v19.03+).')

        # Check the image resource slots.
        log.debug('requested_slots: {}', requested_slots)
        log.debug('image_min_slots: {}', image_min_slots)
        log.debug('image_max_slots: {}', image_max_slots)

        # Check if: requested >= image-minimum
        if image_min_slots > requested_slots:
            raise InvalidAPIParameters(
                'Your resource request is smaller than '
                'the minimum required by the image. ({})'.format(' '.join(
                    f'{k}={v}' for k, v in
                    image_min_slots.to_humanized(known_slot_types).items()
                )))

        # Check if: requested <= image-maximum
        if not (requested_slots <= image_max_slots):
            raise InvalidAPIParameters(
                'Your resource request is larger than '
                'the maximum allowed by the image. ({})'
                .format(' '.join(
                    f'{k}={v}' for k, v in
                    image_max_slots.to_humanized(known_slot_types).items()
                )))

        # Check the keypair resource policy.
        # - Keypair resource occupation includes both running and enqueued sessions,
        #   while agent resource occupation includes only running sessions.
        # - TODO: merge multicontainer-session branch and check
        #         max_containers_per_session.
        # - TODO: check scaling-group resource policy as well.
        total_allowed = ResourceSlot.from_policy(resource_policy, known_slot_types)
        async with self.dbpool.acquire() as conn, conn.begin():
            key_occupied = await self.get_keypair_occupancy(access_key, conn=conn)
            log.debug('{} current_occupancy: {}', access_key, key_occupied)
            log.debug('{} total_allowed: {}', access_key, total_allowed)
            if not (key_occupied + requested_slots <= total_allowed):
                raise InvalidAPIParameters(
                    'Your resource quota is exceeded. ({})'
                    .format(' '.join(
                        f'{k}={v}' for k, v in
                        total_allowed.to_humanized(known_slot_types).items()
                    )))

        # ==== END: ENQUEUING PART ====

        async with self.dbpool.acquire() as conn, conn.begin():

            # Fetch all agent available slots and normalize them to "remaining" slots
            possible_agent_slots = []
            query = (
                sa.select([
                    agents.c.id,
                    agents.c.available_slots,
                    agents.c.occupied_slots,
                ], for_update=True)
                .where(agents.c.status == AgentStatus.ALIVE)
            )
            async for row in conn.execute(query):
                capacity_slots = row['available_slots']
                occupied_slots = row['occupied_slots']
                log.debug('{} capacity: {!r}', row['id'], capacity_slots)
                log.debug('{} occupied: {!r}', row['id'], occupied_slots)
                try:
                    remaining_slots = capacity_slots - occupied_slots

                    # Check if: any(remaining >= requested)
                    if remaining_slots >= requested_slots:
                        possible_agent_slots.append((
                            row['id'],
                            remaining_slots,
                            occupied_slots))
                except ValueError:
                    # happens when requested_slots have more keys
                    # than the agent_slots
                    # (e.g., agent does not have accelerators
                    #  requested by the client)
                    continue

            # Load-balance! (choose the agent with most remaining slots)
            # Here, all items in possible_agent_slots have the same keys,
            # allowing the total ordering property.
            if possible_agent_slots:
                agent_id, _, current_occupied_slots = \
                    max(possible_agent_slots, key=lambda s: s[1])
            else:
                raise InstanceNotAvailable

            # Reserve slots
            query = (sa.update(agents)
                       .values({
                           'occupied_slots': current_occupied_slots + requested_slots
                       })
                       .where(agents.c.id == agent_id))
            await conn.execute(query)

            # Create kernel by invoking the agent on the instance.
            query = (sa.select([agents.c.addr])
                       .where(agents.c.id == agent_id))
            agent_addr = await conn.scalar(query)
            assert agent_addr is not None

            registry_url, registry_creds = \
                await get_registry_info(self.config_server.etcd,
                                        image_ref.registry)

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
                'image': image_ref.canonical,
                'registry': image_ref.registry,
                'tag': session_tag,
                'occupied_slots': requested_slots,
                'occupied_shares': {},
                'environ': [f'{k}={v}' for k, v in environ.items()],
                'kernel_host': None,
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)

        try:

            async with self.handle_kernel_exception(
                    'create_session', sess_id, access_key):
                # the agent may be pulling an image!
                # (TODO: return early and update the kernel status
                #        via asynchronous events)
                async with RPCContext(agent_addr, None) as rpc:
                    config = {
                        'image': {
                            'registry': {
                                'name': image_ref.registry,
                                'url': str(registry_url),
                                **registry_creds,
                            },
                            'digest': image_info['digest'],
                            'canonical': image_ref.canonical,
                            'labels': image_info['labels'],
                        },
                        'resource_slots': requested_slots.to_json(),
                        'idle_timeout': resource_policy['idle_timeout'],
                        'mounts': mounts,
                        'environ': environ,
                    }
                    created_info = await rpc.call.create_kernel(str(kernel_id),
                                                                config)
                if created_info is None:
                    raise KernelCreationFailed('ooops')

        except Exception as e:  # including timeout and cancellation

            # This catches errors during kernel creation via agent RPC.
            log.debug('create_session("{0}", "{1}") -> failed ({2})',
                      sess_id, access_key, repr(e))

            async with self.dbpool.acquire() as conn, conn.begin():

                # The following ops are same to the "kernel_terminated" handler.
                # Since events are only generated by the agents and the "kernel_created"
                # event is generated only when kernel creation was successful,
                # we do this by ourselves.

                # Mark as terminated with error
                query = (
                    kernels.update()
                    .values({
                        'status': KernelStatus.TERMINATED,
                        'status_info': 'creation-failed',
                        'terminated_at': datetime.now(tzutc()),
                    })
                    .where(kernels.c.id == kernel_id))
                await conn.execute(query)

                # Un-reserve slots
                query = (
                    sa.select([agents.c.occupied_slots], for_update=True)
                    .select_from(agents)
                    .where(agents.c.id == agent_id))
                current_occupied_slots = await conn.scalar(query)
                query = (
                    sa.update(agents)
                    .values({
                        'occupied_slots': current_occupied_slots - requested_slots
                    })
                    .where(agents.c.id == agent_id))
                await conn.execute(query)

            # Bubble-up exceptions
            raise

        else:

            log.debug('create_session("{0}", "{1}") -> created on {2}\n{3!r}',
                      sess_id, access_key, agent_id, created_info)
            assert str(kernel_id) == created_info['id']

            # Return and record kernel access information
            agent_host = URL(agent_addr).host
            kernel_host = created_info.get('kernel_host', agent_host)
            service_ports = created_info.get('service_ports', [])
            kernel_access_info = {
                'id': kernel_id,
                'sess_id': sess_id,
                'agent': agent_id,
                'agent_addr': agent_addr,
                'kernel_host': kernel_host,
                'service_ports': service_ports,
            }
            # NOTE: created_info contains resource_spec
            async with self.dbpool.acquire() as conn, conn.begin():
                query = (
                    kernels.update()
                    .values({
                        # TODO: add more kernel status about image pulling
                        # TODO: move this status transition to event handler for
                        #       "kernel_started"
                        'status': KernelStatus.RUNNING,
                        'container_id': created_info['container_id'],
                        'occupied_shares': {},
                        'kernel_host': kernel_host,
                        'repl_in_port': created_info['repl_in_port'],
                        'repl_out_port': created_info['repl_out_port'],
                        'stdin_port': created_info['stdin_port'],
                        'stdout_port': created_info['stdout_port'],
                        'service_ports': service_ports,
                    })
                    .where(kernels.c.id == kernel_id))
                await conn.execute(query)
            return kernel_access_info

    async def get_keypair_occupancy(self, access_key, *, conn=None):
        known_slot_types = \
            await self.config_server.get_resource_slots()
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.select([kernels.c.occupied_slots])
                       .where((kernels.c.access_key == access_key) &
                              (kernels.c.status != KernelStatus.TERMINATED)))
            zero = ResourceSlot()
            key_occupied = sum([
                row['occupied_slots']
                async for row in conn.execute(query)], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

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
                raise
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                return await rpc.call.destroy_kernel(str(kernel['id']))

    async def restart_session(self, sess_id, access_key):
        async with self.handle_kernel_exception(
                'restart_session', sess_id, access_key, set_error=True):
            extra_cols = (
                kernels.c.image,
                kernels.c.registry,
                kernels.c.occupied_slots,
                kernels.c.environ,
            )
            async with self.dbpool.acquire() as conn, conn.begin():
                kernel = await self.get_session(sess_id, access_key,
                                                field=extra_cols,
                                                for_update=True,
                                                db_connection=conn)
                await self.set_session_status(sess_id, access_key,
                                              KernelStatus.RESTARTING,
                                              db_connection=conn)

            registry_url, registry_creds = \
                await get_registry_info(self.config_server.etcd,
                                        kernel['registry'])
            image_ref = ImageRef(kernel['image'], [kernel['registry']])
            image_info = await self.config_server.inspect_image(image_ref)

            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                environ = {
                     k: v for k, v in
                     map(lambda s: s.split('=', 1), kernel['environ'])
                }
                new_config = {
                    'image': {
                        'registry': {
                            'name': image_ref.registry,
                            'url': str(registry_url),
                            **registry_creds,
                        },
                        'digest': image_info['digest'],
                        'canonical': kernel['image'],
                        'labels': image_info['labels'],
                    },
                    'environ': environ,
                    'mounts': [],  # recovered from container config
                    'resource_slots':
                        kernel['occupied_slots'].to_json(),  # unused currently
                }
                kernel_info = await rpc.call.restart_kernel(str(kernel['id']),
                                                            new_config)
                # TODO: what if prev status was "building" or others?
                await self.set_session_status(
                    sess_id, access_key,
                    KernelStatus.RUNNING,
                    container_id=kernel_info['container_id'],
                    repl_in_port=kernel_info['repl_in_port'],
                    repl_out_port=kernel_info['repl_out_port'],
                    stdin_port=kernel_info['stdin_port'],
                    stdout_port=kernel_info['stdout_port'],
                    service_ports=kernel_info.get('service_ports', [])
                )

    async def execute(self, sess_id, access_key,
                      api_version, run_id, mode, code, opts, *,
                      flush_timeout=None):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            # The agent aggregates at most 2 seconds of outputs
            # if the kernel runs for a long time.
            major_api_version = api_version[0]
            if major_api_version == 4:  # manager-agent protocol is same.
                major_api_version = 3
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                coro = rpc.call.execute(major_api_version, str(kernel['id']),
                                        run_id, mode, code, opts,
                                        flush_timeout)
                if coro is None:
                    log.warning('execute cancelled')
                    return None
                return await coro

    async def interrupt_session(self, sess_id, access_key):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                coro = rpc.call.interrupt_kernel(str(kernel['id']))
                if coro is None:
                    log.warning('interrupt cancelled')
                    return None
                return await coro

    async def get_completions(self, sess_id, access_key, mode, text, opts):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 10) as rpc:
                coro = rpc.call.get_completions(str(kernel['id']), mode, text, opts)
                if coro is None:
                    log.warning('get_completions cancelled')
                    return None
                return await coro

    async def start_service(self, sess_id, access_key, service, opts):
        async with self.handle_kernel_exception('execute', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], None) as rpc:
                coro = rpc.call.start_service(str(kernel['id']), service, opts)
                if coro is None:
                    log.warning('stat_service cancelled')
                    return None
                return await coro

    async def upload_file(self, sess_id, access_key, filename, payload):
        async with self.handle_kernel_exception('upload_file', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], None) as rpc:
                coro = rpc.call.upload_file(str(kernel['id']), filename, payload)
                if coro is None:
                    log.warning('upload_file cancelled')
                    return None
                return await coro

    async def download_file(self, sess_id, access_key, filepath):
        async with self.handle_kernel_exception('download_file', sess_id,
                                                access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], None) as rpc:
                coro = rpc.call.download_file(str(kernel['id']), filepath)
                if coro is None:
                    log.warning('download_file cancelled')
                    return None
                return await coro

    async def list_files(self, sess_id, access_key, path):
        async with self.handle_kernel_exception('list_files', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                coro = rpc.call.list_files(str(kernel['id']), path)
                if coro is None:
                    log.warning('list_files cancelled')
                    return None
                return await coro

    async def get_logs(self, sess_id, access_key):
        async with self.handle_kernel_exception('get_logs', sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                coro = rpc.call.get_logs(str(kernel['id']))
                if coro is None:
                    log.warning('get_logs cancelled')
                    return None
                return await coro

    async def refresh_session(self, sess_id, access_key):
        async with self.handle_kernel_exception('refresh_session',
                                                sess_id, access_key):
            kernel = await self.get_session(sess_id, access_key)
            async with RPCContext(kernel['agent_addr'], 30) as rpc:
                coro = rpc.call.refresh_idle(str(kernel['id']))
                if coro is None:
                    log.warning('refresh_session cancelled')
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
        async with RPCContext(agent_addr, 30) as rpc:
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
            query = (sa.select([agents.c.status,
                                agents.c.available_slots],
                               for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            row = await result.first()

            slot_key_and_units = {
                k: v[0] for k, v in
                agent_info['resource_slots'].items()}
            available_slots = ResourceSlot({
                k: v[1] for k, v in
                agent_info['resource_slots'].items()})

            # compare and update etcd slot_keys

            if row is None or row.status is None:
                # new agent detected!
                log.info('agent {0} joined!', agent_id)
                query = agents.insert().values({
                    'id': agent_id,
                    'status': AgentStatus.ALIVE,
                    'region': agent_info['region'],
                    'available_slots': available_slots,
                    'occupied_slots': {},
                    'addr': agent_info['addr'],
                    'first_contact': now,
                    'lost_at': None,
                })
                result = await conn.execute(query)
                assert result.rowcount == 1
                # TODO: aggregate from all agents, instead of replacing everytime
                await self.config_server.update_resource_slots(slot_key_and_units)
            elif row.status == AgentStatus.ALIVE:
                updates = {}
                current_avail_slots = row.available_slots
                if available_slots != current_avail_slots:
                    updates['available_slots'] = available_slots
                # occupied_slots are updated when kernels starts/terminates
                if updates:
                    query = (sa.update(agents)
                               .values(updates)
                               .where(agents.c.id == agent_id))
                    await conn.execute(query)
            elif row.status in (AgentStatus.LOST, AgentStatus.TERMINATED):
                log.warning('agent {0} revived!', agent_id)
                query = (sa.update(agents)
                           .values({
                               'status': AgentStatus.ALIVE,
                               'region': agent_info['region'],
                               'addr': agent_info['addr'],
                               'lost_at': None,
                               'available_slots': available_slots,
                           })
                           .where(agents.c.id == agent_id))
                await conn.execute(query)
                # TODO: aggregate from all agents, instead of replacing everytime
                await self.config_server.update_resource_slots(slot_key_and_units)
            else:
                log.error('should not reach here! {0}', type(row.status))

        # Update the mapping of kernel images to agents.
        known_registries = await get_known_registries(self.config_server.etcd)
        images = msgpack.unpackb(snappy.decompress(agent_info['images']))
        pipe = self.redis_image.pipeline()
        for image in images:
            image_ref = ImageRef(image[0], known_registries)
            pipe.sadd(image_ref.canonical, agent_id)
        await pipe.execute()

    async def mark_agent_terminated(self, agent_id, status, conn=None):
        # TODO: interpret kern_id to sess_id
        # for kern_id in (await app['registry'].get_kernels_in_instance(agent_id)):
        #     for handler in app['stream_pty_handlers'][kern_id].copy():
        #         handler.cancel()
        #         await handler
        #  TODO: define behavior when agent reuse running instances upon revive
        # await app['registry'].forget_all_kernels_in_instance(agent_id)

        global agent_peers

        await self.redis_live.hdel('last_seen', agent_id)

        pipe = self.redis_image.pipeline()
        async for imgname in self.redis_image.iscan():
            pipe.srem(imgname, agent_id)
        await pipe.execute()

        async with reenter_txn(self.dbpool, conn) as conn:

            query = (sa.select([agents.c.status, agents.c.addr], for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)
            row = await result.first()
            peer = agent_peers.pop(row['addr'], None)
            if peer is not None:
                peer.close()
                await peer.wait_closed()
            prev_status = row['status']
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

    async def mark_kernel_terminated(self, kernel_id, reason):
        '''
        Mark the kernel (individual worker) terminated and release
        the resource slots occupied by it.
        '''
        async with self.dbpool.acquire() as conn, conn.begin():
            # Check the current status.
            query = (sa.select([kernels.c.access_key, kernels.c.role, kernels.c.status], for_update=True)
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            row = await result.first()
            if row is None or row['status'] == KernelStatus.TERMINATED:
                # Skip if non-existent or already terminated.
                return
            if row['role'] == 'master':
                # The master session is terminated; decrement the user's concurrency counter
                query = (sa.update(keypairs)
                           .values(concurrency_used=keypairs.c.concurrency_used - 1)
                           .where(keypairs.c.access_key == row['access_key']))
                await conn.execute(query)

            # Change the status to TERMINATED.
            # (we don't delete the row for later logging and billing)
            updates = {
                'status': KernelStatus.TERMINATED,
                'status_info': reason,
                'terminated_at': datetime.now(tzutc()),
            }
            kern_stat = await self.redis_stat.hgetall(kernel_id)
            if kern_stat is not None and 'cpu_used' in kern_stat:
                updates.update({
                    'cpu_used': int(float(kern_stat['cpu_used'])),
                    'mem_max_bytes': int(kern_stat['mem_max_bytes']),
                    'net_rx_bytes': int(kern_stat['net_rx_bytes']),
                    'net_tx_bytes': int(kern_stat['net_tx_bytes']),
                    'io_read_bytes': int(kern_stat['io_read_bytes']),
                    'io_write_bytes': int(kern_stat['io_write_bytes']),
                    'io_max_scratch_size': int(kern_stat['io_max_scratch_size']),
                })
            query = (sa.update(kernels)
                       .values(updates)
                       .where(kernels.c.id == kernel_id))
            await conn.execute(query)

            # Release resource slots.
            query = (sa.select([kernels.c.agent, kernels.c.occupied_slots])
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            kernel = await result.first()
            if kernel is None:
                return
            query = (sa.select([agents.c.occupied_slots],
                               for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == kernel['agent']))
            result = await conn.execute(query)
            agent = await result.first()
            if agent is None:
                return
            # units: absolute
            updated_occupied_slots = \
                agent['occupied_slots'] - kernel['occupied_slots']
            query = (sa.update(agents)
                       .values({
                           'occupied_slots': updated_occupied_slots,
                       })
                       .where(agents.c.id == kernel['agent']))
            await conn.execute(query)

    async def forget_instance(self, inst_id):
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (sa.update(agents)
                       .values(status=AgentStatus.TERMINATED,
                               lost_at=datetime.now(tzutc()))
                       .where(agents.c.id == inst_id))
            await conn.execute(query)
