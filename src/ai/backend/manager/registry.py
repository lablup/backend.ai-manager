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
    DefaultForUnspecified,
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
    except asyncio.TimeoutError:
        log.warning('timeout while connecting to agent at {}', addr)
        raise
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
                                    conn=None, tag=None):
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
                conn=conn, session_tag=tag)
        assert kern is not None
        return kern, create

    async def create_session(self, sess_id: str, access_key: str,
                             image_ref: ImageRef,
                             creation_config: dict,
                             resource_policy: dict,
                             conn=None, session_tag=None):
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
                requested_slots = ResourceSlot(creation_config['resources']) \
                    .as_numeric(known_slot_types, unknown='error', fill_missing=True)
            except ValueError:
                log.exception('request_slots & image_slots calculation error')
                # happens when requested_slots have more keys
                # than the image-defined slots
                # (e.g., image does not support accelerators
                #  requested by the client)
                raise InvalidAPIParameters(
                    'Your resource request has resource types '
                    'not supported by the image.')
            for slot_key, value in requested_slots.items():
                if value is None or value == 0:
                    requested_slots[slot_key] = image_min_slots[slot_key]
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
            requested_slots = ResourceSlot({
                'cpu': cpu,
                'mem': mem,
            }).as_numeric(known_slot_types, fill_missing=True)
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
                    image_min_slots.as_humanized(known_slot_types).items()
                )))

        # Check if: requested <= image-maximum
        if not (requested_slots.lte_unlimited(image_max_slots)):
            raise InvalidAPIParameters(
                'Your resource request is larger than '
                'the maximum allowed by the image. ({}; zero means unlimited)'
                .format(' '.join(
                    f'{k}={v}' for k, v in
                    image_max_slots.as_humanized(known_slot_types).items()
                )))

        # If the resource is not specified, fill them with image minimums.
        for slot_key, slot_val in image_min_slots.items():
            if slot_key not in known_slot_types:
                continue
            if slot_key not in requested_slots:
                requested_slots[slot_key] = slot_val

        # Check the keypair resource policy.
        # - Keypair resource occupation includes both running and enqueued sessions,
        #   while agent resource occupation includes only running sessions.
        # - TODO: merge multicontainer-session branch and check
        #         max_containers_per_session.
        # - TODO: check scaling-group resource policy as well.
        total_allowed = (ResourceSlot(resource_policy['total_resource_slots'])
                         .as_numeric(known_slot_types,
                                     unknown='drop', fill_missing=True))
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.select([kernels.c.occupied_slots])
                       .where((kernels.c.access_key == access_key) &
                              (kernels.c.status != KernelStatus.TERMINATED)))
            zero = ResourceSlot({}).as_numeric(known_slot_types, fill_missing=True)
            key_occupied = sum([
                (ResourceSlot(row['occupied_slots'])
                 .as_numeric(known_slot_types,
                             unknown='drop', fill_missing=True))
                async for row in conn.execute(query)], zero)
            log.debug('{} current_occupancy: {}', access_key, key_occupied)
            log.debug('{} total_allowed: {} (default {})',
                      access_key, total_allowed,
                      resource_policy['default_for_unspecified'].name)
            if (resource_policy['default_for_unspecified'] ==
                DefaultForUnspecified.LIMITED):
                if not (key_occupied + requested_slots <= total_allowed):
                    raise InvalidAPIParameters(
                        'Your resource quota is exceeded. ({})'
                        .format(' '.join(
                            f'{k}={v}' for k, v in
                            total_allowed.as_humanized(known_slot_types).items()
                        )))
            else:
                if not (key_occupied + requested_slots).lte_unlimited(total_allowed):
                    raise InvalidAPIParameters(
                        'Your resource quota is exceeded. ({}; zero means unlimited)'
                        .format(' '.join(
                            f'{k}={v}' for k, v in
                            total_allowed.as_humanized(known_slot_types).items()
                        )))

        # ==== END: ENQUEUING PART ====

        async with reenter_txn(self.dbpool, conn) as conn:

            # Fetch all agent available slots and normalize them to "remaining" slots
            possible_agent_slots = []
            query = (sa.select([agents], for_update=True)
                       .where(agents.c.status == AgentStatus.ALIVE))
            async for row in conn.execute(query):
                total_slots = ResourceSlot(row['available_slots']) \
                              .as_numeric(known_slot_types, unknown='drop')
                occupied_slots = ResourceSlot(row['occupied_slots']) \
                                 .as_numeric(known_slot_types, unknown='drop')
                # TODO: would there be any case that occupied_slots have more keys
                #       than total_slots?
                log.debug('total resource slots: {!r}', total_slots)
                log.debug('occupied resource slots: {!r}', occupied_slots)

                remaining_slots = total_slots - occupied_slots

                # Check if: any(remaining >= requested)
                try:
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
            updated_occupied_slots = current_occupied_slots + requested_slots
            query = (sa.update(agents)
                       .values({
                           'occupied_slots': (updated_occupied_slots
                                              .as_json_numeric(known_slot_types)),
                       })
                       .where(agents.c.id == agent_id))
            result = await conn.execute(query)

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
                'occupied_slots': requested_slots.as_json_numeric(known_slot_types),
                'occupied_shares': {},
                'environ': [f'{k}={v}' for k, v in environ.items()],
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
                        'resource_slots': (requested_slots
                                           .as_json_numeric(known_slot_types)),
                        'idle_timeout': resource_policy['idle_timeout'],
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
                    'resource_slots': kernel['occupied_slots'],  # unused currently
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
                agent_info['resource_slots'].items()}) \
                .as_numeric(slot_key_and_units, unknown='error')

            # compare and update etcd slot_keys

            if row is None or row.status is None:
                # new agent detected!
                log.info('agent {0} joined!', agent_id)
                query = agents.insert().values({
                    'id': agent_id,
                    'status': AgentStatus.ALIVE,
                    'region': agent_info['region'],
                    'available_slots': (available_slots
                                        .as_json_numeric(slot_key_and_units)),
                    'occupied_slots': {},
                    'addr': agent_info['addr'],
                    'first_contact': now,
                    'lost_at': None,
                })
                result = await conn.execute(query)
                assert result.rowcount == 1
                await self.config_server.update_resource_slots(slot_key_and_units)
            elif row.status == AgentStatus.ALIVE:
                updates = {}
                current_avail_slots = (ResourceSlot(row.available_slots)
                                       .as_numeric(slot_key_and_units,
                                                   unknown='drop'))
                if available_slots != current_avail_slots:
                    updates['available_slots'] = \
                        available_slots.as_json_numeric(
                            slot_key_and_units, unknown='drop')
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
                               'available_slots': (
                                   available_slots
                                   .as_json_numeric(slot_key_and_units)),
                           })
                           .where(agents.c.id == agent_id))
                await conn.execute(query)
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
            known_slot_types = \
                await self.config_server.get_resource_slots()
            query = (sa.select([sa.column('agent'), sa.column('occupied_slots')])
                       .select_from(kernels)
                       .where(kernels.c.id == kernel_id))
            result = await conn.execute(query)
            kernel = await result.first()
            if kernel is None:
                return
            query = (sa.select([sa.column('occupied_slots')],
                               for_update=True)
                       .select_from(agents)
                       .where(agents.c.id == kernel['agent']))
            result = await conn.execute(query)
            agent = await result.first()
            if agent is None:
                return
            # units: absolute
            occupied_slots = \
                ResourceSlot(kernel['occupied_slots']).as_numeric(
                    known_slot_types, unknown='drop')
            current_occupied_slots = \
                ResourceSlot(agent['occupied_slots']).as_numeric(
                    known_slot_types, unknown='drop')
            updated_occupied_slots = current_occupied_slots - occupied_slots
            query = (sa.update(agents)
                       .values({
                           'occupied_slots': (updated_occupied_slots
                                              .as_json_numeric(known_slot_types)),
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
