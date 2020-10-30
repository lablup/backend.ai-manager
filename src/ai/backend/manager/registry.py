from __future__ import annotations

import asyncio
from contextvars import ContextVar
from collections import defaultdict
import copy
from datetime import datetime
import logging
from pathlib import Path
import secrets
import time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Container,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)
import uuid

from aiopg.sa.connection import SAConnection
from aiopg.sa.engine import Engine as SAEngine
import aiotools
from async_timeout import timeout as _timeout
from callosum.rpc import Peer, RPCUserError
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from dateutil.tz import tzutc
import snappy
import sqlalchemy as sa
from sqlalchemy.sql.expression import true
from yarl import URL
import zmq.asyncio

from ai.backend.common import msgpack, redis
from ai.backend.common.docker import get_registry_info, get_known_registries, ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.plugin.hook import (
    HookPluginContext,
    ALL_COMPLETED,
    PASSED,
)
from ai.backend.common.types import (
    AccessKey,
    BinarySize,
    KernelCreationConfig,
    KernelId,
    ResourceSlot,
    SessionId,
    SessionResult,
    SessionTypes,
    SlotName,
    SlotTypes,
)
from ai.backend.common.utils import current_loop
from .defs import INTRINSIC_SLOTS
from ..gateway.exceptions import (
    BackendError, InvalidAPIParameters,
    RejectedByHook,
    InstanceNotFound,
    SessionNotFound, TooManySessionsMatched,
    KernelCreationFailed, KernelDestructionFailed,
    KernelExecutionFailed, KernelRestartFailed,
    ScalingGroupNotFound,
    VFolderNotFound,
    AgentError,
    GenericForbidden,
)
from .models import (
    agents, kernels, keypairs, vfolders,
    keypair_resource_policies,
    AgentStatus, KernelStatus,
    query_accessible_vfolders, query_allowed_sgroups,
    recalc_concurrency_used,
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
    USER_RESOURCE_OCCUPYING_KERNEL_STATUSES,
    DEAD_KERNEL_STATUSES,
)
if TYPE_CHECKING:
    from .scheduler import SchedulingContext, PendingSession, AgentAllocationContext
    from ..gateway.events import EventDispatcher

__all__ = ['AgentRegistry', 'InstanceNotFound']

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.registry'))

agent_peers: MutableMapping[str, zmq.asyncio.Socket] = {}  # agent-addr to socket


class PeerInvoker(Peer):

    class _CallStub:

        _cached_funcs: Dict[str, Callable]
        order_key: ContextVar[Optional[str]]

        def __init__(self, peer: Peer):
            self._cached_funcs = {}
            self.peer = peer
            self.order_key = ContextVar('order_key', default=None)

        def __getattr__(self, name: str):
            if f := self._cached_funcs.get(name, None):
                return f
            else:
                async def _wrapped(*args, **kwargs):
                    request_body = {
                        'args': args,
                        'kwargs': kwargs,
                    }
                    self.peer.last_used = time.monotonic()
                    ret = await self.peer.invoke(name, request_body,
                                                 order_key=self.order_key.get())
                    self.peer.last_used = time.monotonic()
                    return ret
                self._cached_funcs[name] = _wrapped
                return _wrapped

    call: _CallStub
    last_used: float

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.call = self._CallStub(self)
        self.last_used = time.monotonic()


@aiotools.actxmgr
async def RPCContext(addr, timeout=None, *, order_key: str = None):
    global agent_peers
    peer = agent_peers.get(addr, None)
    if peer is None:
        peer = PeerInvoker(
            connect=ZeroMQAddress(addr),
            transport=ZeroMQRPCTransport,
            serializer=msgpack.packb,
            deserializer=msgpack.unpackb,
        )
        await peer.__aenter__()
        agent_peers[addr] = peer
    try:
        with _timeout(timeout):
            okey_token = peer.call.order_key.set('')
            try:
                yield peer
            finally:
                peer.call.order_key.reset(okey_token)
    except RPCUserError as orig_exc:
        raise AgentError(orig_exc.name, orig_exc.args)
    except Exception:
        raise


async def cleanup_agent_peers():
    global agent_peers
    closing_tasks = []
    for addr, peer in agent_peers.items():
        closing_tasks.append(peer.__aexit__(None, None, None))
    await asyncio.gather(*closing_tasks, return_exceptions=True)


@aiotools.actxmgr
async def reenter_txn(pool: SAEngine, conn: SAConnection):
    if conn is None:
        async with pool.acquire() as conn, conn.begin():
            yield conn
    else:
        async with conn.begin_nested():
            yield conn


class AgentRegistry:
    """
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    """

    kernel_creation_tracker: Dict[Tuple[str, KernelId], asyncio.Event]

    def __init__(
        self,
        config_server,
        dbpool,
        redis_stat,
        redis_live,
        redis_image,
        event_dispatcher: EventDispatcher,
        hook_plugin_ctx: HookPluginContext,
    ) -> None:
        self.config_server = config_server
        self.dbpool = dbpool
        self.redis_stat = redis_stat
        self.redis_live = redis_live
        self.redis_image = redis_image
        self.event_dispatcher = event_dispatcher
        self.hook_plugin_ctx = hook_plugin_ctx
        self.kernel_creation_tracker = {}

    async def init(self) -> None:
        self.heartbeat_lock = asyncio.Lock()

    async def shutdown(self) -> None:
        await cleanup_agent_peers()

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
        self,
        op: str,
        session_id: SessionId,
        access_key: AccessKey,
        error_callback=None,
        cancellation_callback=None,
        set_error: bool = False,
    ) -> AsyncIterator[None]:
        op_exc = {
            'create_session': KernelCreationFailed,
            'restart_session': KernelRestartFailed,
            'destroy_session': KernelDestructionFailed,
            'execute': KernelExecutionFailed,
            'shutdown_service': KernelExecutionFailed,
            'upload_file': KernelExecutionFailed,
            'download_file': KernelExecutionFailed,
            'list_files': KernelExecutionFailed,
            'get_logs_from_agent': KernelExecutionFailed,
            'refresh_session': KernelExecutionFailed,
        }
        exc_class = op_exc[op]
        # NOTE: Error logging is done outside of this actxmanager.
        try:
            yield
        except asyncio.TimeoutError:
            if set_error:
                await self.set_session_status(
                    session_id,
                    access_key,
                    KernelStatus.ERROR,
                    status_info=f'operation-timeout ({op})',
                )
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
                await self.set_session_status(
                    session_id,
                    access_key,
                    KernelStatus.ERROR,
                    status_info=f'agent-error ({e!r})',
                )
            if error_callback:
                await error_callback()
            raise exc_class('FAILURE', e) from None
        except BackendError:
            # silently re-raise to make them handled by gateway http handlers
            raise
        except Exception as e:
            if set_error:
                await self.set_session_status(
                    session_id,
                    access_key,
                    KernelStatus.ERROR,
                    status_info=f'other-error ({e!r})',
                )
            if error_callback:
                await error_callback()
            raise

    async def get_kernel(
        self,
        kern_id: uuid.UUID,
        field=None,
        allow_stale: bool = False,
        db_connection=None,
    ):
        '''
        Retrieve the kernel information from the given kernel ID.
        This ID is unique for all individual agent-spawned containers.

        If ``field`` is given, it extracts only the raw value of the given
        field, without wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel
        owner instance.
        '''

        cols = [kernels.c.id, kernels.c.sess_id,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key]
        if field == '*':
            cols = [sa.text('*')]
        elif isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.dbpool, db_connection) as conn:
            if allow_stale:
                query = (
                    sa.select(cols)
                    .select_from(kernels)
                    .where(kernels.c.id == kern_id)
                    .limit(1).offset(0))
            else:
                query = (
                    sa.select(cols)
                    .select_from(kernels.join(agents))
                    .where(
                        (kernels.c.id == kern_id) &
                        ~(kernels.c.status.in_(DEAD_KERNEL_STATUSES)) &
                        (agents.c.status == AgentStatus.ALIVE) &
                        (agents.c.id == kernels.c.agent)
                    )
                    .limit(1).offset(0))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise SessionNotFound
            return row

    async def get_session(
        self,
        session_name_or_id: Union[str, uuid.UUID],
        access_key: str, *,
        field=None,
        allow_stale=False,
        for_update=False,
        db_connection=None,
    ):
        '''
        Retrieve the session information from the session UUID or client-specified
        session ID paired with the given access key.
        If the session is composed of multiple containers, it returns the information
        about the master container.

        If ``field`` is given, it extracts only the raw value of the given
        field, without wrapping it as Kernel object.  If ``allow_stale`` is
        true, it does not apply the filter for "active" statuses.
        '''

        cols = [kernels.c.id, kernels.c.status,
                kernels.c.sess_id, kernels.c.access_key,
                kernels.c.agent_addr, kernels.c.kernel_host,
                kernels.c.image, kernels.c.registry,
                kernels.c.service_ports]
        if field == '*':
            cols = [sa.text('*')]
        elif isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))

        cond_id = (
            (sa.sql.expression.cast(kernels.c.id, sa.String).like(f'{session_name_or_id}%')) &
            (kernels.c.access_key == access_key) &
            (kernels.c.role == 'master')
        )
        cond_name = (
            (kernels.c.sess_id.like(f'{session_name_or_id}%')) &
            (kernels.c.access_key == access_key) &
            (kernels.c.role == 'master')
        )
        if allow_stale:
            cond_status = true()  # any status
        else:
            cond_status = ~(kernels.c.status.in_(DEAD_KERNEL_STATUSES))
        query_by_id = (
            sa.select(cols, for_update=for_update)
            .select_from(kernels)
            .where(cond_id & cond_status)
            .order_by(sa.desc(kernels.c.created_at))
            .limit(10).offset(0)
        )
        query_by_name = (
            sa.select(cols, for_update=for_update)
            .select_from(kernels)
            .where(cond_name & cond_status)
            .order_by(sa.desc(kernels.c.created_at))
        )
        if allow_stale:
            query_by_name = query_by_name.limit(10).offset(0)
        else:
            # for backward-compatibility
            query_by_name = query_by_name.limit(1).offset(0)

        async with reenter_txn(self.dbpool, db_connection) as conn:
            for query in [query_by_id, query_by_name]:
                result = await conn.execute(query)
                if result.rowcount > 1:
                    matches = [
                        {
                            'id': str(item['id']),
                            'name': item['sess_id'],
                            'status': item['status'].name,
                        }
                        async for item in result
                    ]
                    raise TooManySessionsMatched(extra_data={
                        'matches': matches,
                    })
                if result.rowcount == 0:
                    continue
                return await result.first()
            raise SessionNotFound

    async def get_sessions(
        self,
        session_names: Container[str],
        field=None,
        allow_stale=False,
        db_connection=None,
    ):
        '''
        Batched version of :meth:`get_session() <AgentRegistry.get_session>`.
        The order of the returend array is same to the order of ``sess_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising SessionNotFound exception.
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
                           .where((kernels.c.sess_id.in_(session_names)) &
                                  (kernels.c.role == 'master')))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.sess_id.in_(session_names)) &
                                  (kernels.c.role == 'master') &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent)))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return rows

    async def enqueue_session(
        self,
        session_name: str,
        access_key: str,
        image_ref: ImageRef,
        session_type: SessionTypes,
        creation_config: dict,
        resource_policy: dict, *,
        domain_name: str,
        bootstrap_script: str,
        group_id: uuid.UUID,
        user_uuid: uuid.UUID,
        user_role: str,
        startup_command: str = None,
        session_tag: str = None,
        internal_data: dict = None,
        starts_at: datetime = None,
    ) -> KernelId:
        mounts = creation_config.get('mounts') or []
        mount_map = creation_config.get('mount_map') or {}
        environ = creation_config.get('environ') or {}
        resource_opts = creation_config.get('resource_opts') or {}
        scaling_group = creation_config.get('scaling_group')
        preopen_ports = creation_config.get('preopen_ports') or []

        # Check scaling group availability if scaling_group parameter is given.
        # If scaling_group is not provided, it will be selected in scheduling step.
        if scaling_group is not None:
            async with self.dbpool.acquire() as conn, conn.begin():
                sgroups = await query_allowed_sgroups(conn, domain_name, group_id, access_key)
                for sgroup in sgroups:
                    if scaling_group == sgroup['name']:
                        break
                else:
                    raise ScalingGroupNotFound

        # sanity check for vfolders
        allowed_vfolder_types = ['user', 'group']
        # allowed_vfolder_types = await request.app['config_server'].etcd.get('path-to-vfolder-type')
        determined_mounts = []
        matched_mounts = set()
        async with self.dbpool.acquire() as conn, conn.begin():
            if mounts:
                extra_vf_conds = (
                    vfolders.c.name.in_(mounts) |
                    vfolders.c.name.startswith('.')
                )
            else:
                extra_vf_conds = vfolders.c.name.startswith('.')
            matched_vfolders = await query_accessible_vfolders(
                conn, user_uuid,
                user_role=user_role, domain_name=domain_name,
                allowed_vfolder_types=allowed_vfolder_types,
                extra_vf_conds=extra_vf_conds)

            for item in matched_vfolders:
                log.debug('Matched vFolder: {}, {}, {}', item['name'], item['group'], item['user'])
                if item['group'] is not None and item['group'] != str(group_id):
                    # User's accessible group vfolders should not be mounted
                    # if not belong to the execution kernel.
                    continue
                if item['name'] == '.local' and item['group'] is not None:
                    mount_prefix = await self.config_server.get('volumes/_mount')
                    fs_prefix = await self.config_server.get('volumes/_fsprefix')
                    folder_path = (Path(mount_prefix) / item['host'] /
                                   fs_prefix.lstrip('/') / item['id'].hex / user_uuid.hex)
                    loop = current_loop()
                    mkdir_lambda = lambda: folder_path.mkdir(parents=True, exist_ok=True)
                    await loop.run_in_executor(None, mkdir_lambda)
                    matched_mounts.add(item['name'])
                    determined_mounts.append((
                        item['name'],
                        item['host'],
                        f'{item["id"].hex}/{user_uuid.hex}',
                        item['permission'].value,
                        ''
                    ))
                else:
                    matched_mounts.add(item['name'])
                    determined_mounts.append((
                        item['name'],
                        item['host'],
                        item['id'].hex,
                        item['permission'].value,
                        item['unmanaged_path'] if item['unmanaged_path'] else '',
                    ))
            if mounts and set(mounts) > matched_mounts:
                raise VFolderNotFound
            creation_config['mounts'] = determined_mounts
        mounts = determined_mounts

        # TODO: merge into a single call
        image_info = await self.config_server.inspect_image(image_ref)
        image_min_slots, image_max_slots = \
            await self.config_server.get_image_slot_ranges(image_ref)
        known_slot_types = await self.config_server.get_resource_slots()

        # Shared memory.
        # We need to subtract the amount of shared memory from the memory limit of
        # a container, since tmpfs including /dev/shm uses host-side kernel memory
        # and cgroup's memory limit does not apply.
        shmem = resource_opts.get('shmem', None)
        if shmem is None:
            shmem = image_info['labels'].get('ai.backend.resource.preferred.shmem', '64m')
        shmem = BinarySize.from_str(shmem)
        resource_opts['shmem'] = shmem
        image_min_slots = copy.deepcopy(image_min_slots)
        image_min_slots['mem'] += shmem

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

            # If intrinsic resources are not specified,
            # fill them with image minimums.
            for k, v in requested_slots.items():
                if (v is None or v == 0) and k in INTRINSIC_SLOTS:
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
        log.debug('resource_opts: {}', resource_opts)
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

        kernel_id = uuid.uuid4()
        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_ENQUEUE_SESSION',
            (KernelId(kernel_id), session_name, access_key),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook(hook_result.src_plugin, hook_result.reason)

        # Create kernel object in PENDING state.
        async with self.dbpool.acquire() as conn, conn.begin():
            # Feed SSH keypair and dotfiles if exists.
            query = (sa.select([keypairs.c.ssh_public_key,
                                keypairs.c.ssh_private_key,
                                keypairs.c.dotfiles])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == access_key))
            result = await conn.execute(query)
            row  = await result.fetchone()
            dotfiles = msgpack.unpackb(row['dotfiles'])
            internal_data = {} if internal_data is None else internal_data
            internal_data.update({'dotfiles': dotfiles})
            if row['ssh_public_key'] and row['ssh_private_key']:
                internal_data['ssh_keypair'] = {
                    'public_key': row['ssh_public_key'],
                    'private_key': row['ssh_private_key'],
                }

            query = kernels.insert().values({
                'id': kernel_id,
                'status': KernelStatus.PENDING,
                'sess_id': session_name,    # TODO: rename column to session_name
                'sess_type': session_type,  # TODO: rename column to session_type
                'starts_at': starts_at,
                'role': 'master',
                'scaling_group': scaling_group,
                'domain_name': domain_name,
                'group_id': group_id,
                'user_uuid': user_uuid,
                'access_key': access_key,
                'image': image_ref.canonical,
                'registry': image_ref.registry,
                'tag': session_tag,
                'internal_data': internal_data,
                'startup_command': startup_command,
                'occupied_slots': requested_slots,
                'occupied_shares': {},
                'resource_opts': resource_opts,
                'environ': [f'{k}={v}' for k, v in environ.items()],
                'mounts': [list(mount) for mount in mounts],  # postgres save tuple as str
                'mount_map': mount_map,
                'bootstrap_script': bootstrap_script,
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
                'preopen_ports': preopen_ports,
            })
            await conn.execute(query)

        await self.hook_plugin_ctx.notify(
            'POST_ENQUEUE_SESSION',
            (KernelId(kernel_id), session_name, access_key),
        )
        await self.event_dispatcher.produce_event('kernel_enqueued', [str(kernel_id)])
        return KernelId(kernel_id)

    async def start_session(
        self,
        sched_ctx: SchedulingContext,
        sess_ctx: PendingSession,
        agent_ctx: AgentAllocationContext,
    ) -> None:
        auto_pull = await self.config_server.get('config/docker/image/auto_pull')
        image_info = await self.config_server.inspect_image(sess_ctx.image_ref)
        registry_url, registry_creds = \
            await get_registry_info(self.config_server.etcd,
                                    sess_ctx.image_ref.registry)
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (
                sa.select([keypair_resource_policies])
                .select_from(keypair_resource_policies)
                .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
            )
            result = await conn.execute(query)
            resource_policy = await result.first()

        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_START_SESSION',
            (sess_ctx.kernel_id, sess_ctx.session_name, sess_ctx.access_key),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook(hook_result.src_plugin, hook_result.reason)

        kernel_creation_id = secrets.token_urlsafe(16)
        start_event = asyncio.Event()
        self.kernel_creation_tracker[
            (kernel_creation_id, sess_ctx.kernel_id)
        ] = start_event
        try:
            # Create the kernel by invoking the agent
            async with self.handle_kernel_exception(
                'create_session', sess_ctx.kernel_id, sess_ctx.access_key,
            ):
                created_info = None
                async with RPCContext(
                    agent_ctx.agent_addr, None, order_key=sess_ctx.session_name,
                ) as rpc:
                    config: KernelCreationConfig = {
                        'image': {
                            'registry': {
                                'name': sess_ctx.image_ref.registry,
                                'url': str(registry_url),
                                **registry_creds,   # type: ignore
                            },
                            'digest': image_info['digest'],
                            'repo_digest': None,
                            'canonical': sess_ctx.image_ref.canonical,
                            'labels': image_info['labels'],
                        },
                        'session_type': sess_ctx.session_type.value,
                        'resource_slots': sess_ctx.requested_slots.to_json(),
                        'idle_timeout': resource_policy['idle_timeout'],
                        'mounts': sess_ctx.mounts,
                        'mount_map': sess_ctx.mount_map,
                        'environ': sess_ctx.environ,
                        'resource_opts': sess_ctx.resource_opts,
                        'bootstrap_script': sess_ctx.bootstrap_script,
                        'startup_command': sess_ctx.startup_command,
                        'internal_data': sess_ctx.internal_data,
                        'auto_pull': auto_pull,
                        'preopen_ports': sess_ctx.preopen_ports,
                    }
                    created_info = await rpc.call.create_kernel(
                        str(sess_ctx.kernel_id),
                        kernel_creation_id,
                        config,
                    )
                if created_info is None:
                    raise KernelCreationFailed('ooops')

            log.debug('start_session(s:{}, ak:{}, k:{}) -> created on ag:{}\n{!r}',
                      sess_ctx.session_name, sess_ctx.access_key, sess_ctx.kernel_id,
                      agent_ctx.agent_id, created_info)
            assert str(sess_ctx.kernel_id) == created_info['id']
            # Wait until the kernel_started event.
            await start_event.wait()
        finally:
            del self.kernel_creation_tracker[
                (kernel_creation_id, sess_ctx.kernel_id)
            ]

        await self.hook_plugin_ctx.notify(
            'POST_START_SESSION',
            (sess_ctx.kernel_id, sess_ctx.session_name, sess_ctx.access_key),
        )

        async with self.dbpool.acquire() as conn, conn.begin():
            # Return and record kernel access information
            agent_host = URL(agent_ctx.agent_addr).host
            kernel_host = created_info.get('kernel_host', agent_host)
            service_ports = created_info.get('service_ports', [])
            # NOTE: created_info contains resource_spec
            query = (
                kernels.update()
                .values({
                    'scaling_group': agent_ctx.scaling_group,
                    'status': KernelStatus.RUNNING,
                    'container_id': created_info['container_id'],
                    'occupied_shares': {},
                    'attached_devices': created_info.get('attached_devices', {}),
                    'kernel_host': kernel_host,
                    'repl_in_port': created_info['repl_in_port'],
                    'repl_out_port': created_info['repl_out_port'],
                    'stdin_port': created_info['stdin_port'],
                    'stdout_port': created_info['stdout_port'],
                    'service_ports': service_ports,
                })
                .where(kernels.c.id == sess_ctx.kernel_id))
            await conn.execute(query)

    async def get_keypair_occupancy(self, access_key, *, conn=None):
        known_slot_types = \
            await self.config_server.get_resource_slots()
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (
                sa.select([kernels.c.occupied_slots])
                .where(
                    (kernels.c.access_key == access_key) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
            )
            zero = ResourceSlot()
            key_occupied = sum([
                row['occupied_slots']
                async for row in conn.execute(query)], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def get_domain_occupancy(self, domain_name, *, conn=None):
        # TODO: store domain occupied_slots in Redis?
        known_slot_types = await self.config_server.get_resource_slots()
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (
                sa.select([kernels.c.occupied_slots])
                .where(
                    (kernels.c.domain_name == domain_name) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
            )
            zero = ResourceSlot()
            key_occupied = sum([row['occupied_slots'] async for row in conn.execute(query)], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def get_group_occupancy(self, group_id, *, conn=None):
        # TODO: store domain occupied_slots in Redis?
        known_slot_types = await self.config_server.get_resource_slots()
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (
                sa.select([kernels.c.occupied_slots])
                .where(
                    (kernels.c.group_id == group_id) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
            )
            zero = ResourceSlot()
            key_occupied = sum([row['occupied_slots'] async for row in conn.execute(query)], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def recalc_resource_usage(self) -> None:
        concurrency_used_per_key: MutableMapping[str, int] = defaultdict(lambda: 0)
        occupied_slots_per_agent: MutableMapping[str, ResourceSlot] = \
            defaultdict(lambda: ResourceSlot({'cpu': 0, 'mem': 0}))
        async with self.dbpool.acquire() as conn, conn.begin():
            # Query running containers and calculate concurrency_used per AK and
            # occupied_slots per agent.
            query = (sa.select([kernels.c.access_key, kernels.c.agent, kernels.c.occupied_slots])
                       .where(kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                       .order_by(sa.asc(kernels.c.access_key)))
            async for row in conn.execute(query):
                occupied_slots_per_agent[row.agent] += ResourceSlot(row.occupied_slots)
            query = (sa.select([kernels.c.access_key, kernels.c.agent, kernels.c.occupied_slots])
                     .where(kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                     .order_by(sa.asc(kernels.c.access_key)))
            async for row in conn.execute(query):
                concurrency_used_per_key[row.access_key] += 1

            if len(concurrency_used_per_key) > 0:
                # Update concurrency_used for keypairs with running containers.
                for ak, used in concurrency_used_per_key.items():
                    query = (sa.update(keypairs)
                               .values(concurrency_used=used)
                               .where(keypairs.c.access_key == ak))
                    await conn.execute(query)
                # Update all other keypairs to have concurrency_used = 0.
                query = (sa.update(keypairs)
                           .values(concurrency_used=0)
                           .where(keypairs.c.concurrency_used != 0)
                           .where(sa.not_(keypairs.c.access_key.in_(concurrency_used_per_key.keys()))))
                await conn.execute(query)
            else:
                query = (sa.update(keypairs)
                           .values(concurrency_used=0)
                           .where(keypairs.c.concurrency_used != 0))
                await conn.execute(query)

            if len(occupied_slots_per_agent) > 0:
                # Update occupied_slots for agents with running containers.
                for aid, slots in occupied_slots_per_agent.items():
                    query = (sa.update(agents)
                               .values(occupied_slots=slots)
                               .where(agents.c.id == aid))
                    await conn.execute(query)
                # Update all other agents to have empty occupied_slots.
                query = (sa.update(agents)
                           .values(occupied_slots=ResourceSlot({}))
                           .where(agents.c.status == AgentStatus.ALIVE)
                           .where(sa.not_(agents.c.id.in_(occupied_slots_per_agent.keys()))))
                await conn.execute(query)
            else:
                query = (sa.update(agents)
                           .values(occupied_slots=ResourceSlot({}))
                           .where(agents.c.status == AgentStatus.ALIVE))
                await conn.execute(query)

    async def destroy_session(
        self,
        sess_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        *,
        forced: bool = False,
        domain_name: str = None,
    ) -> Mapping[str, Any]:

        async with self.dbpool.acquire() as conn:
            kernel = await self.get_session(
                sess_name_or_id, access_key,
                field=[kernels.c.domain_name, kernels.c.role],
                db_connection=conn,
                for_update=True,
            )
            if domain_name is not None and kernel.domain_name != domain_name:
                raise SessionNotFound

            if forced:
                # This is for emergency (e.g., when agents are not responding).
                # Regardless of the container's real status, it marks the session terminated
                # and recalculate the resource usage.
                if kernel.status == KernelStatus.PENDING:
                    await self.set_session_status(
                        kernel['id'],
                        access_key,
                        KernelStatus.CANCELLED,
                        reason='force-cancelled',
                        db_connection=conn,
                    )
                    await self.event_dispatcher.produce_event(
                        'kernel_cancelled',
                        (str(kernel.id), 'force-terminated'),
                    )
                    return {'status': 'cancelled'}
                elif kernel.status in (KernelStatus.PREPARING, KernelStatus.PULLING):
                    raise GenericForbidden('Cannot destroy kernels in preparing/pulling status')
                if kernel.status not in (KernelStatus.ERROR, KernelStatus.TERMINATING):
                    # This is allowed, but if agents are working normally,
                    # the session will become invisible and unaccessible but STILL occupy the actual
                    # resources until the super-admin manually kills & deletes the container.
                    log.warning('force-terminating session in normal status! (k:{}, status:{})',
                                kernel.id, kernel.status)
                await self.set_session_status(
                    kernel['id'],
                    access_key,
                    KernelStatus.TERMINATED,
                    reason='force-terminated',
                    db_connection=conn,
                )
                await self.event_dispatcher.produce_event(
                    'kernel_terminated',
                    (str(kernel.id), 'terminated'),
                )
                # We intentionally skip the agent RPC call!
                await self.recalc_resource_usage()
                return {'status': 'terminated'}

        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_DESTROY_SESSION',
            (kernel['id'], kernel['sess_id'], kernel['access_key']),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook(hook_result.src_plugin, hook_result.reason)

        async with self.handle_kernel_exception(
            'destroy_session', kernel.id, access_key, set_error=True,
        ):
            async with self.dbpool.acquire() as conn, conn.begin():
                if domain_name is not None and kernel.domain_name != domain_name:
                    raise SessionNotFound
                if kernel.status == KernelStatus.PENDING:
                    await self.set_session_status(
                        kernel.id, access_key,
                        KernelStatus.CANCELLED,
                        reason='user-requested',
                        db_connection=conn,
                    )
                    await self.event_dispatcher.produce_event(
                        'kernel_cancelled',
                        (str(kernel.id), 'user-requested'),
                    )
                    return {'status': 'cancelled'}
                elif kernel.status in (KernelStatus.PREPARING, KernelStatus.PULLING):
                    raise GenericForbidden('Cannot destroy kernels in preparing/pulling status')
                else:
                    if kernel.role == 'master':
                        # The master session is terminated; decrement the user's concurrency counter
                        query = (sa.update(keypairs)
                                   .values(concurrency_used=keypairs.c.concurrency_used - 1)
                                   .where(keypairs.c.access_key == kernel.access_key))
                        await conn.execute(query)
                    await self.set_session_status(
                        kernel.id, access_key,
                        KernelStatus.TERMINATING,
                        reason='user-requested',
                        db_connection=conn,
                    )
                    await self.event_dispatcher.produce_event(
                        'kernel_terminating',
                        (str(kernel.id), 'user-requested'),
                    )
            async with RPCContext(kernel['agent_addr'], None, order_key=kernel.id) as rpc:
                await rpc.call.destroy_kernel(str(kernel['id']), 'user-requested')
                last_stat: Optional[Dict[str, Any]]
                last_stat = None
                try:
                    raw_last_stat = await redis.execute_with_retries(
                        lambda: self.redis_stat.get(str(kernel['id']), encoding=None),
                        max_retries=3)
                    if raw_last_stat is not None:
                        last_stat = msgpack.unpackb(raw_last_stat)
                        last_stat['version'] = 2
                except asyncio.TimeoutError:
                    pass

        await self.hook_plugin_ctx.notify(
            'POST_DESTROY_SESSION',
            (kernel['id'], kernel['sess_id'], kernel['access_key']),
        )
        return {
            **(last_stat if last_stat is not None else {}),
            'status': 'terminated',
        }

    async def restart_session(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> None:
        extra_cols = (
            kernels.c.image,
            kernels.c.registry,
            kernels.c.occupied_slots,
            kernels.c.environ,
        )
        async with self.dbpool.acquire() as conn, conn.begin():
            kernel = await self.get_session(
                session_name_or_id,
                access_key,
                field=extra_cols,
                db_connection=conn,
                for_update=True,
            )
            query = (
                kernels.update()
                .values({
                    'status': KernelStatus.RESTARTING,
                })
                .where(kernels.c.id == kernel['id'])
            )
            await conn.execute(query)
        kernel_creation_id = secrets.token_urlsafe(16)
        start_event = asyncio.Event()
        self.kernel_creation_tracker[
            (kernel_creation_id, kernel['id'])
        ] = start_event
        try:
            async with self.handle_kernel_exception(
                'restart_session', kernel['id'], access_key, set_error=True,
            ):
                registry_url, registry_creds = \
                    await get_registry_info(self.config_server.etcd,
                                            kernel['registry'])
                image_ref = ImageRef(kernel['image'], [kernel['registry']])
                image_info = await self.config_server.inspect_image(image_ref)
                async with RPCContext(
                    kernel['agent_addr'], None, order_key=kernel['id'],
                ) as rpc:
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
                    kernel_info = await rpc.call.restart_kernel(
                        kernel_creation_id,
                        str(kernel['id']),
                        new_config,
                    )
            await start_event.wait()
        finally:
            del self.kernel_creation_tracker[
                (kernel_creation_id, kernel['id'])
            ]
        await self.set_session_status(
            kernel['id'],
            access_key,
            KernelStatus.RUNNING,
            container_id=kernel_info['container_id'],
            repl_in_port=kernel_info['repl_in_port'],
            repl_out_port=kernel_info['repl_out_port'],
            stdin_port=kernel_info['stdin_port'],
            stdout_port=kernel_info['stdout_port'],
            service_ports=kernel_info.get('service_ports', []),
        )
        # NOTE: If the restarted session is a batch-type one, then the startup command
        #       will be executed again after restart.

    async def execute(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        api_version: Tuple[int, str],
        run_id: str,
        mode: str,
        code: str,
        opts: Mapping[str, Any],
        *,
        flush_timeout: float = None,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            # The agent aggregates at most 2 seconds of outputs
            # if the kernel runs for a long time.
            major_api_version = api_version[0]
            if major_api_version == 4:  # manager-agent protocol is same.
                major_api_version = 3
            async with RPCContext(kernel['agent_addr'], 30, order_key=kernel['id']) as rpc:
                return await rpc.call.execute(
                    str(kernel['id']),
                    major_api_version,
                    run_id, mode, code, opts,
                    flush_timeout,
                )

    async def interrupt_session(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], 30, order_key=kernel['id']) as rpc:
                return await rpc.call.interrupt_kernel(str(kernel['id']))

    async def get_completions(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        mode: str,
        text: str,
        opts: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], 10, order_key=kernel['id']) as rpc:
                return await rpc.call.get_completions(str(kernel['id']), mode, text, opts)

    async def start_service(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        service: str,
        opts: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], None, order_key=kernel['id']) as rpc:
                return await rpc.call.start_service(str(kernel['id']), service, opts)

    async def shutdown_service(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        service: str
    ) -> None:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('shutdown_service', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], None, order_key=kernel['id']) as rpc:
                return await rpc.call.shutdown_service(str(kernel['id']), service)

    async def upload_file(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        filename: str,
        payload: bytes,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('upload_file', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], None, order_key=kernel['id']) as rpc:
                return await rpc.call.upload_file(str(kernel['id']), filename, payload)

    async def download_file(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        filepath: str,
    ) -> bytes:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('download_file', kernel['id'],
                                                access_key):
            async with RPCContext(kernel['agent_addr'], None, order_key=kernel['id']) as rpc:
                return await rpc.call.download_file(str(kernel['id']), filepath)

    async def list_files(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        path: str,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('list_files', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], 30, order_key=kernel['id']) as rpc:
                return await rpc.call.list_files(str(kernel['id']), path)

    async def get_logs_from_agent(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('get_logs_from_agent', kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], 30, order_key=kernel['id']) as rpc:
                return await rpc.call.get_logs(str(kernel['id']))

    async def refresh_session(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('refresh_session',
                                                kernel['id'], access_key):
            async with RPCContext(kernel['agent_addr'], 30, order_key=kernel['id']) as rpc:
                return await rpc.call.refresh_idle(str(kernel['id']))

    async def increment_session_usage(self, sess_id, access_key, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.update(kernels)
                       .values(num_queries=kernels.c.num_queries + 1)
                       .where((kernels.c.sess_id == sess_id) &
                              (kernels.c.access_key == access_key) &
                              (kernels.c.role == 'master')))
            await conn.execute(query)

    async def kill_all_sessions_in_agent(self, agent_addr):
        async with RPCContext(agent_addr, None) as rpc:
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
        async with self.heartbeat_lock:

            instance_rejoin = False

            # Update "last seen" timestamp for liveness tracking
            await self.redis_live.hset('last_seen', agent_id, now.timestamp())

            # Check and update status of the agent record in DB
            async with self.dbpool.acquire() as conn, conn.begin():
                query = (sa.select([agents.c.status,
                                    agents.c.addr,
                                    agents.c.scaling_group,
                                    agents.c.available_slots],
                                   for_update=True)
                           .select_from(agents)
                           .where(agents.c.id == agent_id))
                result = await conn.execute(query)
                row = await result.first()

                slot_key_and_units = {
                    SlotName(k): SlotTypes(v[0]) for k, v in
                    agent_info['resource_slots'].items()}
                available_slots = ResourceSlot({
                    SlotName(k): v[1] for k, v in
                    agent_info['resource_slots'].items()})
                current_addr = agent_info['addr']
                sgroup = agent_info.get('scaling_group', 'default')

                if row is None or row['status'] is None:
                    # new agent detected!
                    log.info('agent {0} joined!', agent_id)
                    await self.config_server.update_resource_slots(slot_key_and_units)
                    query = agents.insert().values({
                        'id': agent_id,
                        'status': AgentStatus.ALIVE,
                        'region': agent_info['region'],
                        'scaling_group': sgroup,
                        'available_slots': available_slots,
                        'occupied_slots': {},
                        'addr': agent_info['addr'],
                        'first_contact': now,
                        'lost_at': None,
                        'version': agent_info['version'],
                        'compute_plugins': agent_info['compute_plugins'],
                    })
                    result = await conn.execute(query)
                    assert result.rowcount == 1
                elif row['status'] == AgentStatus.ALIVE:
                    updates = {}
                    if row['available_slots'] != available_slots:
                        updates['available_slots'] = available_slots
                    if row['scaling_group'] != sgroup:
                        updates['scaling_group'] = sgroup
                    if row['addr'] != current_addr:
                        updates['addr'] = current_addr
                    updates['version'] = agent_info['version']
                    updates['compute_plugins'] = agent_info['compute_plugins']
                    # occupied_slots are updated when kernels starts/terminates
                    if updates:
                        await self.config_server.update_resource_slots(slot_key_and_units)
                        query = (sa.update(agents)
                                   .values(updates)
                                   .where(agents.c.id == agent_id))
                        await conn.execute(query)
                elif row['status'] in (AgentStatus.LOST, AgentStatus.TERMINATED):
                    await self.config_server.update_resource_slots(slot_key_and_units)
                    instance_rejoin = True
                    query = (sa.update(agents)
                               .values({
                                   'status': AgentStatus.ALIVE,
                                   'region': agent_info['region'],
                                   'scaling_group': sgroup,
                                   'addr': agent_info['addr'],
                                   'lost_at': None,
                                   'available_slots': available_slots,
                                   'version': agent_info['version'],
                                   'compute_plugins': agent_info['compute_plugins'],
                               })
                               .where(agents.c.id == agent_id))
                    await conn.execute(query)
                else:
                    log.error('should not reach here! {0}', type(row.status))

            if instance_rejoin:
                await self.event_dispatcher.produce_event(
                    'instance_started', ('revived', ),
                    agent_id=agent_id)

            # Update the mapping of kernel images to agents.
            known_registries = await get_known_registries(self.config_server.etcd)
            images = msgpack.unpackb(snappy.decompress(agent_info['images']))

            def _pipe_builder():
                pipe = self.redis_image.pipeline()
                for image in images:
                    image_ref = ImageRef(image[0], known_registries)
                    pipe.sadd(image_ref.canonical, agent_id)
                return pipe
            await redis.execute_with_retries(_pipe_builder)

        await self.hook_plugin_ctx.notify(
            'POST_AGENT_HEARTBEAT',
            (agent_id, sgroup, available_slots),
        )

    async def mark_agent_terminated(self, agent_id, status, conn=None):
        global agent_peers
        await self.redis_live.hdel('last_seen', agent_id)

        async def _pipe_builder():
            pipe = self.redis_image.pipeline()
            async for imgname in self.redis_image.iscan():
                pipe.srem(imgname, agent_id)
            return pipe
        await redis.execute_with_retries(_pipe_builder)

        async with reenter_txn(self.dbpool, conn) as conn:

            query = (
                sa.select([
                    agents.c.status,
                    agents.c.addr,
                ], for_update=True)
                .select_from(agents)
                .where(agents.c.id == agent_id)
            )
            result = await conn.execute(query)
            row = await result.first()
            peer = agent_peers.pop(row['addr'], None)
            if peer is not None:
                await peer.__aexit__(None, None, None)
            prev_status = row['status']
            if prev_status in (None, AgentStatus.LOST, AgentStatus.TERMINATED):
                return

            if status == AgentStatus.LOST:
                log.warning('agent {0} heartbeat timeout detected.', agent_id)
            elif status == AgentStatus.TERMINATED:
                log.info('agent {0} has terminated.', agent_id)
            now = datetime.now(tzutc())
            query = (
                sa.update(agents)
                .values({
                    'status': status,
                    'status_changed': now,
                    'lost_at': now,
                })
                .where(agents.c.id == agent_id)
            )
            await conn.execute(query)

    async def set_session_status(
        self,
        session_id: SessionId,
        access_key: AccessKey,
        status: KernelStatus,
        reason: str = '', *,
        db_connection: SAConnection = None,
        **extra_fields,
    ) -> None:
        data = {
            'status': status,
            'status_info': reason,
            'status_changed': datetime.now(tzutc()),
        }
        data.update(extra_fields)
        async with reenter_txn(self.dbpool, db_connection) as conn:
            query = (
                sa.update(kernels)
                .values(data)
                .where(
                    (kernels.c.id == session_id) &
                    (kernels.c.access_key == access_key) &
                    ~(kernels.c.status.in_(DEAD_KERNEL_STATUSES))
                )
            )
            await conn.execute(query)

    async def set_kernel_status(self, kernel_id: KernelId,
                                status: KernelStatus,
                                reason: str = '', *,
                                db_conn: SAConnection = None):
        assert status != KernelStatus.TERMINATED, \
               'TERMINATED status update must be handled in ' \
               'mark_kernel_terminated()'
        data = {
            'status': status,
            'status_info': reason,
            'status_changed': datetime.now(tzutc()),
        }
        async with reenter_txn(self.dbpool, db_conn) as conn:
            query = (
                sa.update(kernels)
                .values(data)
                .where(kernels.c.id == kernel_id)
            )
            await conn.execute(query)

    async def set_session_result(self, kernel_id: KernelId,
                                 success: bool,
                                 exit_code: int, *,
                                 db_conn: SAConnection = None):
        # TODO: store exit code?
        data = {
            'result': SessionResult.SUCCESS if success else SessionResult.FAILURE,
        }
        async with reenter_txn(self.dbpool, db_conn) as conn:
            query = (
                sa.update(kernels)
                .values(data)
                .where(kernels.c.id == kernel_id)
            )
            await conn.execute(query)

    async def sync_kernel_stats(
        self, kernel_ids: Sequence[KernelId], *,
        db_conn: SAConnection = None,
    ) -> None:
        per_kernel_updates = {}

        for kernel_id in kernel_ids:
            raw_kernel_id = str(kernel_id)
            log.debug('sync_kernel_stats(k:{})', kernel_id)
            updates = {}

            async def _get_kstats_from_redis():
                stat_type = await self.redis_stat.type(raw_kernel_id)
                if stat_type == 'string':
                    kern_stat = await self.redis_stat.get(raw_kernel_id, encoding=None)
                    if kern_stat is not None:
                        updates['last_stat'] = msgpack.unpackb(kern_stat)
                else:
                    kern_stat = await self.redis_stat.hgetall(raw_kernel_id)
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

            await redis.execute_with_retries(
                lambda: _get_kstats_from_redis(),
                max_retries=1,
            )
            if not updates:
                log.warning('sync_kernel_stats(k:{}): no statistics updates', kernel_id)
                continue
            per_kernel_updates[kernel_id] = updates

        async with reenter_txn(self.dbpool, db_conn) as conn:
            # TODO: update to use execute_batch() if aiopg supports it.
            for kernel_id, updates in per_kernel_updates.items():
                query = (sa.update(kernels)
                           .values(updates)
                           .where(kernels.c.id == kernel_id))
                await conn.execute(query)

    async def mark_kernel_terminated(self, kernel_id: KernelId,
                                     reason: str,
                                     exit_code: int = None) -> None:
        '''
        Mark the kernel (individual worker) terminated and release
        the resource slots occupied by it.
        '''
        async with self.dbpool.acquire() as conn, conn.begin():
            # Check the current status.
            query = (
                sa.select([
                    kernels.c.access_key,
                    kernels.c.agent,
                    kernels.c.status,
                    kernels.c.occupied_slots
                ], for_update=True)
                .select_from(kernels)
                .where(kernels.c.id == kernel_id)
            )
            result = await conn.execute(query)
            kernel = await result.first()
            if (
                kernel is None
                or kernel['status'] in (
                    KernelStatus.CANCELLED,
                    KernelStatus.TERMINATED,
                    KernelStatus.RESTARTING,
                )
            ):
                # Skip if non-existent, already terminated, or restarting.
                return

            # Change the status to TERMINATED.
            # (we don't delete the row for later logging and billing)
            now = datetime.now(tzutc())
            query = (
                sa.update(kernels)
                .values({
                    'status': KernelStatus.TERMINATED,
                    'status_info': reason,
                    'status_changed': now,
                    'terminated_at': now,
                })
                .where(kernels.c.id == kernel_id)
            )
            await conn.execute(query)
            await recalc_concurrency_used(conn, kernel['access_key'])

            # Release agent resource slots.
            query = (
                sa.select([
                    agents.c.occupied_slots,
                ], for_update=True)
                .select_from(agents)
                .where(agents.c.id == kernel['agent'])
            )
            result = await conn.execute(query)
            agent = await result.first()
            if agent is None:
                return
            updated_occupied_slots = \
                agent['occupied_slots'] - kernel['occupied_slots']
            query = (
                sa.update(agents)
                .values({
                    'occupied_slots': updated_occupied_slots,
                })
                .where(agents.c.id == kernel['agent'])
            )
            await conn.execute(query)

        # Perform statistics sync in a separate transaction block, since
        # it may take a while to fetch stats from Redis.
        async with self.dbpool.acquire() as conn, conn.begin():
            await self.sync_kernel_stats([kernel_id], db_conn=conn)
