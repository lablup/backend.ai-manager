from __future__ import annotations

import asyncio
from contextvars import ContextVar
from collections import defaultdict
import copy
from datetime import datetime
from decimal import Decimal
import itertools
import logging
import secrets
import time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Container,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
    cast,
)
import uuid
import weakref

import aiodocker
import aiohttp
import aiotools
from aioredis import Redis
from async_timeout import timeout as _timeout
from callosum.rpc import Peer, RPCUserError
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from dateutil.tz import tzutc
import snappy
import sqlalchemy as sa
from sqlalchemy.sql.expression import true
from yarl import URL

from ai.backend.common import msgpack, redis
from ai.backend.common.docker import get_registry_info, get_known_registries, ImageRef
from ai.backend.common.events import (
    AgentStartedEvent,
    KernelCancelledEvent,
    KernelTerminatedEvent,
    KernelTerminatingEvent,
    SessionCancelledEvent,
    SessionEnqueuedEvent,
    SessionStartedEvent,
    SessionTerminatedEvent,
)
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.plugin.hook import (
    HookPluginContext,
    ALL_COMPLETED,
    PASSED,
)
from ai.backend.common.service_ports import parse_service_ports
from ai.backend.common.types import (
    AccessKey,
    AgentId,
    BinarySize,
    ClusterInfo,
    ClusterMode,
    ClusterSSHKeyPair,
    HardwareMetadata,
    KernelEnqueueingConfig,
    KernelId,
    ResourceSlot,
    SessionId,
    SessionResult,
    SessionTypes,
    SlotName,
    SlotTypes,
    check_typed_dict,
)
from ai.backend.common.utils import nmget

from .api.exceptions import (
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
from .config import SharedConfig
from .exceptions import MultiAgentError
from .defs import DEFAULT_ROLE, INTRINSIC_SLOTS
from .types import SessionGetter
from .models import (
    agents, kernels, keypairs, vfolders,
    query_group_dotfiles, query_domain_dotfiles,
    keypair_resource_policies,
    AgentStatus, KernelStatus,
    query_accessible_vfolders, query_allowed_sgroups,
    recalc_agent_resource_occupancy,
    recalc_concurrency_used,
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
    USER_RESOURCE_OCCUPYING_KERNEL_STATUSES,
    DEAD_KERNEL_STATUSES,
)
from .models.kernel import match_session_ids, get_all_kernels, get_main_kernels
from .models.utils import ExtendedAsyncSAEngine, reenter_txn, execute_with_retry, sql_json_merge

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import (
        AsyncConnection as SAConnection,
    )
    from sqlalchemy.engine.row import Row

    from ai.backend.common.events import EventDispatcher, EventProducer

    from .models.storage import StorageSessionManager
    from .scheduler.types import (
        AgentAllocationContext,
        KernelAgentBinding,
        SchedulingContext,
        PendingSession,
    )

__all__ = ['AgentRegistry', 'InstanceNotFound']

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.registry'))

agent_peers: MutableMapping[str, PeerInvoker] = {}  # agent-addr to socket

_read_only_txn_opts = {
    'postgresql_readonly': True,
}


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
async def RPCContext(agent_id, addr, timeout=None, *, order_key: str = None):
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
        raise AgentError(agent_id, orig_exc.name, orig_exc.repr, orig_exc.args)
    except Exception:
        raise


async def cleanup_agent_peers():
    global agent_peers
    closing_tasks = []
    for addr, peer in agent_peers.items():
        closing_tasks.append(peer.__aexit__(None, None, None))
    await asyncio.gather(*closing_tasks, return_exceptions=True)


class AgentRegistry:
    """
    Provide a high-level API to create, destroy, and query the computation
    kernels.

    The registry is also responsible to implement our resource management
    policy, such as the limitation of maximum number of kernels per instance.
    """

    kernel_creation_tracker: Dict[KernelId, asyncio.Future]
    _post_kernel_creation_tasks: weakref.WeakValueDictionary[KernelId, asyncio.Task]

    def __init__(
        self,
        shared_config: SharedConfig,
        db: ExtendedAsyncSAEngine,
        redis_stat: Redis,
        redis_live: Redis,
        redis_image: Redis,
        event_dispatcher: EventDispatcher,
        event_producer: EventProducer,
        storage_manager:  StorageSessionManager,
        hook_plugin_ctx: HookPluginContext,
    ) -> None:
        self.shared_config = shared_config
        self.docker = aiodocker.Docker()
        self.db = db
        self.redis_stat = redis_stat
        self.redis_live = redis_live
        self.redis_image = redis_image
        self.event_dispatcher = event_dispatcher
        self.event_producer = event_producer
        self.storage_manager = storage_manager
        self.hook_plugin_ctx = hook_plugin_ctx
        self.kernel_creation_tracker = {}
        self._post_kernel_creation_tasks = weakref.WeakValueDictionary()

    async def init(self) -> None:
        self.heartbeat_lock = asyncio.Lock()

    async def shutdown(self) -> None:
        await cleanup_agent_peers()

    async def get_instance(self, inst_id: AgentId, field=None):
        async with self.db.begin_readonly() as conn:
            cols = [agents.c.id]
            if field is not None:
                cols.append(field)
            query = (sa.select(cols)
                       .select_from(agents)
                       .where(agents.c.id == inst_id))
            result = await conn.execute(query)
            row = result.first()
            if not row:
                raise InstanceNotFound(inst_id)
            return row

    async def enumerate_instances(self, check_shadow=True):

        async with self.db.begin_readonly() as conn:
            query = (sa.select('*').select_from(agents))
            if check_shadow:
                query = query.where(agents.c.status == AgentStatus.ALIVE)
            async for row in (await conn.stream(query)):
                yield row

    async def update_instance(self, inst_id, updated_fields):

        async def _update() -> None:
            async with self.db.begin() as conn:
                query = (
                    sa.update(agents)
                    .values(**updated_fields)
                    .where(agents.c.id == inst_id)
                )
                await conn.execute(query)

        await execute_with_retry(_update)

    async def gather_agent_hwinfo(self, instance_id: AgentId) -> Mapping[str, HardwareMetadata]:
        agent = await self.get_instance(instance_id, agents.c.addr)
        async with RPCContext(agent['id'], agent['addr'], None) as rpc:
            result = await rpc.call.gather_hwinfo()
            return {
                k: check_typed_dict(v, HardwareMetadata)  # type: ignore  # (python/mypy#9827)
                for k, v in result.items()
            }

    async def gather_storage_hwinfo(self, vfolder_host: str) -> HardwareMetadata:
        proxy_name, volume_name = self.storage_manager.split_host(vfolder_host)
        async with self.storage_manager.request(
            proxy_name, 'GET', 'volume/hwinfo',
            json={'volume': volume_name},
            raise_for_status=True,
        ) as (_, storage_resp):
            return check_typed_dict(
                await storage_resp.json(), HardwareMetadata,  # type: ignore  # (python/mypy#9827)
            )

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
            if set_error:
                await self.set_session_status(
                    session_id,
                    access_key,
                    KernelStatus.ERROR,
                    status_info=f'agent-error ({e!r})',
                    status_data={
                        "error": {
                            "src": "agent",
                            "agent_id": e.agent_id,
                            "name": e.exc_name,
                            "repr": e.exc_repr,
                        }
                    }
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
                    status_data={
                        "error": {
                            "src": "other",
                            "name": e.__class__.__name__,
                            "repr": repr(e)
                        }
                    }
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
        """
        Retrieve the kernel information from the given kernel ID.
        This ID is unique for all individual agent-spawned containers.

        If ``field`` is given, it extracts only the raw value of the given
        field, without wrapping it as Kernel object.
        If ``allow_stale`` is true, it skips checking validity of the kernel
        owner instance.
        """
        cols = [kernels.c.id, kernels.c.session_id,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key]
        if field == '*':
            cols = [sa.text('*')]
        elif isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.db, db_connection, _read_only_txn_opts) as conn:
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
            row = result.first()
            if row is None:
                raise SessionNotFound
            return row

    async def get_kernels(
        self,
        session_name_or_id: Union[str, uuid.UUID],
        access_key: str, *,
        field=None,
        allow_stale: bool = False,
        for_update: bool = False,
        db_connection: SAConnection = None,
        cluster_role: str = None,
    ) -> Sequence[sa.engine.Row]:
        """
        Retrieve the kernel information by kernel's ID, kernel's session UUID
        (session_id), or kernel's name (session_id) paired with access_key.
        If the session is composed of multiple containers, this will return
        every container information, unless field and role is specified by the caller.

        :param session_name_or_id: kernel's id, session_id (session name), or session_id.
        :param access_key: Access key used to create kernels.
        :param field: If given, it extracts only the raw value of the given field, without
                      wrapping it as Kernel object.
        :param allow_stale: If True, filter "inactive" kernels as well as "active" ones.
                            If False, filter "active" kernels only.
        :param for_update: Apply for_update during select query.
        :param db_connection: Database connection for reuse.
        :param cluster_role: Filter kernels by role. "main", "sub", or None (all).
        """
        cols = [
            kernels.c.id,
            kernels.c.session_id,
            kernels.c.session_name,
            kernels.c.session_type,
            kernels.c.status,
            kernels.c.cluster_mode,
            kernels.c.cluster_role,
            kernels.c.cluster_idx,
            kernels.c.access_key,
            kernels.c.agent_addr,
            kernels.c.kernel_host,
            kernels.c.image,
            kernels.c.registry,
            kernels.c.service_ports,
        ]
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
            (kernels.c.access_key == access_key)
        )
        cond_name = (
            (kernels.c.session_name.like(f'{session_name_or_id}%')) &
            (kernels.c.access_key == access_key)
        )
        cond_session_id = (
            (sa.sql.expression.cast(kernels.c.session_id, sa.String).like(f'{session_name_or_id}%')) &
            (kernels.c.access_key == access_key)
        )
        if cluster_role is not None:
            cond_id = cond_id & (kernels.c.cluster_role == cluster_role)
            cond_name = cond_name & (kernels.c.cluster_role == cluster_role)
            cond_session_id = cond_session_id & (kernels.c.cluster_role == cluster_role)
        if allow_stale:
            cond_status = true()  # any status
        else:
            cond_status = ~(kernels.c.status.in_(DEAD_KERNEL_STATUSES))
        query_by_id = (
            sa.select(cols)
            .select_from(kernels)
            .where(cond_id & cond_status)
            .order_by(sa.desc(kernels.c.created_at))
            .limit(10).offset(0)
        )
        if for_update:
            query_by_id = query_by_id.with_for_update()
        query_by_name = (
            sa.select(cols)
            .select_from(kernels)
            .where(cond_name & cond_status)
            .order_by(sa.desc(kernels.c.created_at))
        )
        if for_update:
            query_by_name = query_by_name.with_for_update()
        query_by_session_id = (
            sa.select(cols)
            .select_from(kernels)
            .where(cond_session_id & cond_status)
            .order_by(sa.desc(kernels.c.created_at))
            .limit(10).offset(0)
        )
        if for_update:
            query_by_session_id = query_by_session_id.with_for_update()
        if allow_stale:
            query_by_name = query_by_name.limit(10).offset(0)
        else:
            # for backward-compatibility
            query_by_name = query_by_name.limit(1).offset(0)

        async with reenter_txn(self.db, db_connection) as conn:
            for query in [
                query_by_id,
                query_by_session_id,
                query_by_name,
            ]:
                result = await conn.execute(query)
                if result.rowcount == 0:
                    continue
                return result.fetchall()
        raise SessionNotFound

    async def get_session_by_session_id(
        self,
        session_id: SessionId,
        *,
        db_connection: SAConnection,
        for_update: bool = False,
    ) -> sa.engine.Row:
        query = (
            sa.select(
                [sa.text('*')],
            )
            .select_from(kernels)
            .where(
                (kernels.c.session_id == session_id) &
                (kernels.c.cluster_role == DEFAULT_ROLE)
            )
        )
        if for_update:
            query = query.with_for_update()
        result = await db_connection.execute(query)
        row = result.first()
        if row is None:
            raise SessionNotFound
        return row

    async def get_session_by_kernel_id(
        self,
        kernel_id: KernelId,
        *,
        db_connection: SAConnection,
        for_update: bool = False,
    ) -> sa.engine.Row:
        query = (
            sa.select(
                [sa.text('*')],
            )
            .select_from(kernels)
            .where(
                (kernels.c.session_id == (
                    sa.select([kernels.c.session_id])
                    .select_from(kernels)
                    .where(kernels.c.id == kernel_id)
                )) &
                (kernels.c.cluster_role == DEFAULT_ROLE)
            )
        )
        if for_update:
            query = query.with_for_update()
        result = await db_connection.execute(query)
        row = result.first()
        if row is None:
            raise SessionNotFound
        return row

    async def get_session(
        self,
        session_name_or_id: Union[str, uuid.UUID],
        access_key: Union[str, AccessKey],
        *,
        allow_stale: bool = False,
        for_update: bool = False,
        db_connection: SAConnection = None,
    ) -> sa.engine.Row:
        """
        Retrieve the session information by kernel's ID, kernel's session UUID
        (session_id), or kernel's name (session_id) paired with access_key.
        If the session is composed of multiple containers, this will return
        the information of the main kernel.

        :param session_name_or_id: kernel's id, session_id (session name), or session_id.
        :param access_key: Access key used to create kernels.
        :param field: If given, it extracts only the raw value of the given field, without
                      wrapping it as Kernel object.
        :param allow_stale: If True, filter "inactive" kernels as well as "active" ones.
                            If False, filter "active" kernels only.
        :param for_update: Apply for_update during select query.
        :param db_connection: Database connection for reuse.
        :param cluster_role: Filter kernels by role. "main", "sub", or None (all).
        """
        async with reenter_txn(self.db, db_connection, _read_only_txn_opts) as conn:
            if allow_stale:
                extra_cond = None
            else:
                extra_cond = (~kernels.c.status.in_(DEAD_KERNEL_STATUSES))
            session_infos = await match_session_ids(
                session_name_or_id,
                AccessKey(access_key),
                for_update=for_update,
                extra_cond=extra_cond,
                db_connection=conn,
            )
            if not session_infos:
                raise SessionNotFound()
            if len(session_infos) > 1:
                raise TooManySessionsMatched(extra_data={'matches': session_infos})
            kernel_list = await get_main_kernels(
                [SessionId(session_infos[0]['session_id'])],
                db_connection=conn,
            )
            return kernel_list[0]

    async def get_session_kernels(
        self,
        session_id: str,
        access_key: str, *,
        field=None,
        allow_stale: bool = False,
        for_update: bool = False,
        db_connection: SAConnection = None,
        cluster_role: str = None,
    ) -> Sequence[sa.engine.Row]:
        """
        Retrieve the information of all kernels of a session by session UUID.
        If the session is bundled with multiple containers,
        this will return every information of them.

        :param session_id: Session's UUID.
        :param access_key: Access key used to create the session.
        :param field: If given, it extracts only the raw value of the given field, without
                      wrapping it as Kernel object.
        :param allow_stale: If True, filter "inactive" kernels as well as "active" ones.
                            If False, filter "active" kernels only.
        :param for_update: Apply for_update during select query.
        :param db_connection: Database connection for reuse.
        :param cluster_role: Filter kernels by role. "main", "sub", or None (all).
        """
        return await self.get_kernels(
            session_id, access_key,
            field=field, for_update=for_update,
            db_connection=db_connection,
            cluster_role=cluster_role,
        )

    async def get_sessions(
        self,
        session_names: Container[str],
        field=None,
        allow_stale=False,
        db_connection=None,
    ):
        """
        Batched version of :meth:`get_session() <AgentRegistry.get_session>`.
        The order of the returend array is same to the order of ``sess_ids``.
        For non-existent or missing kernel IDs, it fills None in their
        positions without raising SessionNotFound exception.
        """

        cols = [kernels.c.id, kernels.c.session_id,
                kernels.c.agent_addr, kernels.c.kernel_host, kernels.c.access_key,
                kernels.c.service_ports]
        if isinstance(field, (tuple, list)):
            cols.extend(field)
        elif isinstance(field, (sa.Column, sa.sql.elements.ColumnClause)):
            cols.append(field)
        elif isinstance(field, str):
            cols.append(sa.column(field))
        async with reenter_txn(self.db, db_connection, _read_only_txn_opts) as conn:
            if allow_stale:
                query = (sa.select(cols)
                           .select_from(kernels)
                           .where((kernels.c.session_id.in_(session_names)) &
                                  (kernels.c.cluster_role == DEFAULT_ROLE)))
            else:
                query = (sa.select(cols)
                           .select_from(kernels.join(agents))
                           .where((kernels.c.session_id.in_(session_names)) &
                                  (kernels.c.cluster_role == DEFAULT_ROLE) &
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  (agents.c.id == kernels.c.agent)))
            result = await conn.execute(query)
            rows = result.fetchall()
            return rows

    async def enqueue_session(
        self,
        session_creation_id: str,
        session_name: str,
        access_key: str,
        kernel_enqueue_configs: List[KernelEnqueueingConfig],
        scaling_group: Optional[str],
        session_type: SessionTypes,
        resource_policy: str,
        *,
        domain_name: str,
        group_id: uuid.UUID,
        user_uuid: uuid.UUID,
        user_role: str,
        cluster_mode: ClusterMode = ClusterMode.SINGLE_NODE,
        cluster_size: int = 1,
        startup_command: str = None,
        session_tag: str = None,
        internal_data: dict = None,
        starts_at: datetime = None,
    ) -> SessionId:

        mounts = kernel_enqueue_configs[0]['creation_config'].get('mounts') or []
        mount_map = kernel_enqueue_configs[0]['creation_config'].get('mount_map') or {}
        session_id = SessionId(uuid.uuid4())

        # Check scaling group availability if scaling_group parameter is given.
        # If scaling_group is not provided, it will be selected in scheduling step.
        if scaling_group is not None:
            async with self.db.begin_readonly() as conn:
                sgroups = await query_allowed_sgroups(conn, domain_name, group_id, access_key)
                for sgroup in sgroups:
                    if scaling_group == sgroup['name']:
                        break
                else:
                    raise ScalingGroupNotFound

        # sanity check for vfolders
        allowed_vfolder_types = ['user', 'group']
        # allowed_vfolder_types = await root_ctx.shared_config.etcd.get('path-to-vfolder-type')
        determined_mounts = []
        matched_mounts = set()
        async with self.db.begin_readonly() as conn:
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
            if item['group'] is not None and item['group'] != str(group_id):
                # User's accessible group vfolders should not be mounted
                # if not belong to the execution kernel.
                continue
            mount_path = await self.storage_manager.get_mount_path(item['host'], item['id'])
            if item['name'] == '.local' and item['group'] is not None:
                try:
                    async with self.storage_manager.request(
                        item['host'], 'POST', 'folder/file/mkdir',
                        params={
                            'volume': self.storage_manager.split_host(item['host'])[1],
                            'vfid': item['id'],
                            'relpath': str(user_uuid.hex)
                        },
                    ):
                        pass
                except aiohttp.ClientResponseError:
                    # the server may respond with error if the directory already exists
                    pass
                matched_mounts.add(item['name'])
                determined_mounts.append((
                    item['name'],
                    item['host'],
                    f"{mount_path}/{user_uuid.hex}",
                    item['permission'].value,
                    ''
                ))
            else:
                matched_mounts.add(item['name'])
                determined_mounts.append((
                    item['name'],
                    item['host'],
                    mount_path,
                    item['permission'].value,
                    item['unmanaged_path'] if item['unmanaged_path'] else '',
                ))
        if mounts and set(mounts) > matched_mounts:
            raise VFolderNotFound
        mounts = determined_mounts

        ids = []
        is_multicontainer = cluster_size > 1
        if is_multicontainer:
            if len(kernel_enqueue_configs) == 1:
                log.debug(
                    'enqueue_session(): replicating kernel_enqueue_config with cluster_size={}',
                    cluster_size,
                )
                # the first kernel_config is repliacted to sub-containers
                assert kernel_enqueue_configs[0]['cluster_role'] == DEFAULT_ROLE
                kernel_enqueue_configs[0]['cluster_idx'] = 1
                for i in range(cluster_size - 1):
                    sub_kernel_config = cast(KernelEnqueueingConfig, {**kernel_enqueue_configs[0]})
                    sub_kernel_config['cluster_role'] = 'sub'
                    sub_kernel_config['cluster_idx'] = i + 1
                    kernel_enqueue_configs.append(sub_kernel_config)
            elif len(kernel_enqueue_configs) > 1:
                # each container should have its own kernel_config
                log.debug(
                    'enqueue_session(): using given kernel_enqueue_configs with cluster_size={}',
                    cluster_size,
                )
                if len(kernel_enqueue_configs) != cluster_size:
                    raise InvalidAPIParameters(
                        "The number of kernel configs differs from the cluster size")
            else:
                raise InvalidAPIParameters("Missing kernel configurations")

        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_ENQUEUE_SESSION',
            (session_id, session_name, access_key),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook.from_hook_result(hook_result)

        for kernel in kernel_enqueue_configs:
            kernel_id: KernelId
            if kernel['cluster_role'] == DEFAULT_ROLE:
                kernel_id = cast(KernelId, session_id)
            else:
                kernel_id = KernelId(uuid.uuid4())
            creation_config = kernel['creation_config']
            image_ref = kernel['image_ref']
            resource_opts = creation_config.get('resource_opts') or {}

            creation_config['mounts'] = mounts
            # TODO: merge into a single call
            image_info = await self.shared_config.inspect_image(image_ref)
            image_min_slots, image_max_slots = \
                await self.shared_config.get_image_slot_ranges(image_ref)
            known_slot_types = await self.shared_config.get_resource_slots()

            # Parse service ports to check for port errors
            parse_service_ports(image_info['labels'].get('ai.backend.service-ports', ''), BackendError)

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
            log_fmt = "s:{} k:{} r:{}-{}"
            log_args = (session_id, kernel_id, kernel['cluster_role'], kernel['cluster_idx'])
            log.debug(log_fmt + ' -> requested_slots: {}', *log_args, requested_slots)
            log.debug(log_fmt + ' -> resource_opts: {}', *log_args, resource_opts)
            log.debug(log_fmt + ' -> image_min_slots: {}', *log_args, image_min_slots)
            log.debug(log_fmt + ' -> image_max_slots: {}', *log_args, image_max_slots)

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

            environ = kernel_enqueue_configs[0]['creation_config'].get('environ') or {}

            # Create kernel object in PENDING state.
            async with self.db.begin_readonly() as conn:
                # Feed SSH keypair and dotfiles if exists.
                query = (sa.select([keypairs.c.ssh_public_key,
                                    keypairs.c.ssh_private_key,
                                    keypairs.c.dotfiles])
                           .select_from(keypairs)
                           .where(keypairs.c.access_key == access_key))
                result = await conn.execute(query)
                row  = result.first()
                dotfiles = msgpack.unpackb(row['dotfiles'])
                internal_data = {} if internal_data is None else internal_data
                internal_data.update({'dotfiles': dotfiles})
                if row['ssh_public_key'] and row['ssh_private_key']:
                    internal_data['ssh_keypair'] = {
                        'public_key': row['ssh_public_key'],
                        'private_key': row['ssh_private_key'],
                    }
                # use dotfiles in the priority of keypair > group > domain
                dotfile_paths = set(map(lambda x: x['path'], dotfiles))
                # add keypair dotfiles
                internal_data.update({'dotfiles': list(dotfiles)})
                # add group dotfiles
                dotfiles, _ = await query_group_dotfiles(conn, group_id)
                for dotfile in dotfiles:
                    if dotfile['path'] not in dotfile_paths:
                        internal_data['dotfiles'].append(dotfile)
                        dotfile_paths.add(dotfile['path'])
                # add domain dotfiles
                dotfiles, _ = await query_domain_dotfiles(conn, domain_name)
                for dotfile in dotfiles:
                    if dotfile['path'] not in dotfile_paths:
                        internal_data['dotfiles'].append(dotfile)
                        dotfile_paths.add(dotfile['path'])
                # reverse the dotfiles list so that higher priority can overwrite
                # in case the actual path is the same
                internal_data['dotfiles'].reverse()

                # check if there is no name conflict of dotfile and vfolder
                for dotfile in internal_data.get('dotfiles', []):
                    if dotfile['path'].startswith('/'):
                        if dotfile['path'].startswith('/home/'):
                            path_arr = dotfile['path'].split('/')
                            # check if there is a dotfile whose path equals /home/work/vfolder_name
                            if len(path_arr) >= 3 and path_arr[2] == 'work' and \
                                    path_arr[3] in matched_mounts:
                                raise BackendError(
                                    f'There is a vfolder whose name conflicts with '
                                    f'dotfile {path_arr[3]} with path "{dotfile["path"]}"')
                    else:
                        if dotfile['path'] in matched_mounts:
                            raise BackendError(
                                f'There is a vfolder whose name conflicts with '
                                f'dotfile {dotfile["path"]}')

            async def _enqueue() -> None:
                nonlocal ids
                async with self.db.begin() as conn:
                    query = kernels.insert().values({
                        'id': kernel_id,
                        'status': KernelStatus.PENDING,
                        'session_creation_id': session_creation_id,
                        'session_id': session_id,
                        'session_name': session_name,
                        'session_type': session_type,
                        'cluster_mode': cluster_mode.value,
                        'cluster_size': cluster_size,
                        'cluster_role': kernel['cluster_role'],
                        'cluster_idx': kernel['cluster_idx'],
                        'cluster_hostname': f"{kernel['cluster_role']}{kernel['cluster_idx']}",
                        'scaling_group': scaling_group,
                        'domain_name': domain_name,
                        'group_id': group_id,
                        'user_uuid': user_uuid,
                        'access_key': access_key,
                        'image': image_ref.canonical,
                        'registry': image_ref.registry,
                        'tag': session_tag,
                        'starts_at': starts_at,
                        'internal_data': internal_data,
                        'startup_command': kernel.get('startup_command'),
                        'occupied_slots': requested_slots,
                        'occupied_shares': {},
                        'resource_opts': resource_opts,
                        'environ': [f'{k}={v}' for k, v in environ.items()],
                        'mounts': [list(mount) for mount in mounts],  # postgres save tuple as str
                        'mount_map': mount_map,
                        'bootstrap_script': kernel.get('bootstrap_script'),
                        'repl_in_port': 0,
                        'repl_out_port': 0,
                        'stdin_port': 0,
                        'stdout_port': 0,
                        'preopen_ports': creation_config.get('preopen_ports', []),
                    })
                    await conn.execute(query)
                    ids.append(kernel_id)

            await execute_with_retry(_enqueue)

        await self.hook_plugin_ctx.notify(
            'POST_ENQUEUE_SESSION',
            (session_id, session_name, access_key),
        )
        await self.event_producer.produce_event(
            SessionEnqueuedEvent(session_id, session_creation_id)
        )
        return session_id

    async def start_session(
        self,
        sched_ctx: SchedulingContext,
        scheduled_session: PendingSession,
    ) -> None:
        from .scheduler.types import KernelAgentBinding, AgentAllocationContext
        kernel_agent_bindings: Sequence[KernelAgentBinding] = [
            KernelAgentBinding(
                kernel=k,
                agent_alloc_ctx=AgentAllocationContext(
                    agent_id=k.agent_id,
                    agent_addr=k.agent_addr,
                    scaling_group=scheduled_session.scaling_group,
                ),
            )
            for k in scheduled_session.kernels
        ]
        session_creation_id = scheduled_session.session_creation_id

        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_START_SESSION',
            (scheduled_session.session_id, scheduled_session.session_name, scheduled_session.access_key),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook.from_hook_result(hook_result)

        # Get resource policy for the session
        # TODO: memoize with TTL
        async with self.db.begin_readonly() as conn:
            query = (
                sa.select([keypair_resource_policies])
                .select_from(keypair_resource_policies)
                .where(keypair_resource_policies.c.name == scheduled_session.resource_policy)
            )
            result = await conn.execute(query)
            resource_policy = result.first()
        auto_pull = await self.shared_config.get_raw('config/docker/image/auto_pull')

        # Aggregate image registry information
        keyfunc = lambda item: item.kernel.image_ref
        image_infos = {}
        for image_ref, _ in itertools.groupby(
            sorted(kernel_agent_bindings, key=keyfunc), key=keyfunc,
        ):
            image_infos[image_ref] = await self.shared_config.inspect_image(image_ref)
            registry_url, registry_creds = \
                await get_registry_info(self.shared_config.etcd, image_ref.registry)
        image_info = {
            'image_infos': image_infos,
            'registry_url': registry_url,
            'registry_creds': registry_creds,
            'resource_policy': resource_policy,
            'auto_pull': auto_pull,
        }

        network_name: Optional[str] = None
        if scheduled_session.cluster_mode == ClusterMode.SINGLE_NODE:
            if scheduled_session.cluster_size > 1:
                network_name = f'bai-singlenode-{scheduled_session.session_id}'
                try:
                    async with RPCContext(
                        kernel_agent_bindings[0].agent_alloc_ctx.agent_id,
                        kernel_agent_bindings[0].agent_alloc_ctx.agent_addr,
                        None,
                        order_key=scheduled_session.session_id,
                    ) as rpc:
                        await rpc.call.create_local_network(network_name)
                except Exception:
                    log.exception(f"Failed to create an agent-local network {network_name}")
                    raise
            else:
                network_name = None
        elif scheduled_session.cluster_mode == ClusterMode.MULTI_NODE:
            # Create overlay network for multi-node sessions
            network_name = f'bai-multinode-{scheduled_session.session_id}'
            try:
                # Overlay networks can only be created at the Swarm manager.
                await self.docker.networks.create({
                    'Name': network_name,
                    'Driver': 'overlay',
                    'Attachable': True,
                    'Labels': {
                        'ai.backend.cluster-network': '1'
                    }
                })
            except Exception:
                log.exception(f"Failed to create an overlay network {network_name}")
                raise
        keyfunc = lambda item: item.kernel.cluster_role
        replicas = {
            cluster_role: len([*group_iterator])
            for cluster_role, group_iterator in itertools.groupby(
                sorted(kernel_agent_bindings, key=keyfunc),
                key=keyfunc,
            )
        }
        cluster_info = ClusterInfo(
            mode=scheduled_session.cluster_mode,
            size=scheduled_session.cluster_size,
            replicas=replicas,
            network_name=network_name,
            ssh_keypair=(
                await self.create_cluster_ssh_keypair()
                if scheduled_session.cluster_size > 1 else None
            ),
        )
        scheduled_session.environ.update({
            'BACKENDAI_SESSION_ID': str(scheduled_session.session_id),
            'BACKENDAI_SESSION_NAME': str(scheduled_session.session_name),
            'BACKENDAI_CLUSTER_SIZE': str(scheduled_session.cluster_size),
            'BACKENDAI_CLUSTER_REPLICAS':
                ",".join(f"{k}:{v}" for k, v in replicas.items()),
            'BACKENDAI_CLUSTER_HOSTS':
                ",".join(binding.kernel.cluster_hostname for binding in kernel_agent_bindings),
            'BACKENDAI_ACCESS_KEY': scheduled_session.access_key,
        })

        # Aggregate by agents to minimize RPC calls
        per_agent_tasks = []

        keyfunc = lambda item: item.agent_alloc_ctx.agent_id
        for agent_id, group_iterator in itertools.groupby(
            sorted(kernel_agent_bindings, key=keyfunc), key=keyfunc,
        ):
            items = [*group_iterator]
            # Within a group, agent_alloc_ctx are same.
            agent_alloc_ctx = items[0].agent_alloc_ctx
            per_agent_tasks.append(
                (
                    agent_alloc_ctx,
                    self._create_kernels_in_one_agent(
                        agent_alloc_ctx,
                        scheduled_session,
                        items,
                        image_info,
                        cluster_info,
                    ),
                )
            )

        if per_agent_tasks:
            agent_errors = []
            results = await asyncio.gather(
                *[item[1] for item in per_agent_tasks],
                return_exceptions=True,
            )
            for agent_alloc_tx, result in zip((item[0] for item in per_agent_tasks), results):
                if isinstance(result, aiotools.MultiError):
                    agent_errors.extend(result.__errors__)
                elif isinstance(result, Exception):
                    # mark to be destroyed afterwards
                    agent_errors.append(result)
            if agent_errors:
                raise MultiAgentError(
                    "agent(s) raise errors during kernel creation",
                    errors=agent_errors,
                )

        # If all is well, let's say the session is ready.
        await self.event_producer.produce_event(
            SessionStartedEvent(scheduled_session.session_id, session_creation_id)
        )
        await self.hook_plugin_ctx.notify(
            'POST_START_SESSION',
            (scheduled_session.session_id, scheduled_session.session_name, scheduled_session.access_key),
        )

    async def _post_create_kernel(
        self,
        agent_alloc_ctx: AgentAllocationContext,
        created_info,
    ):
        # Wait until the kernel_started event.
        kernel_id = KernelId(uuid.UUID(created_info['id']))
        start_event = self.kernel_creation_tracker[kernel_id]
        try:
            await start_event
        except asyncio.CancelledError:
            log.warning("post_create_kernel(k:{}) cancelled", kernel_id)
            return
        # Record kernel access information

        async def _finialize_running() -> None:
            async with self.db.begin() as conn:
                agent_host = URL(agent_alloc_ctx.agent_addr).host
                kernel_host = created_info.get('kernel_host', agent_host)
                service_ports = created_info.get('service_ports', [])
                # NOTE: created_info contains resource_spec
                query = (
                    kernels.update()
                    .values({
                        'scaling_group': agent_alloc_ctx.scaling_group,
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
                    .where(kernels.c.id == created_info['id']))
                await conn.execute(query)

        await execute_with_retry(_finialize_running)

    async def _create_kernels_in_one_agent(
        self,
        agent_alloc_ctx: AgentAllocationContext,
        scheduled_session: PendingSession,
        items: Sequence[KernelAgentBinding],
        image_info,
        cluster_info,
    ) -> None:
        loop = asyncio.get_running_loop()
        registry_url = image_info['registry_url']
        registry_creds = image_info['registry_creds']
        image_infos = image_info['image_infos']
        resource_policy = image_info['resource_policy']
        auto_pull = image_info['auto_pull']
        async with RPCContext(
            agent_alloc_ctx.agent_id,
            agent_alloc_ctx.agent_addr,
            None,
            order_key=scheduled_session.session_id,
        ) as rpc:
            kernel_creation_id = secrets.token_urlsafe(16)
            # Prepare kernel_started event handling
            for binding in items:
                self.kernel_creation_tracker[
                    binding.kernel.kernel_id
                ] = loop.create_future()
            try:
                # Issue a batched RPC call to create kernels on this agent
                created_infos = await rpc.call.create_kernels(
                    kernel_creation_id,
                    str(scheduled_session.session_id),
                    [str(binding.kernel.kernel_id) for binding in items],
                    [
                        {
                            'image': {
                                'registry': {
                                    'name': binding.kernel.image_ref.registry,
                                    'url': str(registry_url),
                                    **registry_creds,   # type: ignore
                                },
                                'digest': image_infos[binding.kernel.image_ref]['digest'],
                                'repo_digest': None,
                                'canonical': binding.kernel.image_ref.canonical,
                                'labels': image_infos[binding.kernel.image_ref]['labels'],
                            },
                            'session_type': scheduled_session.session_type.value,
                            'cluster_role': binding.kernel.cluster_role,
                            'cluster_idx': binding.kernel.cluster_idx,
                            'cluster_hostname': binding.kernel.cluster_hostname,
                            'idle_timeout': resource_policy['idle_timeout'],
                            'mounts': scheduled_session.mounts,
                            'mount_map': scheduled_session.mount_map,
                            'environ': {
                                # inherit per-session environment variables
                                **scheduled_session.environ,
                                # set per-kernel environment variables
                                'BACKENDAI_KERNEL_ID': str(binding.kernel.kernel_id),
                                'BACKENDAI_KERNEL_IMAGE': str(binding.kernel.image_ref),
                                'BACKENDAI_CLUSTER_ROLE': binding.kernel.cluster_role,
                                'BACKENDAI_CLUSTER_IDX': str(binding.kernel.cluster_idx),
                                'BACKENDAI_CLUSTER_HOST': str(binding.kernel.cluster_hostname),
                            },
                            'resource_slots': binding.kernel.requested_slots.to_json(),
                            'resource_opts': binding.kernel.resource_opts,
                            'bootstrap_script': binding.kernel.bootstrap_script,
                            'startup_command': binding.kernel.startup_command,
                            'internal_data': scheduled_session.internal_data,
                            'auto_pull': auto_pull,
                            'preopen_ports': scheduled_session.preopen_ports,
                        }
                        for binding in items
                    ],
                    cluster_info,
                )
                log.debug(
                    'start_session(s:{}, ak:{}, k:{}) -> created on ag:{}',
                    scheduled_session.session_name,
                    scheduled_session.access_key,
                    [binding.kernel.kernel_id for binding in items],
                    agent_alloc_ctx.agent_id,
                )
                # Post-process kernel creation
                async with aiotools.TaskGroup() as tg:
                    for created_info in created_infos:
                        post_task = tg.create_task(self._post_create_kernel(
                            agent_alloc_ctx,
                            created_info,
                        ))
                        self._post_kernel_creation_tasks[created_info['id']] = post_task
            except Exception:
                # The agent has already cancelled or issued the destruction lifecycle event
                # for this batch of kernels.
                for binding in items:
                    start_event = self.kernel_creation_tracker[
                        binding.kernel.kernel_id
                    ]
                    start_event.cancel()
                raise
            finally:
                # clean up for sure
                for binding in items:
                    del self.kernel_creation_tracker[
                        binding.kernel.kernel_id
                    ]

    async def create_cluster_ssh_keypair(self) -> ClusterSSHKeyPair:
        key = rsa.generate_private_key(
            backend=default_backend(),
            public_exponent=65537,
            key_size=2048,
        )
        public_key = key.public_key().public_bytes(
            serialization.Encoding.OpenSSH,
            serialization.PublicFormat.OpenSSH,
        )
        public_key += b' work@cluster.backend.ai.local'
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return {
            'private_key': pem.decode('utf-8'),
            'public_key': public_key.decode('utf-8'),
        }

    async def get_keypair_occupancy(self, access_key, *, conn=None):
        known_slot_types = \
            await self.shared_config.get_resource_slots()
        async with reenter_txn(self.db, conn) as conn:
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
                async for row in (await conn.stream(query))], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def get_domain_occupancy(self, domain_name, *, conn=None):
        # TODO: store domain occupied_slots in Redis?
        known_slot_types = await self.shared_config.get_resource_slots()
        async with reenter_txn(self.db, conn) as conn:
            query = (
                sa.select([kernels.c.occupied_slots])
                .where(
                    (kernels.c.domain_name == domain_name) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
            )
            zero = ResourceSlot()
            key_occupied = sum([row['occupied_slots'] async for row in (await conn.stream(query))], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def get_group_occupancy(self, group_id, *, conn=None):
        # TODO: store domain occupied_slots in Redis?
        known_slot_types = await self.shared_config.get_resource_slots()
        async with reenter_txn(self.db, conn) as conn:
            query = (
                sa.select([kernels.c.occupied_slots])
                .where(
                    (kernels.c.group_id == group_id) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
            )
            zero = ResourceSlot()
            key_occupied = sum([row['occupied_slots'] async for row in (await conn.stream(query))], zero)
            # drop no-longer used slot types
            drops = [k for k in key_occupied.keys() if k not in known_slot_types]
            for k in drops:
                del key_occupied[k]
            return key_occupied

    async def recalc_resource_usage(self) -> None:
        concurrency_used_per_key: MutableMapping[str, int] = defaultdict(lambda: 0)
        occupied_slots_per_agent: MutableMapping[str, ResourceSlot] = \
            defaultdict(lambda: ResourceSlot({'cpu': 0, 'mem': 0}))

        async def _recalc() -> None:
            async with self.db.begin() as conn:
                # Query running containers and calculate concurrency_used per AK and
                # occupied_slots per agent.
                query = (
                    sa.select([kernels.c.access_key, kernels.c.agent, kernels.c.occupied_slots])
                    .where(kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                    .order_by(sa.asc(kernels.c.access_key))
                )
                async for row in (await conn.stream(query)):
                    occupied_slots_per_agent[row.agent] += ResourceSlot(row.occupied_slots)
                query = (
                    sa.select([kernels.c.access_key, kernels.c.agent, kernels.c.occupied_slots])
                    .where(kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                    .order_by(sa.asc(kernels.c.access_key))
                )
                async for row in (await conn.stream(query)):
                    concurrency_used_per_key[row.access_key] += 1

                if len(concurrency_used_per_key) > 0:
                    # Update concurrency_used for keypairs with running containers.
                    for ak, used in concurrency_used_per_key.items():
                        query = (
                            sa.update(keypairs)
                            .values(concurrency_used=used)
                            .where(keypairs.c.access_key == ak)
                        )
                        await conn.execute(query)
                    # Update all other keypairs to have concurrency_used = 0.
                    query = (
                        sa.update(keypairs)
                        .values(concurrency_used=0)
                        .where(keypairs.c.concurrency_used != 0)
                        .where(sa.not_(keypairs.c.access_key.in_(concurrency_used_per_key.keys())))
                    )
                    await conn.execute(query)
                else:
                    query = (
                        sa.update(keypairs)
                        .values(concurrency_used=0)
                        .where(keypairs.c.concurrency_used != 0)
                    )
                    await conn.execute(query)

                if len(occupied_slots_per_agent) > 0:
                    # Update occupied_slots for agents with running containers.
                    for aid, slots in occupied_slots_per_agent.items():
                        query = (
                            sa.update(agents)
                            .values(occupied_slots=slots)
                            .where(agents.c.id == aid)
                        )
                        await conn.execute(query)
                    # Update all other agents to have empty occupied_slots.
                    query = (
                        sa.update(agents)
                        .values(occupied_slots=ResourceSlot({}))
                        .where(agents.c.status == AgentStatus.ALIVE)
                        .where(sa.not_(agents.c.id.in_(occupied_slots_per_agent.keys())))
                    )
                    await conn.execute(query)
                else:
                    query = (
                        sa.update(agents)
                        .values(occupied_slots=ResourceSlot({}))
                        .where(agents.c.status == AgentStatus.ALIVE)
                    )
                    await conn.execute(query)

        await execute_with_retry(_recalc)

    async def destroy_session_lowlevel(
        self,
        session_id: SessionId,
        kernels: Sequence[Row],  # should have (id, agent, agent_addr, container_id) columns
    ) -> None:
        """
        Destroy the kernels that belongs the to given session unconditionally
        and without generation of any relevant events nor invocation of plugin hooks.
        """
        keyfunc = lambda item: item['agent'] if item['agent'] is not None else ''
        for agent_id, group_iterator in itertools.groupby(
            sorted(kernels, key=keyfunc), key=keyfunc,
        ):
            rpc_coros = []
            destroyed_kernels = []
            grouped_kernels = [*group_iterator]
            for kernel in grouped_kernels:
                if kernel['container_id'] is not None and kernel['agent_addr'] is not None:
                    destroyed_kernels.append(kernel)
            if not destroyed_kernels:
                return
            async with RPCContext(
                destroyed_kernels[0]['agent'],
                destroyed_kernels[0]['agent_addr'],
                None,
                order_key=session_id,
            ) as rpc:
                for kernel in destroyed_kernels:
                    # internally it enqueues a "destroy" lifecycle event.
                    rpc_coros.append(
                        rpc.call.destroy_kernel(
                            str(kernel['id']),
                            "failed-to-start",
                            suppress_events=True,
                        )
                    )
                await asyncio.gather(*rpc_coros)

    async def destroy_session(
        self,
        session_getter: SessionGetter,
        *,
        forced: bool = False,
        reason: str = 'user-requested',
    ) -> Mapping[str, Any]:
        """
        Destroy session kernels. Do not destroy
        PREPARING/TERMINATING/ERROR and PULLING sessions.

        :param forced: If True, destroy PREPARING/TERMINATING/ERROR session.
                       However, PULLING session still cannot be destroyed.
        :param reason: Reason to destroy a session if client wants to specify it manually.
        """
        async with self.db.begin_readonly() as conn:
            session = await session_getter(db_connection=conn)
        if forced:
            reason = 'force-terminated'
        hook_result = await self.hook_plugin_ctx.dispatch(
            'PRE_DESTROY_SESSION',
            (session['session_id'], session['session_name'], session['access_key']),
            return_when=ALL_COMPLETED,
        )
        if hook_result.status != PASSED:
            raise RejectedByHook.from_hook_result(hook_result)

        async with self.handle_kernel_exception(
            'destroy_session', session['id'], session['access_key'], set_error=True,
        ):

            async def _fetch() -> Sequence[Row]:
                async with self.db.begin_readonly() as conn:
                    query = (
                        sa.select([
                            kernels.c.id,
                            kernels.c.session_id,
                            kernels.c.session_creation_id,
                            kernels.c.status,
                            kernels.c.access_key,
                            kernels.c.cluster_role,
                            kernels.c.agent,
                            kernels.c.agent_addr,
                            kernels.c.container_id,
                        ])
                        .select_from(kernels)
                        .where(kernels.c.session_id == session['id'])
                    )
                    result = await conn.execute(query)
                    kernel_list = result.fetchall()
                return kernel_list

            kernel_list = await execute_with_retry(_fetch)
            main_stat = {}
            per_agent_tasks = []
            now = datetime.now(tzutc())

            keyfunc = lambda item: item['agent'] if item['agent'] is not None else ''
            for agent_id, group_iterator in itertools.groupby(
                sorted(kernel_list, key=keyfunc), key=keyfunc,
            ):
                destroyed_kernels = []
                grouped_kernels = [*group_iterator]
                for kernel in grouped_kernels:
                    if kernel['status'] == KernelStatus.PENDING:

                        async def _update() -> None:
                            async with self.db.begin() as conn:
                                await conn.execute(
                                    sa.update(kernels)
                                    .values({
                                        'status': KernelStatus.CANCELLED,
                                        'status_info': reason,
                                        'status_changed': now,
                                        'terminated_at': now,
                                    })
                                    .where(kernels.c.id == kernel['id'])
                                )

                        await execute_with_retry(_update)
                        await self.event_producer.produce_event(
                            KernelCancelledEvent(kernel['id'], '', reason)
                        )
                        if kernel['cluster_role'] == DEFAULT_ROLE:
                            main_stat = {'status': 'cancelled'}
                            await self.event_producer.produce_event(
                                SessionCancelledEvent(
                                    kernel['session_id'],
                                    kernel['session_creation_id'],
                                    reason,
                                )
                            )
                    elif kernel['status'] == KernelStatus.PULLING:
                        raise GenericForbidden('Cannot destroy kernels in pulling status')
                    elif kernel['status'] in (
                        KernelStatus.SCHEDULED,
                        KernelStatus.PREPARING,
                        KernelStatus.TERMINATING,
                        KernelStatus.ERROR,
                    ):
                        if not forced:
                            raise GenericForbidden(
                                'Cannot destroy kernels in scheduled/preparing/terminating/error status'
                            )
                        log.warning('force-terminating kernel (k:{}, status:{})',
                                    kernel['id'], kernel['status'])
                        if kernel['container_id'] is not None:
                            destroyed_kernels.append(kernel)

                        async def _update() -> None:
                            async with self.db.begin() as conn:
                                if kernel['cluster_role'] == DEFAULT_ROLE:
                                    # The main session is terminated;
                                    # decrement the user's concurrency counter
                                    await conn.execute(
                                        sa.update(keypairs)
                                        .values({
                                            'concurrency_used': keypairs.c.concurrency_used - 1,
                                        })
                                        .where(keypairs.c.access_key == kernel['access_key'])
                                    )
                                await conn.execute(
                                    sa.update(kernels)
                                    .values({
                                        'status': KernelStatus.TERMINATED,
                                        'status_info': reason,
                                    })
                                    .where(kernels.c.id == kernel['id'])
                                )

                        await execute_with_retry(_update)
                        await self.event_producer.produce_event(
                            KernelTerminatedEvent(kernel['id'], reason)
                        )
                    else:

                        async def _update() -> None:
                            async with self.db.begin() as conn:
                                if kernel['cluster_role'] == DEFAULT_ROLE:
                                    # The main session is terminated;
                                    # decrement the user's concurrency counter
                                    await conn.execute(
                                        sa.update(keypairs)
                                        .values({
                                            'concurrency_used': keypairs.c.concurrency_used - 1,
                                        })
                                        .where(keypairs.c.access_key == kernel['access_key'])
                                    )
                                await conn.execute(
                                    sa.update(kernels)
                                    .values({
                                        'status': KernelStatus.TERMINATING,
                                        'status_info': reason,
                                        'status_data': {
                                            "kernel": {"exit_code": None},
                                            "session": {"status": "terminating"},
                                        }
                                    })
                                    .where(kernels.c.id == kernel['id'])
                                )

                        await execute_with_retry(_update)
                        await self.event_producer.produce_event(
                            KernelTerminatingEvent(kernel['id'], reason),
                        )

                    if kernel['agent_addr'] is None:
                        await self.mark_kernel_terminated(kernel['id'], 'missing-agent-allocation')
                        if kernel['cluster_role'] == DEFAULT_ROLE:
                            main_stat = {'status': 'terminated'}
                    else:
                        destroyed_kernels.append(kernel)

                async def _destroy_kernels_in_agent(session, destroyed_kernels) -> None:
                    nonlocal main_stat
                    async with RPCContext(
                        destroyed_kernels[0]['agent'],
                        destroyed_kernels[0]['agent_addr'],
                        None,
                        order_key=session['session_id'],
                    ) as rpc:
                        rpc_coros = []
                        for kernel in destroyed_kernels:
                            # internally it enqueues a "destroy" lifecycle event.
                            if kernel['status'] != KernelStatus.SCHEDULED:
                                rpc_coros.append(
                                    rpc.call.destroy_kernel(str(kernel['id']), reason)
                                )
                        try:
                            await asyncio.gather(*rpc_coros)
                        except Exception:
                            log.exception(
                                "destroy_kernels_in_agent(a:{}, s:{}): unexpected error",
                                destroyed_kernels[0]['agent'],
                                session['session_id'],
                            )
                        for kernel in destroyed_kernels:
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
                            if kernel['cluster_role'] == DEFAULT_ROLE:
                                main_stat = {
                                    **(last_stat if last_stat is not None else {}),
                                    'status': 'terminated',
                                }

                if destroyed_kernels:
                    per_agent_tasks.append(_destroy_kernels_in_agent(session, destroyed_kernels))

            if per_agent_tasks:
                await asyncio.gather(*per_agent_tasks)
            await self.hook_plugin_ctx.notify(
                'POST_DESTROY_SESSION',
                (session['session_id'], session['session_name'], session['access_key']),
            )
            if forced:
                await self.recalc_resource_usage()
            return main_stat

    async def clean_session(
        self,
        session_id: SessionId,
    ) -> None:

        async def _fetch() -> Row:
            async with self.db.begin() as conn:
                query = (
                    sa.select([
                        kernels.c.session_id,
                        kernels.c.cluster_mode,
                        kernels.c.cluster_size,
                        kernels.c.agent,
                        kernels.c.agent_addr,
                    ])
                    .select_from(kernels)
                    .where(
                        (kernels.c.session_id == session_id) &
                        (kernels.c.cluster_role == DEFAULT_ROLE)
                    )
                )
                result = await conn.execute(query)
                return result.first()

        session = await execute_with_retry(_fetch)
        if session is None:
            return
        if session['cluster_mode'] == ClusterMode.SINGLE_NODE and session['cluster_size'] > 1:
            network_name = f'bai-singlenode-{session["session_id"]}'
            try:
                async with RPCContext(
                    session['agent'],       # the main-container's agent
                    session['agent_addr'],
                    None,
                    order_key=session['session_id'],
                ) as rpc:
                    await rpc.call.destroy_local_network(network_name)
            except Exception:
                log.exception(f"Failed to destroy the agent-local network {network_name}")
        elif session['cluster_mode'] == ClusterMode.MULTI_NODE:
            network_name = f'bai-multinode-{session["session_id"]}'
            try:
                try:
                    # await rpc.call.destroy_overlay_network(network_name)
                    await asyncio.sleep(2.0)
                    network = await self.docker.networks.get(network_name)
                    await network.delete()
                except aiodocker.DockerError as e:
                    if e.status == 404:
                        # It may have been auto-destructed when the last container was detached.
                        pass
                    else:
                        raise
            except Exception:
                log.exception(f"Failed to destroy the overlay network {network_name}")
        else:
            pass

    async def restart_session(
        self,
        session_creation_id: str,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> None:
        log.warning('restart_session({})', session_name_or_id)
        async with self.db.begin() as conn:
            session_infos = await match_session_ids(
                session_name_or_id,
                access_key,
                db_connection=conn,
            )
            if len(session_infos) > 1:
                raise TooManySessionsMatched(extra_data={'matches': session_infos})
            elif len(session_infos) == 0:
                raise SessionNotFound()
            session_id = session_infos[0]['session_id']
            kernel_list = [row for row in await get_all_kernels(
                [session_id],
                db_connection=conn,
            )][0]

        async def _restart_kernel(kernel) -> None:
            loop = asyncio.get_running_loop()
            try:
                kernel_creation_id = secrets.token_urlsafe(16)
                start_event = loop.create_future()
                self.kernel_creation_tracker[
                    kernel['id']
                ] = start_event
                try:
                    async with self.db.begin() as conn:
                        query = (
                            kernels.update()
                            .values({
                                'status': KernelStatus.RESTARTING,
                            })
                            .where(kernels.c.id == kernel['id'])
                        )
                        await conn.execute(query)
                    async with RPCContext(
                        kernel['agent'],       # the main-container's agent
                        kernel['agent_addr'],
                        None,
                        order_key=None,
                    ) as rpc:
                        updated_config: Dict[str, Any] = {
                            # TODO: support resacling of sub-containers
                        }
                        kernel_info = await rpc.call.restart_kernel(
                            kernel_creation_id,
                            str(kernel['session_id']),
                            str(kernel['id']),
                            updated_config,
                        )
                    await start_event
                    async with self.db.begin() as conn:
                        query = (
                            kernels.update()
                            .values({
                                'status': KernelStatus.RUNNING,
                                'container_id': kernel_info['container_id'],
                                'repl_in_port': kernel_info['repl_in_port'],
                                'repl_out_port': kernel_info['repl_out_port'],
                                'stdin_port': kernel_info['stdin_port'],
                                'stdout_port': kernel_info['stdout_port'],
                                'service_ports': kernel_info.get('service_ports', []),
                            })
                            .where(kernels.c.id == kernel['id'])
                        )
                        await conn.execute(query)
                finally:
                    del self.kernel_creation_tracker[
                        kernel['id']
                    ]
            except Exception:
                log.exception('unexpected-error in _restart_kerenl()')

        restart_coros = []
        for kernel in kernel_list:
            restart_coros.append(_restart_kernel(kernel))
        async with self.handle_kernel_exception(
            'restart_session', session_name_or_id, access_key, set_error=True,
        ):
            await asyncio.gather(*restart_coros)

        # NOTE: If the restarted session is a batch-type one, then the startup command
        #       will be executed again after restart.
        await self.event_producer.produce_event(
            SessionStartedEvent(session_id, session_creation_id)
        )

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
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                30,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.execute(
                    str(kernel['id']),
                    major_api_version,
                    run_id, mode, code, opts,
                    flush_timeout,
                )

    async def execute_batch(
        self,
        session_id: SessionId,
    ) -> None:
        async with self.db.begin() as conn:
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.status,
                    kernels.c.agent_addr,
                    kernels.c.startup_command,
                ])
                .select_from(kernels)
                .where(
                    (kernels.c.session_id == session_id) &
                    (kernels.c.session_type == SessionTypes.BATCH) &
                    (kernels.c.status == KernelStatus.RUNNING) &
                    (kernels.c.cluster_role == DEFAULT_ROLE)
                )
            )
            result = await conn.execute(query)
            kernel = result.first()
            if kernel is None:
                return
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                30,
                order_key=str(session_id),
            ) as rpc:
                return await rpc.call.execute_batch(str(kernel['id']), kernel['startup_command'])

    async def interrupt_session(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                30,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.interrupt_kernel(str(kernel['id']))

    async def get_completions(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        text: str,
        opts: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                10,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.get_completions(str(kernel['id']), text, opts)

    async def start_service(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        service: str,
        opts: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('execute', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                None,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.start_service(str(kernel['id']), service, opts)

    async def shutdown_service(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        service: str
    ) -> None:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('shutdown_service', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                None,
                order_key=kernel['id'],
            ) as rpc:
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
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                None,
                order_key=kernel['id'],
            ) as rpc:
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
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                None,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.download_file(str(kernel['id']), filepath)

    async def list_files(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
        path: str,
    ) -> Mapping[str, Any]:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('list_files', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                30,
                order_key=kernel['id'],
            ) as rpc:
                return await rpc.call.list_files(str(kernel['id']), path)

    async def get_logs_from_agent(
        self,
        session_name_or_id: Union[str, SessionId],
        access_key: AccessKey,
    ) -> str:
        kernel = await self.get_session(session_name_or_id, access_key)
        async with self.handle_kernel_exception('get_logs_from_agent', kernel['id'], access_key):
            async with RPCContext(
                kernel['agent'],
                kernel['agent_addr'],
                30,
                order_key=kernel['id'],
            ) as rpc:
                reply = await rpc.call.get_logs(str(kernel['id']))
                return reply['logs']

    async def increment_session_usage(
        self,
        session_name: str,
        access_key: AccessKey,
        conn: SAConnection = None,
    ) -> None:
        pass
        # async with reenter_txn(self.db, conn) as conn:
        #     query = (
        #         sa.update(kernels)
        #         .values(num_queries=kernels.c.num_queries + 1)
        #         .where(
        #             (kernels.c.session_name == session_name) &
        #             (kernels.c.access_key == access_key) &
        #             (kernels.c.cluster_role == DEFAULT_ROLE)
        #         )
        #     )
        #     await execute_with_retry(conn, query)

    async def kill_all_sessions_in_agent(self, agent_id, agent_addr):
        async with RPCContext(agent_id, agent_addr, None) as rpc:
            coro = rpc.call.clean_all_kernels('manager-freeze-force-kill')
            return await coro

    async def kill_all_sessions(self, conn=None):
        async with reenter_txn(self.db, conn, {'postgresql_readonly': True}) as conn:
            query = (sa.select([agents.c.id, agents.c.addr])
                       .where(agents.c.status == AgentStatus.ALIVE))
            result = await conn.execute(query)
            rows = result.fetchall()
        tasks = []
        for row in rows:
            tasks.append(
                self.kill_all_sessions_in_agent(row['id'], row['addr'])
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    async def handle_heartbeat(self, agent_id, agent_info):
        now = datetime.now(tzutc())
        slot_key_and_units = {
            SlotName(k): SlotTypes(v[0]) for k, v in
            agent_info['resource_slots'].items()}
        available_slots = ResourceSlot({
            SlotName(k): Decimal(v[1]) for k, v in
            agent_info['resource_slots'].items()})
        current_addr = agent_info['addr']
        sgroup = agent_info.get('scaling_group', 'default')

        async with self.heartbeat_lock:

            instance_rejoin = False

            # Update "last seen" timestamp for liveness tracking
            await self.redis_live.hset('agent.last_seen', agent_id, now.timestamp())

            # Check and update status of the agent record in DB
            async def _update() -> None:
                nonlocal instance_rejoin
                async with self.db.begin() as conn:
                    fetch_query = (
                        sa.select([
                            agents.c.status,
                            agents.c.addr,
                            agents.c.scaling_group,
                            agents.c.available_slots,
                            agents.c.version,
                            agents.c.compute_plugins,
                        ])
                        .select_from(agents)
                        .where(agents.c.id == agent_id)
                        .with_for_update()
                    )
                    result = await conn.execute(fetch_query)
                    row = result.first()

                    if row is None or row['status'] is None:
                        # new agent detected!
                        log.info('agent {0} joined!', agent_id)
                        await self.shared_config.update_resource_slots(slot_key_and_units)
                        insert_query = sa.insert(agents).values({
                            'id': agent_id,
                            'status': AgentStatus.ALIVE,
                            'region': agent_info['region'],
                            'scaling_group': sgroup,
                            'available_slots': available_slots,
                            'occupied_slots': {},
                            'addr': agent_info['addr'],
                            'first_contact': now,
                            'lost_at': sa.null(),
                            'version': agent_info['version'],
                            'compute_plugins': agent_info['compute_plugins'],
                        })
                        result = await conn.execute(insert_query)
                        assert result.rowcount == 1
                    elif row['status'] == AgentStatus.ALIVE:
                        updates = {}
                        if row['available_slots'] != available_slots:
                            updates['available_slots'] = available_slots
                        if row['scaling_group'] != sgroup:
                            updates['scaling_group'] = sgroup
                        if row['addr'] != current_addr:
                            updates['addr'] = current_addr
                        if row['version'] != agent_info['version']:
                            updates['version'] = agent_info['version']
                        if row['compute_plugins'] != agent_info['compute_plugins']:
                            updates['compute_plugins'] = agent_info['compute_plugins']
                        # occupied_slots are updated when kernels starts/terminates
                        if updates:
                            await self.shared_config.update_resource_slots(slot_key_and_units)
                            update_query = (
                                sa.update(agents)
                                .values(updates)
                                .where(agents.c.id == agent_id)
                            )
                            await conn.execute(update_query)
                    elif row['status'] in (AgentStatus.LOST, AgentStatus.TERMINATED):
                        await self.shared_config.update_resource_slots(slot_key_and_units)
                        instance_rejoin = True
                        update_query = (
                            sa.update(agents)
                            .values({
                                'status': AgentStatus.ALIVE,
                                'region': agent_info['region'],
                                'scaling_group': sgroup,
                                'addr': agent_info['addr'],
                                'lost_at': sa.null(),
                                'available_slots': available_slots,
                                'version': agent_info['version'],
                                'compute_plugins': agent_info['compute_plugins'],
                            })
                            .where(agents.c.id == agent_id)
                        )
                        await conn.execute(update_query)
                    else:
                        log.error('should not reach here! {0}', type(row['status']))

            await execute_with_retry(_update)

            if instance_rejoin:
                await self.event_producer.produce_event(
                    AgentStartedEvent('revived'),
                    source=agent_id,
                )

            # Update the mapping of kernel images to agents.
            known_registries = await get_known_registries(self.shared_config.etcd)
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

    async def mark_agent_terminated(self, agent_id: AgentId, status: AgentStatus) -> None:
        global agent_peers
        await self.redis_live.hdel('agent.last_seen', agent_id)

        async def _pipe_builder():
            pipe = self.redis_image.pipeline()
            async for imgname in self.redis_image.iscan():
                pipe.srem(imgname, agent_id)
            return pipe

        async def _update() -> None:
            async with self.db.begin() as conn:
                fetch_query = (
                    sa.select([
                        agents.c.status,
                        agents.c.addr,
                    ])
                    .select_from(agents)
                    .where(agents.c.id == agent_id)
                    .with_for_update()
                )
                result = await conn.execute(fetch_query)
                row = result.first()
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
                update_query = (
                    sa.update(agents)
                    .values({
                        'status': status,
                        'status_changed': now,
                        'lost_at': now,
                    })
                    .where(agents.c.id == agent_id)
                )
                await conn.execute(update_query)

        await redis.execute_with_retries(_pipe_builder)
        await execute_with_retry(_update)

    async def set_session_status(
        self,
        session_id: SessionId,
        access_key: AccessKey,
        status: KernelStatus,
        reason: str = '',
        **extra_fields,
    ) -> None:
        now = datetime.now(tzutc())
        data = {
            'status': status,
            'status_info': reason,
            'status_changed': now,
        }
        if status in (KernelStatus.CANCELLED, KernelStatus.TERMINATED):
            data['terminated_at'] = now
        data.update(extra_fields)

        async def _update() -> None:
            async with self.db.begin() as conn:
                query = (
                    sa.update(kernels)
                    .values(data)
                    .where(
                        (kernels.c.session_id == session_id) &
                        (kernels.c.access_key == access_key) &
                        ~(kernels.c.status.in_(DEAD_KERNEL_STATUSES))
                    )
                )
                await conn.execute(query)

        await execute_with_retry(_update)

    async def set_kernel_status(
        self, kernel_id: KernelId,
        status: KernelStatus,
        reason: str = '',
    ) -> None:
        assert status != KernelStatus.TERMINATED, \
               'TERMINATED status update must be handled in ' \
               'mark_kernel_terminated()'
        now = datetime.now(tzutc())
        data = {
            'status': status,
            'status_info': reason,
            'status_changed': now,
        }
        if status in (KernelStatus.CANCELLED, KernelStatus.TERMINATED):
            data['terminated_at'] = now

        async def _update() -> None:
            async with self.db.begin() as conn:
                query = (
                    sa.update(kernels)
                    .values(data)
                    .where(kernels.c.id == kernel_id)
                )
                await conn.execute(query)

        await execute_with_retry(_update)

    async def set_session_result(
        self,
        session_id: SessionId,
        success: bool,
        exit_code: int,
    ) -> None:
        # TODO: store exit code?
        data = {
            'result': SessionResult.SUCCESS if success else SessionResult.FAILURE,
        }

        async def _update() -> None:
            async with self.db.begin() as conn:
                query = (
                    sa.update(kernels)
                    .values(data)
                    .where(kernels.c.id == session_id)
                )
                await conn.execute(query)

        await execute_with_retry(_update)

    async def sync_kernel_stats(
        self, kernel_ids: Sequence[KernelId],
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

        async def _update():
            async with self.db.begin() as conn:
                for kernel_id, updates in per_kernel_updates.items():
                    update_query = (
                        sa.update(kernels)
                        .values(updates)
                        .where(kernels.c.id == kernel_id)
                    )
                    await conn.execute(update_query)

        await execute_with_retry(_update)

    async def mark_kernel_terminated(
        self,
        kernel_id: KernelId,
        reason: str,
        exit_code: int = None,
    ) -> None:
        """
        Mark the kernel (individual worker) terminated and release
        the resource slots occupied by it.
        """
        post_task = self._post_kernel_creation_tasks.get(kernel_id, None)
        if post_task is not None and not post_task.done():
            post_task.cancel()
            await post_task

        async def _update_kernel_status() -> Row | None:
            async with self.db.begin() as conn:
                # Check the current status.
                select_query = (
                    sa.select([
                        kernels.c.access_key,
                        kernels.c.agent,
                        kernels.c.status,
                        kernels.c.occupied_slots,
                        kernels.c.session_id,
                    ])
                    .select_from(kernels)
                    .where(kernels.c.id == kernel_id)
                )
                result = await conn.execute(select_query)
                kernel = result.first()
                if (
                    kernel is None
                    or kernel['status'] in (
                        KernelStatus.CANCELLED,
                        KernelStatus.TERMINATED,
                        KernelStatus.RESTARTING,
                    )
                ):
                    # Skip if non-existent, already terminated, or restarting.
                    return None

                # Change the status to TERMINATED.
                # (we don't delete the row for later logging and billing)
                now = datetime.now(tzutc())
                update_query = (
                    sa.update(kernels)
                    .values({
                        'status': KernelStatus.TERMINATED,
                        'status_info': reason,
                        'status_changed': now,
                        'status_data': sql_json_merge(
                            kernels.c.status_data,
                            ("kernel",),
                            {"exit_code": exit_code},
                        ),
                        'terminated_at': now,
                    })
                    .where(kernels.c.id == kernel_id)
                )
                await conn.execute(update_query)
                return kernel

        kernel = await execute_with_retry(_update_kernel_status)
        if kernel is None:
            return

        async def _recalc() -> None:
            assert kernel is not None
            async with self.db.begin() as conn:
                await recalc_concurrency_used(conn, kernel['access_key'])
                await recalc_agent_resource_occupancy(conn, kernel['agent'])

        await execute_with_retry(_recalc)

        # Perform statistics sync in a separate transaction block, since
        # it may take a while to fetch stats from Redis.

        await self.sync_kernel_stats([kernel_id])

    async def check_session_terminated(
        self,
        kernel_id: KernelId,
        reason: str,
    ) -> None:

        async def _check_and_mark() -> Tuple[bool, SessionId | None]:
            async with self.db.begin() as conn:
                session_id_query = (
                    sa.select([
                        kernels.c.session_id,
                    ])
                    .select_from(kernels)
                    .where(kernels.c.id == kernel_id)
                )
                kernels_query = (
                    sa.select([
                        kernels.c.session_id,
                        kernels.c.status_data,
                        kernels.c.status,
                    ])
                    .select_from(kernels)
                    .where(
                        (kernels.c.session_id == session_id_query.scalar_subquery())
                    )
                    .with_for_update()
                )
                result = await conn.execute(kernels_query)
                rows = result.fetchall()
                if not rows:
                    return False, None
                session_id = rows[0]['session_id']
                if nmget(rows[0]['status_data'], "session.status") == "terminated":
                    # if already marked "session-terminated", skip the rest process
                    return False, session_id
                all_terminated = all(map(
                    lambda row: row['status'] == KernelStatus.TERMINATED,
                    rows,
                ))
                if all_terminated:
                    await conn.execute(
                        sa.update(kernels)
                        .values(
                            status_data=sql_json_merge(
                                kernels.c.status_data,
                                ("session",),
                                {
                                    "status": "terminated",
                                },
                            ),
                        )
                        .where(
                            (kernels.c.session_id == session_id)
                        )
                    )
                return all_terminated, session_id

        do_fire_event, session_id = await execute_with_retry(_check_and_mark)
        if session_id is None:
            return
        if do_fire_event:
            await self.event_producer.produce_event(
                SessionTerminatedEvent(session_id, reason)
            )

    async def mark_session_terminated(
        self,
        session_id: SessionId,
        reason: str,
    ) -> None:
        await self.clean_session(session_id)
