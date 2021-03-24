from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
import enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
    TYPE_CHECKING,
    Union,
)
from uuid import UUID
import uuid

from aioredis import Redis
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common import msgpack, redis
from ai.backend.common.types import (
    AccessKey,
    BinarySize,
    ClusterMode,
    KernelId,
    SessionId,
    SessionTypes,
    SessionResult,
    SlotName,
    ResourceSlot,
)

from ..defs import DEFAULT_ROLE
from .base import (
    BigInt,
    EnumType,
    GUID,
    Item,
    KernelIDColumn,
    PaginatedList,
    ResourceSlotColumn,
    SessionIDColumnType,
    batch_result,
    batch_multiresult,
    metadata,
)
from .group import groups
from .user import users
from .keypair import keypairs
if TYPE_CHECKING:
    from .gql import GraphQueryContext

__all__: Sequence[str] = (
    'kernels',
    'session_dependencies',
    'KernelStatus',
    'ComputeContainer',
    'ComputeSession',
    'ComputeContainerList',
    'ComputeSessionList',
    'LegacyComputeSession',
    'LegacyComputeSessionList',
    'AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES',
    'USER_RESOURCE_OCCUPYING_KERNEL_STATUSES',
    'RESOURCE_USAGE_KERNEL_STATUSES',
    'DEAD_KERNEL_STATUSES',
    'LIVE_STATUS',
    'recalc_concurrency_used',
)


class KernelStatus(enum.Enum):
    # values are only meaningful inside the gateway
    PENDING = 0
    # ---
    PREPARING = 10
    # ---
    BUILDING = 20
    PULLING = 21
    # ---
    RUNNING = 30
    RESTARTING = 31
    RESIZING = 32
    SUSPENDED = 33
    # ---
    TERMINATING = 40
    TERMINATED = 41
    ERROR = 42
    CANCELLED = 43


# statuses to consider when calculating current resource usage
AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES = tuple(
    e for e in KernelStatus
    if e not in (
        KernelStatus.TERMINATED,
        KernelStatus.PENDING,
        KernelStatus.CANCELLED,
    )
)

USER_RESOURCE_OCCUPYING_KERNEL_STATUSES = tuple(
    e for e in KernelStatus
    if e not in (
        KernelStatus.TERMINATING,
        KernelStatus.TERMINATED,
        KernelStatus.PENDING,
        KernelStatus.CANCELLED,
    )
)

# statuses to consider when calculating historical resource usage
RESOURCE_USAGE_KERNEL_STATUSES = (
    KernelStatus.TERMINATED,
    KernelStatus.RUNNING,
)

DEAD_KERNEL_STATUSES = (
    KernelStatus.CANCELLED,
    KernelStatus.TERMINATED,
)

LIVE_STATUS = (
    KernelStatus.RUNNING,
)


def default_hostname(context) -> str:
    params = context.get_current_parameters()
    return f"{params['cluster_role']}{params['cluster_idx']}"


kernels = sa.Table(
    'kernels', metadata,
    # The Backend.AI-side UUID for each kernel
    # (mapped to a container in the docker backend and a pod in the k8s backend)
    KernelIDColumn(),
    # session_id == id when the kernel is the main container in a multi-container session or a
    # single-container session.
    # Otherwise, it refers the kernel ID of the main contaienr of the belonged multi-container session.
    sa.Column('session_id', SessionIDColumnType, unique=False, index=True, nullable=False),
    sa.Column('session_creation_id', sa.String(length=32), unique=False, index=False),
    sa.Column('session_name', sa.String(length=64), unique=False, index=True),     # previously sess_id
    sa.Column('session_type', EnumType(SessionTypes), index=True, nullable=False,  # previously sess_type
              default=SessionTypes.INTERACTIVE, server_default=SessionTypes.INTERACTIVE.name),
    sa.Column('cluster_mode', sa.String(length=16), nullable=False,
              default=ClusterMode.SINGLE_NODE, server_default=ClusterMode.SINGLE_NODE.name),
    sa.Column('cluster_size', sa.Integer, nullable=False, default=1),
    sa.Column('cluster_role', sa.String(length=16), nullable=False, default=DEFAULT_ROLE, index=True),
    sa.Column('cluster_idx', sa.Integer, nullable=False, default=0),
    sa.Column('cluster_hostname', sa.String(length=64), nullable=False, default=default_hostname),

    # Resource ownership
    sa.Column('scaling_group', sa.ForeignKey('scaling_groups.name'), index=True, nullable=True),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id'), nullable=True),
    sa.Column('agent_addr', sa.String(length=128), nullable=True),
    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False),
    sa.Column('group_id', GUID, sa.ForeignKey('groups.id'), nullable=False),
    sa.Column('user_uuid', GUID, sa.ForeignKey('users.uuid'), nullable=False),
    sa.Column('access_key', sa.String(length=20), sa.ForeignKey('keypairs.access_key')),
    sa.Column('image', sa.String(length=512)),
    sa.Column('registry', sa.String(length=512)),
    sa.Column('tag', sa.String(length=64), nullable=True),

    # Resource occupation
    sa.Column('container_id', sa.String(length=64)),
    sa.Column('occupied_slots', ResourceSlotColumn(), nullable=False),
    sa.Column('occupied_shares', pgsql.JSONB(), nullable=False, default={}),  # legacy
    sa.Column('environ', sa.ARRAY(sa.String), nullable=True),
    sa.Column('mounts', sa.ARRAY(sa.String), nullable=True),  # list of list
    sa.Column('mount_map', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('attached_devices', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True),

    # Port mappings
    # If kernel_host is NULL, it is assumed to be same to the agent host or IP.
    sa.Column('kernel_host', sa.String(length=128), nullable=True),
    sa.Column('repl_in_port', sa.Integer(), nullable=False),
    sa.Column('repl_out_port', sa.Integer(), nullable=False),
    sa.Column('stdin_port', sa.Integer(), nullable=False),   # legacy for stream_pty
    sa.Column('stdout_port', sa.Integer(), nullable=False),  # legacy for stream_pty
    sa.Column('service_ports', pgsql.JSONB(), nullable=True),
    sa.Column('preopen_ports', sa.ARRAY(sa.Integer), nullable=True),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), index=True),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null(), index=True),
    sa.Column('starts_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null()),
    sa.Column('status', EnumType(KernelStatus),
              default=KernelStatus.PENDING,
              server_default=KernelStatus.PENDING.name,
              nullable=False, index=True),
    sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True, index=True),
    sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null()),
    sa.Column('status_data', pgsql.JSONB(), nullable=True, default=sa.null()),
    sa.Column('startup_command', sa.Text, nullable=True),
    sa.Column('result', EnumType(SessionResult),
              default=SessionResult.UNDEFINED,
              server_default=SessionResult.UNDEFINED.name,
              nullable=False, index=True),
    sa.Column('internal_data', pgsql.JSONB(), nullable=True),
    sa.Column('container_log', sa.LargeBinary(), nullable=True),
    # Resource metrics measured upon termination
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('last_stat', pgsql.JSONB(), nullable=True, default=sa.null()),

    sa.Index('ix_kernels_sess_id_role', 'session_id', 'cluster_role', unique=False),
    sa.Index('ix_kernels_status_role', 'status', 'cluster_role'),
    sa.Index('ix_kernels_updated_order',
             sa.func.greatest('created_at', 'terminated_at', 'status_changed'),
             unique=False),
    sa.Index('ix_kernels_unique_sess_token', 'access_key', 'session_id',
             unique=True,
             postgresql_where=sa.text(
                 "status NOT IN ('TERMINATED', 'CANCELLED') and "
                 "cluster_role = 'main'")),
)

session_dependencies = sa.Table(
    'session_dependencies', metadata,
    sa.Column('session_id', GUID,
              sa.ForeignKey('kernels.id', onupdate='CASCADE', ondelete='CASCADE'),
              index=True, nullable=False),
    sa.Column('depends_on', GUID,
              sa.ForeignKey('kernels.id', onupdate='CASCADE', ondelete='CASCADE'),
              index=True, nullable=False),
    sa.PrimaryKeyConstraint('session_id', 'depends_on'),
)

DEFAULT_SESSION_ORDERING = [
    sa.desc(sa.func.greatest(
        kernels.c.created_at,
        kernels.c.terminated_at,
        kernels.c.status_changed,
    ))
]


class SessionInfo(TypedDict):
    session_id: SessionId
    session_name: str
    status: KernelStatus
    created_at: datetime


async def match_session_ids(
    session_name_or_id: Union[str, UUID],
    access_key: AccessKey,
    *,
    db_connection: SAConnection,
    extra_cond=None,
    for_update: bool = False,
    max_matches: int = 10,
) -> Sequence[SessionInfo]:
    """
    Match the prefix of session ID or session name among the sessions that belongs to the given
    access key, and return the list of session IDs with matching prefixes.
    """
    cond_id = (
        (sa.sql.expression.cast(kernels.c.id, sa.String).like(f'{session_name_or_id}%')) &
        (kernels.c.access_key == access_key)
    )
    if extra_cond is not None:
        cond_id = cond_id & extra_cond
    cond_name = (
        (kernels.c.session_name.like(f'{session_name_or_id}%')) &
        (kernels.c.access_key == access_key)
    )
    if extra_cond is not None:
        cond_name = cond_name & extra_cond
    cond_session_id = (
        (sa.sql.expression.cast(kernels.c.session_id, sa.String).like(f'{session_name_or_id}%')) &
        (kernels.c.access_key == access_key)
    )
    if extra_cond is not None:
        cond_session_id = cond_session_id & extra_cond
    info_cols = [
        kernels.c.session_id,
        kernels.c.session_name,
        kernels.c.status,
        kernels.c.created_at,
    ]
    match_sid_by_id = (
        sa.select(info_cols)
        .select_from(kernels)
        .where(
            (kernels.c.session_id.in_(
                sa.select(
                    [kernels.c.session_id]
                )
                .select_from(kernels)
                .where(cond_id)
                .group_by(kernels.c.session_id)
                .limit(max_matches).offset(0)
            )) &
            (kernels.c.cluster_role == DEFAULT_ROLE)
        )
        .order_by(sa.desc(kernels.c.created_at))
    )
    if for_update:
        match_sid_by_id = match_sid_by_id.with_for_update()
    match_sid_by_name = (
        sa.select(info_cols)
        .select_from(kernels)
        .where(
            (kernels.c.session_id.in_(
                sa.select(
                    [kernels.c.session_id]
                )
                .select_from(kernels)
                .where(cond_name)
                .group_by(kernels.c.session_id)
                .limit(max_matches).offset(0)
            )) &
            (kernels.c.cluster_role == DEFAULT_ROLE)
        )
        .order_by(sa.desc(kernels.c.created_at))
    )
    if for_update:
        match_sid_by_name = match_sid_by_name.with_for_update()
    match_sid_by_session_id = (
        sa.select(info_cols)
        .select_from(kernels)
        .where(
            (kernels.c.session_id.in_(
                sa.select(
                    [kernels.c.session_id]
                )
                .select_from(kernels)
                .where(cond_session_id)
                .group_by(kernels.c.session_id)
                .limit(max_matches).offset(0)
            )) &
            (kernels.c.cluster_role == DEFAULT_ROLE)
        )
        .order_by(sa.desc(kernels.c.created_at))
    )
    if for_update:
        match_sid_by_session_id = match_sid_by_session_id.with_for_update()
    for match_query in [
        match_sid_by_session_id,
        match_sid_by_name,
        match_sid_by_id,
    ]:
        result = await db_connection.execute(match_query)
        rows = result.fetchall()
        if not rows:
            continue
        return [
            SessionInfo(
                session_id=row['session_id'],
                session_name=row['session_name'],
                status=row['status'],
                created_at=row['created_at'],
            ) for row in rows
        ]
    return []


async def get_main_kernels(
    session_ids: Sequence[SessionId],
    *,
    db_connection: SAConnection,
    for_update: bool = False,
) -> Sequence[Row]:
    """
    Return a list of the main kernels for the given session IDs.
    If a given session ID does not exist, its position will be ``None``.
    """
    session_id_to_rows = OrderedDict(
        (session_id, None) for session_id in session_ids
    )
    query = (
        sa.select([kernels])
        .select_from(kernels)
        .where(
            (kernels.c.session_id.in_(session_ids)) &
            (kernels.c.cluster_role == DEFAULT_ROLE)
        )
    )
    result = await db_connection.execute(query)
    for row in result.fetchall():
        session_id_to_rows[row['session_id']] = row
    return [*session_id_to_rows.values()]


async def get_all_kernels(
    session_ids: Sequence[SessionId],
    *,
    db_connection: SAConnection,
    for_update: bool = False,
) -> Sequence[Sequence[Row]]:
    """
    Return a list of all belonging kernel lists per the given session IDs
    in the order they are given.
    If a given session ID does not exist, an empty list will be returned
    at the position of that session ID.
    """
    session_id_to_rowsets: Dict[SessionId, List[Row]]
    session_id_to_rowsets = OrderedDict(
        (session_id, []) for session_id in session_ids
    )
    for session_id in session_ids:
        query = (
            sa.select([sa.text('*')])
            .select_from(kernels)
            .where(
                (kernels.c.session_id == session_id)
            )
        )
        result = await db_connection.execute(query)
        if result.rowcount == 0:
            continue
        session_id_to_rowsets[session_id].extend(
            row for row in result.fetchall()
        )
    return [*session_id_to_rowsets.values()]


class ComputeContainer(graphene.ObjectType):
    class Meta:
        interfaces = (Item, )

    # identity
    idx = graphene.Int()          # legacy
    role = graphene.String()      # legacy
    hostname = graphene.String()  # legacy
    cluster_idx = graphene.Int()
    cluster_role = graphene.String()
    cluster_hostname = graphene.String()
    session_id = graphene.UUID()  # owner session

    # image
    image = graphene.String()
    registry = graphene.String()

    # status
    status = graphene.String()
    status_changed = GQLDateTime()
    status_info = graphene.String()
    status_data = graphene.JSONString()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()
    starts_at = GQLDateTime()

    # resources
    agent = graphene.String()
    container_id = graphene.String()
    resource_opts = graphene.JSONString()
    occupied_slots = graphene.JSONString()
    live_stat = graphene.JSONString()
    last_stat = graphene.JSONString()

    @classmethod
    def parse_row(cls, ctx: GraphQueryContext, row: Row) -> Mapping[str, Any]:
        assert row is not None
        from .user import UserRole
        is_superadmin = (ctx.user['role'] == UserRole.SUPERADMIN)
        if is_superadmin:
            hide_agents = False
        else:
            hide_agents = ctx.local_config['manager']['hide-agents']
        return {
            # identity
            'id': row['id'],
            'idx': row['cluster_idx'],
            'role': row['cluster_role'],
            'hostname': row['cluster_hostname'],
            'cluster_idx': row['cluster_idx'],
            'cluster_role': row['cluster_role'],
            'cluster_hostname': row['cluster_hostname'],
            'session_id': row['session_id'],

            # image
            'image': row['image'],
            'registry': row['registry'],

            # status
            'status': row['status'].name,
            'status_changed': row['status_changed'],
            'status_info': row['status_info'],
            'status_data': row['status_data'],
            'created_at': row['created_at'],
            'terminated_at': row['terminated_at'],
            'starts_at': row['starts_at'],
            'occupied_slots': row['occupied_slots'].to_json(),

            # resources
            'agent': row['agent'] if not hide_agents else None,
            'container_id': row['container_id'] if not hide_agents else None,
            'resource_opts': row['resource_opts'],

            # statistics
            # live_stat is resolved by Graphene
            'last_stat': row['last_stat'],
        }

    @classmethod
    def from_row(cls, ctx: GraphQueryContext, row: Row) -> Optional[ComputeContainer]:
        if row is None:
            return None
        props = cls.parse_row(ctx, row)
        return cls(**props)

    async def resolve_live_stat(self, info: graphene.ResolveInfo) -> Optional[Mapping[str, Any]]:
        if not hasattr(self, 'status'):
            return None
        graph_ctx: GraphQueryContext = info.context
        if KernelStatus[self.status] in LIVE_STATUS:
            raw_live_stat = await redis.execute_with_retries(
                lambda: graph_ctx.redis_stat.get(str(self.id), encoding=None))
            if raw_live_stat is not None:
                live_stat = msgpack.unpackb(raw_live_stat)
                return live_stat
            return None
        else:
            return self.last_stat

    @classmethod
    async def load_count(
        cls,
        ctx: GraphQueryContext,
        session_id: SessionId,
        *,
        cluster_role: str = None,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: str = None,
    ) -> int:
        query = (
            sa.select([sa.func.count(kernels.c.id)])
            .select_from(kernels)
            .where(kernels.c.session_id == session_id)
        )
        if cluster_role is not None:
            query = query.where(kernels.c.cluster_role == cluster_role)
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(kernels.c.group_id == group_id)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        result = await ctx.db_conn.execute(query)
        return result.scalar()

    @classmethod
    async def load_slice(
        cls,
        ctx: GraphQueryContext,
        limit: int,
        offset: int,
        session_id: SessionId,
        *,
        cluster_role: str = None,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: AccessKey = None,
        order_key: str = None,
        order_asc: bool = True,
    ) -> Sequence[Optional[ComputeContainer]]:
        if order_key is None:
            _ordering = DEFAULT_SESSION_ORDERING
        else:
            _order_func = sa.asc if order_asc else sa.desc
            _ordering = [_order_func(getattr(kernels.c, order_key))]
        query = (
            sa.select([kernels])
            .select_from(kernels)
            .where(kernels.c.session_id == session_id)
            .order_by(*_ordering)
            .limit(limit)
            .offset(offset)
        )
        if cluster_role is not None:
            query = query.where(kernels.c.cluster_role == cluster_role)
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(kernels.c.group_id == group_id)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        return [cls.from_row(ctx, r) async for r in (await ctx.db_conn.stream(query))]

    @classmethod
    async def batch_load_by_session(
        cls,
        ctx: GraphQueryContext,
        session_ids: Sequence[SessionId],
    ) -> Sequence[Sequence[ComputeContainer]]:
        query = (
            sa.select([kernels])
            .select_from(kernels)
            # TODO: use "owner session ID" when we implement multi-container session
            .where(kernels.c.session_id.in_(session_ids))
        )
        return await batch_multiresult(
            ctx, ctx.db_conn, query, cls,
            session_ids, lambda row: row['session_id'],
        )

    @classmethod
    async def batch_load_detail(
        cls,
        ctx: GraphQueryContext,
        container_ids: Sequence[KernelId],
        *,
        domain_name: str = None,
        access_key: AccessKey = None,
    ) -> Sequence[Optional[ComputeContainer]]:
        j = (
            kernels
            .join(groups, groups.c.id == kernels.c.group_id)
            .join(users, users.c.uuid == kernels.c.user_uuid)
        )
        query = (
            sa.select([kernels])
            .select_from(j)
            .where(
                (kernels.c.id.in_(container_ids))
            ))
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        return await batch_result(
            ctx, ctx.db_conn, query, cls,
            container_ids, lambda row: row['id'],
        )


class ComputeSession(graphene.ObjectType):
    class Meta:
        interfaces = (Item, )

    # identity
    tag = graphene.String()
    name = graphene.String()
    type = graphene.String()
    session_id = graphene.UUID()

    # image
    image = graphene.String()     # image for the main container
    registry = graphene.String()  # image registry for the main container
    cluster_template = graphene.String()
    cluster_mode = graphene.String()
    cluster_size = graphene.Int()

    # ownership
    domain_name = graphene.String()
    group_name = graphene.String()
    group_id = graphene.UUID()
    user_email = graphene.String()
    user_id = graphene.UUID()
    access_key = graphene.String()
    created_user_email = graphene.String()
    created_user_id = graphene.UUID()

    # status
    status = graphene.String()
    status_changed = GQLDateTime()
    status_info = graphene.String()
    status_data = graphene.JSONString()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()
    starts_at = GQLDateTime()
    startup_command = graphene.String()
    result = graphene.String()

    # resources
    resource_opts = graphene.JSONString()
    scaling_group = graphene.String()
    service_ports = graphene.JSONString()
    mounts = graphene.List(lambda: graphene.String)
    occupied_slots = graphene.JSONString()

    # statistics
    num_queries = BigInt()

    # owned containers (aka kernels)
    containers = graphene.List(lambda: ComputeContainer)

    # relations
    dependencies = graphene.List(lambda: ComputeSession)

    @classmethod
    def parse_row(cls, ctx: GraphQueryContext, row: Row) -> Mapping[str, Any]:
        assert row is not None
        return {
            # identity
            'id': row['id'],
            'tag': row['tag'],
            'name': row['session_name'],
            'type': row['session_type'].name,
            'session_id': row['session_id'],

            # image
            'image': row['image'],
            'registry': row['registry'],
            'cluster_template': None,  # TODO: implement
            'cluster_mode': row['cluster_mode'],
            'cluster_size': row['cluster_size'],

            # ownership
            'domain_name': row['domain_name'],
            'group_name': row['group_name'],
            'group_id': row['group_id'],
            'user_email': row['email'],
            'user_id': row['user_uuid'],
            'access_key': row['access_key'],
            'created_user_email': None,  # TODO: implement
            'created_user_id': None,     # TODO: implement

            # status
            'status': row['status'].name,
            'status_changed': row['status_changed'],
            'status_info': row['status_info'],
            'status_data': row['status_data'],
            'created_at': row['created_at'],
            'terminated_at': row['terminated_at'],
            'starts_at': row['starts_at'],
            'startup_command': row['startup_command'],
            'result': row['result'].name,

            # resources
            'resource_opts': row['resource_opts'],
            'scaling_group': row['scaling_group'],
            'service_ports': row['service_ports'],
            'mounts': row['mounts'],

            # statistics
            'num_queries': row['num_queries'],
        }

    @classmethod
    def from_row(cls, ctx: GraphQueryContext, row: Row) -> ComputeSession | None:
        if row is None:
            return None
        props = cls.parse_row(ctx, row)
        return cls(**props)

    async def resolve_occupied_slots(self, info: graphene.ResolveInfo) -> Mapping[str, Any]:
        """
        Calculate the sum of occupied resource slots of all sub-kernels,
        and return the JSON-serializable object from the sum result.
        """
        graph_ctx: GraphQueryContext = info.context
        loader = graph_ctx.dataloader_manager.get_loader(graph_ctx, 'ComputeContainer.by_session')
        containers = await loader.load(self.session_id)
        zero = ResourceSlot()
        return sum(
            (ResourceSlot({
                SlotName(k): Decimal(v) for k, v in c.occupied_slots.items()
            }) for c in containers),
            start=zero,
        ).to_json()

    async def resolve_containers(
        self,
        info: graphene.ResolveInfo,
    ) -> Iterable[ComputeContainer]:
        graph_ctx: GraphQueryContext = info.context
        loader = graph_ctx.dataloader_manager.get_loader(graph_ctx, 'ComputeContainer.by_session')
        return await loader.load(self.session_id)

    async def resolve_dependencies(
        self,
        info: graphene.ResolveInfo,
    ) -> Iterable[ComputeSession]:
        graph_ctx: GraphQueryContext = info.context
        loader = graph_ctx.dataloader_manager.get_loader(graph_ctx, 'ComputeSession.by_dependency')
        return await loader.load(self.id)

    @classmethod
    async def load_count(
        cls,
        ctx: GraphQueryContext,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: str = None,
        status: str = None,
    ) -> int:
        if isinstance(status, str):
            status_list = [KernelStatus[s] for s in status.split(',')]
        elif isinstance(status, KernelStatus):
            status_list = [status]
        query = (
            sa.select([sa.func.count(kernels.c.id)])
            .select_from(kernels)
            .where(kernels.c.cluster_role == DEFAULT_ROLE)
        )
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(kernels.c.group_id == group_id)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        if status is not None:
            query = query.where(kernels.c.status.in_(status_list))
        result = await ctx.db_conn.execute(query)
        return result.scalar()

    @classmethod
    async def load_slice(
        cls,
        ctx: GraphQueryContext,
        limit: int,
        offset: int,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: str = None,
        status: str = None,
        order_key: str = None,
        order_asc: bool = True,
    ) -> Sequence[ComputeSession | None]:
        if isinstance(status, str):
            status_list = [KernelStatus[s] for s in status.split(',')]
        elif isinstance(status, KernelStatus):
            status_list = [status]
        if order_key is None:
            _ordering = DEFAULT_SESSION_ORDERING
        else:
            _order_func = sa.asc if order_asc else sa.desc
            _ordering = [_order_func(getattr(kernels.c, order_key))]
        j = (
            kernels
            .join(groups, groups.c.id == kernels.c.group_id)
            .join(users, users.c.uuid == kernels.c.user_uuid)
        )
        query = (
            sa.select([
                kernels,
                groups.c.name.label('group_name'),
                users.c.email,
            ])
            .select_from(j)
            .where(kernels.c.cluster_role == DEFAULT_ROLE)
            .order_by(*_ordering)
            .limit(limit)
            .offset(offset)
        )
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(kernels.c.group_id == group_id)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        if status is not None:
            query = query.where(kernels.c.status.in_(status_list))
        return [cls.from_row(ctx, r) async for r in (await ctx.db_conn.stream(query))]

    @classmethod
    async def batch_load_by_dependency(
        cls,
        ctx: GraphQueryContext,
        session_ids: Sequence[SessionId],
    ) -> Sequence[Sequence[ComputeSession]]:
        j = sa.join(
            kernels, session_dependencies,
            kernels.c.session_id == session_dependencies.c.depends_on,
        )
        query = (
            sa.select([kernels])
            .select_from(j)
            .where(
                (kernels.c.cluster_role == DEFAULT_ROLE) &
                (session_dependencies.c.session_id.in_(session_ids))
            )
        )
        return await batch_multiresult(
            ctx, ctx.db_conn, query, cls,
            session_ids, lambda row: row['id'],
        )

    @classmethod
    async def batch_load_detail(
        cls,
        ctx: GraphQueryContext,
        session_ids: Sequence[SessionId],
        *,
        domain_name: str = None,
        access_key: str = None,
    ) -> Sequence[ComputeSession | None]:
        j = (
            kernels
            .join(groups, groups.c.id == kernels.c.group_id)
            .join(users, users.c.uuid == kernels.c.user_uuid)
        )
        query = (
            sa.select([
                kernels,
                groups.c.name.label('group_name'),
                users.c.email,
            ])
            .select_from(j)
            .where(
                (kernels.c.cluster_role == DEFAULT_ROLE) &
                (kernels.c.id.in_(session_ids))
            ))
        if domain_name is not None:
            query = query.where(kernels.c.domain_name == domain_name)
        if access_key is not None:
            query = query.where(kernels.c.access_key == access_key)
        return await batch_result(
            ctx, ctx.db_conn, query, cls,
            session_ids, lambda row: row['id'],
        )


class ComputeContainerList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(ComputeContainer, required=True)


class ComputeSessionList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(ComputeSession, required=True)


# --------- pre-v5 legacy -----------

MetricValueType = TypeVar('MetricValueType', int, float)


class LegacyComputeSession(graphene.ObjectType):
    """
    Represents a main session.
    """
    class Meta:
        interfaces = (Item, )

    tag = graphene.String()  # Only for ComputeSession
    sess_id = graphene.String()    # legacy
    sess_type = graphene.String()  # legacy
    session_name = graphene.String()
    session_type = graphene.String()
    role = graphene.String()
    image = graphene.String()
    registry = graphene.String()
    domain_name = graphene.String()
    group_name = graphene.String()
    group_id = graphene.UUID()
    scaling_group = graphene.String()
    user_uuid = graphene.UUID()
    access_key = graphene.String()

    status = graphene.String()
    status_changed = GQLDateTime()
    status_info = graphene.String()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()
    startup_command = graphene.String()
    result = graphene.String()

    # hidable fields by configuration
    agent = graphene.String()
    container_id = graphene.String()

    service_ports = graphene.JSONString()

    occupied_slots = graphene.JSONString()
    occupied_shares = graphene.JSONString()
    mounts = graphene.List(lambda: graphene.List(lambda: graphene.String))
    resource_opts = graphene.JSONString()

    num_queries = BigInt()
    live_stat = graphene.JSONString()
    last_stat = graphene.JSONString()

    user_email = graphene.String()

    # Legacy fields
    lang = graphene.String()
    mem_slot = graphene.Int()
    cpu_slot = graphene.Float()
    gpu_slot = graphene.Float()
    tpu_slot = graphene.Float()
    cpu_used = BigInt()
    cpu_using = graphene.Float()
    mem_max_bytes = BigInt()
    mem_cur_bytes = BigInt()
    net_rx_bytes = BigInt()
    net_tx_bytes = BigInt()
    io_read_bytes = BigInt()
    io_write_bytes = BigInt()
    io_max_scratch_size = BigInt()
    io_cur_scratch_size = BigInt()

    @classmethod
    async def _resolve_live_stat(
        cls,
        redis_stat: Redis,
        kernel_id: str,
    ) -> Optional[Mapping[str, Any]]:
        cstat = await redis.execute_with_retries(
            lambda: redis_stat.get(kernel_id, encoding=None))
        if cstat is not None:
            cstat = msgpack.unpackb(cstat)
        return cstat

    async def resolve_live_stat(self, info: graphene.ResolveInfo) -> Optional[Mapping[str, Any]]:
        if not hasattr(self, 'status'):
            return None
        graph_ctx: GraphQueryContext = info.context
        if KernelStatus[self.status] not in LIVE_STATUS:
            return self.last_stat
        else:
            return await type(self)._resolve_live_stat(graph_ctx.redis_stat, str(self.id))

    async def _resolve_legacy_metric(
        self,
        info: graphene.ResolveInfo,
        metric_key: str,
        metric_field: str,
        convert_type: Type[MetricValueType],
    ) -> Optional[MetricValueType]:
        if not hasattr(self, 'status'):
            return None
        graph_ctx: GraphQueryContext = info.context
        if KernelStatus[self.status] not in LIVE_STATUS:
            if self.last_stat is None:
                return convert_type(0)
            metric = self.last_stat.get(metric_key)
            if metric is None:
                return convert_type(0)
            value = metric.get(metric_field)
            if value is None:
                return convert_type(0)
            return convert_type(value)
        else:
            kstat = await type(self)._resolve_live_stat(graph_ctx.redis_stat, str(self.id))
            if kstat is None:
                return convert_type(0)
            metric = kstat.get(metric_key)
            if metric is None:
                return convert_type(0)
            value = metric.get(metric_field)
            if value is None:
                return convert_type(0)
            return convert_type(value)

    async def resolve_cpu_used(self, info: graphene.ResolveInfo) -> Optional[float]:
        return await self._resolve_legacy_metric(info, 'cpu_used', 'current', float)

    async def resolve_cpu_using(self, info: graphene.ResolveInfo) -> Optional[float]:
        return await self._resolve_legacy_metric(info, 'cpu_util', 'pct', float)

    async def resolve_mem_max_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'mem', 'stats.max', int)

    async def resolve_mem_cur_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'mem', 'current', int)

    async def resolve_net_rx_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'net_rx', 'stats.rate', int)

    async def resolve_net_tx_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'net_tx', 'stats.rate', int)

    async def resolve_io_read_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'io_read', 'current', int)

    async def resolve_io_write_bytes(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'io_write', 'current', int)

    async def resolve_io_max_scratch_size(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'io_scratch_size', 'stats.max', int)

    async def resolve_io_cur_scratch_size(self, info: graphene.ResolveInfo) -> Optional[int]:
        return await self._resolve_legacy_metric(info, 'io_scratch_size', 'current', int)

    @classmethod
    def parse_row(cls, ctx: GraphQueryContext, row: Row) -> Mapping[str, Any]:
        assert row is not None
        from .user import UserRole
        mega = 2 ** 20
        is_superadmin = (ctx.user['role'] == UserRole.SUPERADMIN)
        if is_superadmin:
            hide_agents = False
        else:
            hide_agents = ctx.local_config['manager']['hide-agents']
        return {
            'id': row['id'],
            'sess_id': row['session_name'],         # legacy, will be deprecated
            'sess_type': row['session_type'].name,  # legacy, will be deprecated
            'session_name': row['session_name'],
            'session_type': row['session_type'].name,
            'role': row['cluster_role'],
            'tag': row['tag'],
            'image': row['image'],
            'registry': row['registry'],
            'domain_name': row['domain_name'],
            'group_name': row['name'],  # group.name (group is omitted since use_labels=True is not used)
            'group_id': row['group_id'],
            'scaling_group': row['scaling_group'],
            'user_uuid': row['user_uuid'],
            'access_key': row['access_key'],
            'status': row['status'].name,
            'status_changed': row['status_changed'],
            'status_info': row['status_info'],
            'status_data': row['status_data'],
            'created_at': row['created_at'],
            'terminated_at': row['terminated_at'],
            'startup_command': row['startup_command'],
            'result': row['result'].name,
            'service_ports': row['service_ports'],
            'occupied_slots': row['occupied_slots'].to_json(),
            'mounts': row['mounts'],
            'resource_opts': row['resource_opts'],
            'num_queries': row['num_queries'],
            # optionally hidden
            'agent': row['agent'] if not hide_agents else None,
            'container_id': row['container_id'] if not hide_agents else None,
            # live_stat is resolved by Graphene
            'last_stat': row['last_stat'],
            'user_email': row['email'],
            # Legacy fields
            # NOTE: currently graphene always uses resolve methods!
            'cpu_used': 0,
            'mem_max_bytes': 0,
            'mem_cur_bytes': 0,
            'net_rx_bytes': 0,
            'net_tx_bytes': 0,
            'io_read_bytes': 0,
            'io_write_bytes': 0,
            'io_max_scratch_size': 0,
            'io_cur_scratch_size': 0,
            'lang': row['image'],
            'occupied_shares': row['occupied_shares'],
            'mem_slot': BinarySize.from_str(
                row['occupied_slots'].get('mem', 0)) // mega,
            'cpu_slot': float(row['occupied_slots'].get('cpu', 0)),
            'gpu_slot': float(row['occupied_slots'].get('cuda.device', 0)),
            'tpu_slot': float(row['occupied_slots'].get('tpu.device', 0)),
        }

    @classmethod
    def from_row(cls, context: GraphQueryContext, row: Row) -> Optional[LegacyComputeSession]:
        if row is None:
            return None
        props = cls.parse_row(context, row)
        return cls(**props)

    @classmethod
    async def load_count(
        cls,
        ctx: GraphQueryContext,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: AccessKey = None,
        status: str = None,
    ) -> int:
        if isinstance(status, str):
            status_list = [KernelStatus[s] for s in status.split(',')]
        elif isinstance(status, KernelStatus):
            status_list = [status]
        async with ctx.db.begin() as conn:
            query = (
                sa.select([sa.func.count(kernels.c.session_id)])
                .select_from(kernels)
                .where(kernels.c.cluster_role == DEFAULT_ROLE)
            )
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if status is not None:
                query = query.where(kernels.c.status.in_(status_list))
            result = await conn.execute(query)
            return result.scalar()

    @classmethod
    async def load_slice(
        cls,
        ctx: GraphQueryContext,
        limit: int,
        offset: int,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        access_key: AccessKey = None,
        status: str = None,
        order_key: str = None,
        order_asc: bool = True
    ) -> Sequence[LegacyComputeSession]:
        if isinstance(status, str):
            status_list = [KernelStatus[s] for s in status.split(',')]
        elif isinstance(status, KernelStatus):
            status_list = [status]
        async with ctx.db.begin() as conn:
            if order_key is None:
                _ordering = DEFAULT_SESSION_ORDERING
            else:
                _order_func = sa.asc if order_asc else sa.desc
                _ordering = [_order_func(getattr(kernels.c, order_key))]
            j = (kernels.join(groups, groups.c.id == kernels.c.group_id)
                        .join(users, users.c.uuid == kernels.c.user_uuid))
            query = (
                sa.select([kernels, groups.c.name, users.c.email])
                .select_from(j)
                .where(kernels.c.cluster_role == DEFAULT_ROLE)
                .order_by(*_ordering)
                .limit(limit)
                .offset(offset)
            )
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if status is not None:
                query = query.where(kernels.c.status.in_(status_list))
            return [
                obj async for r in (await conn.stream(query))
                if (obj := cls.from_row(ctx, r)) is not None
            ]

    @classmethod
    async def batch_load(
        cls,
        ctx: GraphQueryContext,
        access_keys: AccessKey,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        status: str = None,
    ) -> Sequence[Optional[LegacyComputeSession]]:
        async with ctx.db.begin() as conn:
            j = (kernels.join(groups, groups.c.id == kernels.c.group_id)
                        .join(users, users.c.uuid == kernels.c.user_uuid))
            query = (
                sa.select([kernels, groups.c.name, users.c.email])
                .select_from(j)
                .where(
                    (kernels.c.access_key.in_(access_keys)) &
                    (kernels.c.cluster_role == DEFAULT_ROLE)
                )
                .order_by(
                    sa.desc(sa.func.greatest(
                        kernels.c.created_at,
                        kernels.c.terminated_at,
                        kernels.c.status_changed,
                    ))
                )
                .limit(100))
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            if status is not None:
                query = query.where(kernels.c.status == status)
            return await batch_result(
                ctx, conn, query, cls,
                access_keys, lambda row: row['access_key'],
            )

    @classmethod
    async def batch_load_detail(
        cls,
        ctx: GraphQueryContext,
        sess_ids: Sequence[SessionId],
        *,
        domain_name: str = None,
        access_key: AccessKey = None,
        status: str = None,
    ) -> Sequence[Sequence[LegacyComputeSession]]:
        async with ctx.db.begin() as conn:
            status_list = []
            if isinstance(status, str):
                status_list = [KernelStatus[s] for s in status.split(',')]
            elif isinstance(status, KernelStatus):
                status_list = [status]
            elif status is None:
                status_list = [KernelStatus['RUNNING']]
            j = (kernels.join(groups, groups.c.id == kernels.c.group_id)
                        .join(users, users.c.uuid == kernels.c.user_uuid))
            query = (sa.select([kernels, groups.c.name, users.c.email])
                       .select_from(j)
                       .where((kernels.c.cluster_role == DEFAULT_ROLE) &
                              (kernels.c.session_id.in_(sess_ids))))
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if status_list:
                query = query.where(kernels.c.status.in_(status_list))
            return await batch_multiresult(
                ctx, conn, query, cls,
                sess_ids, lambda row: row['session_name'],
            )


class LegacyComputeSessionList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(LegacyComputeSession, required=True)


async def recalc_concurrency_used(db_conn: SAConnection, access_key: AccessKey) -> None:
    query = (
        sa.update(keypairs)
        .values(
            concurrency_used=(
                sa.select([sa.func.count(kernels.c.id)])
                .select_from(kernels)
                .where(
                    (kernels.c.access_key == access_key) &
                    (kernels.c.status.in_(USER_RESOURCE_OCCUPYING_KERNEL_STATUSES))
                )
                .scalar_subquery()
            ),
        )
        .where(keypairs.c.access_key == access_key)
    )
    await db_conn.execute(query)
