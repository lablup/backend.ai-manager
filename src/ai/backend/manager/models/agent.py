from __future__ import annotations

import enum
from typing import (
    Any,
    Optional,
    Mapping,
    Sequence,
    TYPE_CHECKING,
)

from aiopg.sa.connection import SAConnection
from aiopg.sa.result import RowProxy
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.sql.expression import true
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common import msgpack, redis
from ai.backend.common.types import AgentId, BinarySize, ResourceSlot
from .kernel import AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES, kernels
from .base import (
    metadata,
    batch_result,
    EnumType, Item, PaginatedList,
    ResourceSlotColumn,
)
if TYPE_CHECKING:
    from ..registry import AgentRegistry

__all__: Sequence[str] = (
    'agents', 'AgentStatus',
    'AgentList', 'Agent',
    'recalc_agent_resource_occupancy',
)


class AgentStatus(enum.Enum):
    ALIVE = 0
    LOST = 1
    RESTARTING = 2
    TERMINATED = 3


agents = sa.Table(
    'agents', metadata,
    sa.Column('id', sa.String(length=64), primary_key=True),
    sa.Column('status', EnumType(AgentStatus), nullable=False, index=True,
              default=AgentStatus.ALIVE),
    sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True),
    sa.Column('region', sa.String(length=64), index=True, nullable=False),
    sa.Column('scaling_group', sa.ForeignKey('scaling_groups.name'), index=True,
              nullable=False, server_default='default', default='default'),
    sa.Column('schedulable', sa.Boolean(),
              nullable=False, server_default=true(), default=True),

    sa.Column('available_slots', ResourceSlotColumn(), nullable=False),
    sa.Column('occupied_slots', ResourceSlotColumn(), nullable=False),

    sa.Column('addr', sa.String(length=128), nullable=False),
    sa.Column('first_contact', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('lost_at', sa.DateTime(timezone=True), nullable=True),

    sa.Column('version', sa.String(length=64), nullable=False),
    sa.Column('compute_plugins', pgsql.JSONB(), nullable=False, default={}),
)


class Agent(graphene.ObjectType):

    class Meta:
        interfaces = (Item, )

    status = graphene.String()
    status_changed = GQLDateTime()
    region = graphene.String()
    scaling_group = graphene.String()
    schedulable = graphene.Boolean()
    available_slots = graphene.JSONString()
    occupied_slots = graphene.JSONString()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()
    live_stat = graphene.JSONString()
    version = graphene.String()
    compute_plugins = graphene.JSONString()
    hardware_metadata = graphene.JSONString()

    # Legacy fields
    mem_slots = graphene.Int()
    cpu_slots = graphene.Float()
    gpu_slots = graphene.Float()
    tpu_slots = graphene.Float()
    used_mem_slots = graphene.Int()
    used_cpu_slots = graphene.Float()
    used_gpu_slots = graphene.Float()
    used_tpu_slots = graphene.Float()
    cpu_cur_pct = graphene.Float()
    mem_cur_bytes = graphene.Float()

    compute_containers = graphene.List(
        'ai.backend.manager.models.ComputeContainer',
        status=graphene.String())

    @classmethod
    def from_row(
        cls,
        context: Mapping[str, Any],
        row: RowProxy,
    ) -> Agent:
        mega = 2 ** 20
        return cls(
            id=row['id'],
            status=row['status'].name,
            status_changed=row['status_changed'],
            region=row['region'],
            scaling_group=row['scaling_group'],
            schedulable=row['schedulable'],
            available_slots=row['available_slots'].to_json(),
            occupied_slots=row['occupied_slots'].to_json(),
            addr=row['addr'],
            first_contact=row['first_contact'],
            lost_at=row['lost_at'],
            version=row['version'],
            compute_plugins=row['compute_plugins'],
            # legacy fields
            mem_slots=BinarySize.from_str(row['available_slots']['mem']) // mega,
            cpu_slots=row['available_slots']['cpu'],
            gpu_slots=row['available_slots'].get('cuda.device', 0),
            tpu_slots=row['available_slots'].get('tpu.device', 0),
            used_mem_slots=BinarySize.from_str(
                row['occupied_slots'].get('mem', 0)) // mega,
            used_cpu_slots=float(row['occupied_slots'].get('cpu', 0)),
            used_gpu_slots=float(row['occupied_slots'].get('cuda.device', 0)),
            used_tpu_slots=float(row['occupied_slots'].get('tpu.device', 0)),
        )

    async def resolve_live_stat(self, info):
        rs = info.context['redis_stat']
        live_stat = await redis.execute_with_retries(
            lambda: rs.get(str(self.id), encoding=None))
        if live_stat is not None:
            live_stat = msgpack.unpackb(live_stat)
        return live_stat

    async def resolve_cpu_cur_pct(self, info):
        rs = info.context['redis_stat']
        live_stat = await redis.execute_with_retries(
            lambda: rs.get(str(self.id), encoding=None))
        if live_stat is not None:
            live_stat = msgpack.unpackb(live_stat)
            try:
                return float(live_stat['node']['cpu_util']['pct'])
            except (KeyError, TypeError, ValueError):
                return 0.0
        return 0.0

    async def resolve_mem_cur_bytes(self, info):
        rs = info.context['redis_stat']
        live_stat = await redis.execute_with_retries(
            lambda: rs.get(str(self.id), encoding=None))
        if live_stat is not None:
            live_stat = msgpack.unpackb(live_stat)
            try:
                return int(live_stat['node']['mem']['current'])
            except (KeyError, TypeError, ValueError):
                return 0
        return 0

    async def resolve_computations(self, info, status=None):
        '''
        Retrieves all children worker sessions run by this agent.
        '''
        manager = info.context['dlmgr']
        loader = manager.get_loader('Computation.by_agent_id', status=status)
        return await loader.load(self.id)

    async def resolve_hardware_metadata(self, info):
        registry: AgentRegistry = info.context['registry']
        return await registry.gather_agent_hwinfo(self.id)

    @staticmethod
    async def load_count(
        context, *,
        scaling_group=None,
        status=None,
    ) -> int:
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([sa.func.count(agents.c.id)])
                .select_from(agents)
                .as_scalar()
            )
            if scaling_group is not None:
                query = query.where(agents.c.scaling_group == scaling_group)
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            result = await conn.execute(query)
            return await result.scalar()

    @classmethod
    async def load_slice(
        cls, context, limit, offset, *,
        scaling_group=None,
        status=None,
        order_key=None,
        order_asc=True,
    ) -> Sequence[Agent]:
        async with context['dbpool'].acquire() as conn:
            # TODO: optimization for pagination using subquery, join
            if order_key is None:
                _ordering = agents.c.id
            else:
                _order_func = sa.asc if order_asc else sa.desc
                _ordering = _order_func(getattr(agents.c, order_key))
            query = (
                sa.select([agents])
                .select_from(agents)
                .order_by(_ordering)
                .limit(limit)
                .offset(offset)
            )
            if scaling_group is not None:
                query = query.where(agents.c.scaling_group == scaling_group)
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            return [
                cls.from_row(context, row) async for row in conn.execute(query)
            ]

    @classmethod
    async def load_all(
        cls, context, *,
        scaling_group=None,
        status=None,
    ) -> Sequence[Agent]:
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([agents])
                .select_from(agents)
            )
            if scaling_group is not None:
                query = query.where(agents.c.scaling_group == scaling_group)
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            return [
                cls.from_row(context, row) async for row in conn.execute(query)
            ]

    @classmethod
    async def batch_load(
        cls, context, agent_ids, *,
        status=None,
    ) -> Sequence[Optional[Agent]]:
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([agents])
                       .select_from(agents)
                       .where(agents.c.id.in_(agent_ids))
                       .order_by(agents.c.id))
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            return await batch_result(
                context, conn, query, cls,
                agent_ids, lambda row: row['id'],
            )


class AgentList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(Agent, required=True)


async def recalc_agent_resource_occupancy(db_conn: SAConnection, agent_id: AgentId) -> None:
    query = (
        sa.select([
            kernels.c.occupied_slots,
        ])
        .select_from(kernels)
        .where(
            (kernels.c.agent == agent_id) &
            (kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES))
        )
    )
    occupied_slots = ResourceSlot()
    result = await db_conn.execute(query)
    async for row in result:
        occupied_slots += row['occupied_slots']
    query = (
        sa.update(agents)
        .values({
            'occupied_slots': occupied_slots,
        })
        .where(agents.c.id == agent_id)
    )
    await db_conn.execute(query)
