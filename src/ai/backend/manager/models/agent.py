from collections import OrderedDict
import enum
from typing import Sequence

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common import msgpack, redis
from ai.backend.common.types import BinarySize
from .base import (
    metadata,
    EnumType, Item, PaginatedList,
    ResourceSlotColumn,
)

__all__: Sequence[str] = (
    'agents', 'AgentStatus',
    'AgentList', 'Agent',
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

    id = graphene.ID()
    status = graphene.String()
    status_changed = GQLDateTime()
    region = graphene.String()
    scaling_group = graphene.String()
    available_slots = graphene.JSONString()
    occupied_slots = graphene.JSONString()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()
    live_stat = graphene.JSONString()
    version = graphene.String()
    compute_plugins = graphene.JSONString()

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

    computations = graphene.List(
        'ai.backend.manager.models.Computation',
        status=graphene.String())

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        mega = 2 ** 20
        return cls(
            id=row['id'],
            status=row['status'].name,
            status_changed=row['status_changed'],
            region=row['region'],
            scaling_group=row['scaling_group'],
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

    @staticmethod
    async def load_count(context, *,
                         scaling_group=None,
                         status=None):
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
            count = await result.fetchone()
            return count[0]

    @staticmethod
    async def load_slice(context, limit, offset, *,
                         scaling_group=None,
                         status=None,
                         order_key=None, order_asc=True):
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
            result = await conn.execute(query)
            rows = await result.fetchall()
            _agents = []
            for r in rows:
                _agent = Agent.from_row(context, r)
                _agents.append(_agent)
            return _agents

    @staticmethod
    async def load_all(context, *,
                       scaling_group=None, status=None):
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
            result = await conn.execute(query)
            rows = await result.fetchall()
            _agents = []
            for r in rows:
                _agent = Agent.from_row(context, r)
                _agents.append(_agent)
            return _agents

    @staticmethod
    async def batch_load(context, agent_ids, *, status=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([agents])
                       .select_from(agents)
                       .where(agents.c.id.in_(agent_ids))
                       .order_by(agents.c.id))
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            objs_per_key = OrderedDict()
            for k in agent_ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Agent.from_row(context, row)
                objs_per_key[row.id] = o
        return tuple(objs_per_key.values())


class AgentList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(Agent, required=True)
