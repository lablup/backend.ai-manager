from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.types import ResourceSlot, BinarySize
from .base import metadata, EnumType, Item, PaginatedList

__all__ = (
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
    sa.Column('region', sa.String(length=64), index=True, nullable=False),

    sa.Column('available_slots', pgsql.JSONB(), nullable=False),
    sa.Column('occupied_slots', pgsql.JSONB(), nullable=False),

    sa.Column('addr', sa.String(length=128), nullable=False),
    sa.Column('first_contact', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('lost_at', sa.DateTime(timezone=True), nullable=True),
)


class Agent(graphene.ObjectType):

    class Meta:
        interfaces = (Item, )

    id = graphene.ID()
    status = graphene.String()
    region = graphene.String()
    available_slots = graphene.JSONString()
    occupied_slots = graphene.JSONString()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()

    # Legacy fields
    mem_slots = graphene.Int()
    cpu_slots = graphene.Float()
    gpu_slots = graphene.Float()
    tpu_slots = graphene.Float()
    used_mem_slots = graphene.Int()
    used_cpu_slots = graphene.Float()
    used_gpu_slots = graphene.Float()
    used_tpu_slots = graphene.Float()

    # TODO: Dynamic fields
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
            status=row['status'],
            region=row['region'],
            available_slots=(ResourceSlot(row['available_slots'])
                             .as_json_humanized(context['known_slot_types'])),
            occupied_slots=(ResourceSlot(row['occupied_slots'])
                            .as_json_humanized(context['known_slot_types'])),
            addr=row['addr'],
            first_contact=row['first_contact'],
            lost_at=row['lost_at'],
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

    async def resolve_computations(self, info, status=None):
        '''
        Retrieves all children worker sessions run by this agent.
        '''
        manager = info.context['dlmgr']
        loader = manager.get_loader('Computation.by_agent_id', status=status)
        return await loader.load(self.id)

    @staticmethod
    async def load_count(context, status=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([sa.func.count(agents.c.id)])
                       .select_from(agents)
                       .as_scalar())
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            result = await conn.execute(query)
            count = await result.fetchone()
            return count[0]

    @staticmethod
    async def load_slice(context, limit, offset, status=None):
        async with context['dbpool'].acquire() as conn:
            # TODO: optimization for pagination using subquery, join
            query = (sa.select('*')
                       .select_from(agents)
                       .order_by(agents.c.id)
                       .limit(limit)
                       .offset(offset))
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            result = await conn.execute(query)
            rows = await result.fetchall()
            _agents = []
            for r in rows:
                _agent = Agent.from_row(context, r)
                await Agent._append_dynamic_fields(context, _agent)
                _agents.append(_agent)
            return _agents

    async def _append_dynamic_fields(context, agent):
        rs = context['redis_stat']
        cpu_pct, mem_cur_bytes = await rs.hmget(
            str(agent.id),
            'cpu_pct', 'mem_cur_bytes',
        )
        agent.cpu_cur_pct = cpu_pct
        agent.mem_cur_bytes = mem_cur_bytes

    @staticmethod
    async def load_all(context, status=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(agents))
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            result = await conn.execute(query)
            rows = await result.fetchall()
            _agents = []
            for r in rows:
                _agent = Agent.from_row(context, r)
                await Agent._append_dynamic_fields(context, _agent)
                _agents.append(_agent)
            return _agents

    @staticmethod
    async def batch_load(context, agent_ids, status=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
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
                await Agent._append_dynamic_fields(context, o)
                objs_per_key[row.id] = o
        return tuple(objs_per_key.values())


class AgentList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(Agent, required=True)