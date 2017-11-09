from collections import OrderedDict
import enum
from typing import NamedTuple, Optional

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa

from .base import metadata, EnumType

__all__ = (
    'agents', 'AgentStatus', 'ResourceSlot',
    'Agent',
)


class ResourceSlot(NamedTuple):
    id: Optional[str] = None
    mem: int = 0      # MiBytes
    cpu: float = 0.0  # fraction of CPU cores
    gpu: float = 0.0  # fraction of GPU devices


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

    sa.Column('mem_slots', sa.BigInteger(), nullable=False),  # MiBytes
    sa.Column('cpu_slots', sa.Float(), nullable=False),  # number of CPU cores
    sa.Column('gpu_slots', sa.Float(), nullable=False),  # number of GPU devices

    sa.Column('used_mem_slots', sa.BigInteger(), nullable=False),
    sa.Column('used_cpu_slots', sa.Float(), nullable=False),
    sa.Column('used_gpu_slots', sa.Float(), nullable=False),

    sa.Column('addr', sa.String(length=128), nullable=False),
    sa.Column('first_contact', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('lost_at', sa.DateTime(timezone=True), nullable=True),
)


class Agent(graphene.ObjectType):

    id = graphene.String()
    status = graphene.String()
    region = graphene.String()
    mem_slots = graphene.Int()
    cpu_slots = graphene.Float()
    gpu_slots = graphene.Float()
    used_mem_slots = graphene.Int()
    used_cpu_slots = graphene.Float()
    used_gpu_slots = graphene.Float()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()

    computations = graphene.List(
        'ai.backend.manager.models.Computation',
        status=graphene.String())

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row.id,
            status=row.status,
            region=row.region,
            mem_slots=row.mem_slots,
            cpu_slots=row.cpu_slots,
            gpu_slots=row.gpu_slots,
            used_mem_slots=row.used_mem_slots,
            used_cpu_slots=row.used_cpu_slots,
            used_gpu_slots=row.used_gpu_slots,
            addr=row.addr,
            first_contact=row.first_contact,
            lost_at=row.lost_at,
        )

    async def resolve_computations(self, info, status=None):
        '''
        Retrieves all children worker sessions run by this agent.
        '''
        manager = info.context['dlmgr']
        loader = manager.get_loader('Computation.by_agent_id', status=status)
        return await loader.load(self.id)

    @staticmethod
    async def load_all(dbpool, status=None):
        async with dbpool.acquire() as conn:
            query = (sa.select('*')
                       .select_from(agents))
            if status is not None:
                status = AgentStatus[status]
                query = query.where(agents.c.status == status)
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [Agent.from_row(r) for r in rows]

    @staticmethod
    async def batch_load(dbpool, agent_ids):
        async with dbpool.acquire() as conn:
            query = (sa.select('*')
                       .select_from(agents)
                       .where(agents.c.id.in_(agent_ids))
                       .order_by(agents.c.id))
            objs_per_key = OrderedDict()
            for k in agent_ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Agent.from_row(row)
                objs_per_key[row.id] = o
        return tuple(objs_per_key.values())
