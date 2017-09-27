from collections import namedtuple
import enum
import sqlalchemy as sa

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa

from .base import metadata, EnumType

__all__ = (
    'agents', 'AgentStatus', 'ResourceSlot',
    'Agent',
)

ResourceSlot = namedtuple('ResourceSlot', 'id mem cpu gpu')


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

    sa.Column('mem_slots', sa.Integer(), nullable=False),        # in the unit of 256 MBytes
    sa.Column('cpu_slots', sa.Integer(), nullable=False),        # 2 * number of cores
    sa.Column('gpu_slots', sa.Integer(), nullable=False),        # 2 * number of GPU devices

    sa.Column('used_mem_slots', sa.Integer(), nullable=False),
    sa.Column('used_cpu_slots', sa.Integer(), nullable=False),
    sa.Column('used_gpu_slots', sa.Integer(), nullable=False),

    sa.Column('addr', sa.String(length=128), nullable=False),
    sa.Column('first_contact', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('lost_at', sa.DateTime(timezone=True), nullable=True),
)


class Agent(graphene.ObjectType):

    id = graphene.String()
    status = graphene.String()
    mem_slots = graphene.Int()
    cpu_slots = graphene.Int()
    gpu_slots = graphene.Int()
    used_mem_slots = graphene.Int()
    used_cpu_slots = graphene.Int()
    used_gpu_slots = graphene.Int()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()

    computations = graphene.List(
        'sorna.manager.models.Computation',
        status=graphene.String())

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row.id,
            status=row.status,
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
