from collections import OrderedDict, UserDict
from decimal import Decimal
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.types import BinarySize
from .base import metadata, EnumType

__all__ = (
    'agents', 'AgentStatus', 'ResourceSlot',
    'Agent',
)


class ResourceSlot(UserDict):

    __slots__ = ('data', 'numeric')

    def __init__(self, *args, numeric=False):
        super().__init__(*args)
        self.numeric = numeric

    def __add__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be added together.')
        return type(self)({
            k: self.get(k, 0) + other.get(k, 0)
            for k in (self.keys() | other.keys())
        }, numeric=True)

    def __sub__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be operands of subtraction.')
        if other.keys() - self.keys():
            raise ValueError('Cannot subtract resource slot with more keys!')
        return type(self)({
            k: self.data[k] - other.get(k, 0)
            for k in self.keys()
        }, numeric=True)

    def __eq__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() != other.keys():
            raise TypeError('Only slots with same keys can be compared.')
        self_values = [self.data[k] for k in sorted(self.data.keys())]
        other_values = [other.data[k] for k in sorted(other.data.keys())]
        return self_values == other_values

    def __ne__(self, other):
        return not self.__eq__(other)

    def eq_contains(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot contain other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def eq_contained(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be contained in other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def __le__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return not any(s > o for s, o in zip(self_values, other_values))

    def __lt__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return (not any(s > o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def __ge__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return not any(s < o for s, o in zip(self_values, other_values))

    def __gt__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return (not any(s < o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def as_numeric(self, slot_types, default_slot_type=None):
        data = {}
        for k, v in self.data.items():
            unit = slot_types.get(k, default_slot_type)
            if unit is None:
                raise ValueError('uknown slot type', k)
            elif unit == 'bytes':
                v = BinarySize.from_str(v)
            else:
                v = Decimal(v)
            data[k] = v
        return type(self)(data, numeric=True)

    def as_humanized(self, slot_types):
        data = {}
        for k, v in self.data.items():
            unit = slot_types.get(k, 'count')
            if unit == 'bytes':
                v = '{:g}'.format(BinarySize(v))
            else:
                v = str(v)
            data[k] = v
        return type(self)(data, numeric=False)

    # legacy:
    # mem: Decimal = Decimal(0)  # multiple of GiBytes
    # cpu: Decimal = Decimal(0)  # multiple of CPU cores
    # accel_slots: Optional[Mapping[str, Decimal]] = None


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
    id = graphene.String()
    status = graphene.String()
    region = graphene.String()
    available_slots = graphene.JSONString()
    occupied_slots = graphene.JSONString()
    addr = graphene.String()
    first_contact = GQLDateTime()
    lost_at = GQLDateTime()

    # TODO: Dynamic fields
    cpu_cur_pct = graphene.Float()
    mem_cur_bytes = graphene.Float()

    computations = graphene.List(
        'ai.backend.manager.models.Computation',
        status=graphene.String())

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            status=row['status'],
            region=row['region'],
            available_slots=row['available_slots'],
            occupied_slots=row['occupied_slots'],
            addr=row['addr'],
            first_contact=row['first_contact'],
            lost_at=row['lost_at'],
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
    async def batch_load(dbpool, agent_ids, status=None):
        async with dbpool.acquire() as conn:
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
                o = Agent.from_row(row)
                objs_per_key[row.id] = o
        return tuple(objs_per_key.values())
