from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa

from .base import metadata, zero_if_none, EnumType, IDColumn

__all__ = (
    'kernels', 'KernelStatus',
    'ComputeSession', 'ComputeWorker', 'Computation',
)


class KernelStatus(enum.Enum):
    # values are only meaningful inside the gateway
    PREPARING = 10
    # ---
    BUILDING = 20
    # ---
    RUNNING = 30
    RESTARTING = 31
    RESIZING = 32
    SUSPENDED = 33
    # ---
    TERMINATING = 40
    TERMINATED = 41
    ERROR = 42


LIVE_STATUS = frozenset(['BUILDING', 'RUNNING'])


kernels = sa.Table(
    'kernels', metadata,
    IDColumn(),
    sa.Column('sess_id', sa.String(length=64), unique=False, index=True),
    sa.Column('role', sa.String(length=16), nullable=False, default='master'),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id')),
    sa.Column('agent_addr', sa.String(length=128), nullable=False),
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key')),
    sa.Column('lang', sa.String(length=64)),

    # Resource occupation
    sa.Column('container_id', sa.String(length=64)),
    sa.Column('cpu_set', sa.ARRAY(sa.Integer)),
    sa.Column('gpu_set', sa.ARRAY(sa.Integer)),
    sa.Column('mem_slot', sa.BigInteger(), nullable=False),
    sa.Column('cpu_slot', sa.Float(), nullable=False),
    sa.Column('gpu_slot', sa.Float(), nullable=False),

    # Port mappings
    sa.Column('repl_in_port', sa.Integer(), nullable=False),
    sa.Column('repl_out_port', sa.Integer(), nullable=False),
    sa.Column('stdin_port', sa.Integer(), nullable=False),
    sa.Column('stdout_port', sa.Integer(), nullable=False),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), index=True),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null(), index=True),
    sa.Column('status', EnumType(KernelStatus),
              default=KernelStatus.PREPARING, index=True),
    sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null()),

    # Live stats
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('cpu_used', sa.BigInteger(), default=0),       # msec
    sa.Column('mem_max_bytes', sa.BigInteger(), default=0),
    sa.Column('net_rx_bytes', sa.BigInteger(), default=0),
    sa.Column('net_tx_bytes', sa.BigInteger(), default=0),
    sa.Column('io_read_bytes', sa.BigInteger(), default=0),
    sa.Column('io_write_bytes', sa.BigInteger(), default=0),
    sa.Column('io_max_scratch_size', sa.BigInteger(), default=0),

    sa.Index('ix_kernels_sess_id_role', 'sess_id', 'role', unique=False),
)


class SessionCommons:

    sess_id = graphene.String()
    id = graphene.UUID()
    role = graphene.String()
    lang = graphene.String()

    status = graphene.String()
    status_info = graphene.String()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()

    agent = graphene.String()
    container_id = graphene.String()

    mem_slot = graphene.Int()
    cpu_slot = graphene.Float()
    gpu_slot = graphene.Float()

    num_queries = graphene.Int()
    cpu_used = graphene.Int()
    mem_max_bytes = graphene.Int()
    mem_cur_bytes = graphene.Int()
    net_rx_bytes = graphene.Int()
    net_tx_bytes = graphene.Int()
    io_read_bytes = graphene.Int()
    io_write_bytes = graphene.Int()
    io_max_scratch_size = graphene.Int()
    io_cur_scratch_size = graphene.Int()

    async def resolve_cpu_used(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.cpu_used)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'cpu_used')
            return float(ret) if ret is not None else 0

    async def resolve_mem_max_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.mem_max_bytes)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'mem_max_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_mem_cur_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return 0
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'mem_cur_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_net_rx_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.net_rx_bytes)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'net_rx_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_net_tx_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.net_tx_bytes)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'net_tx_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_io_read_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_read_bytes)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'io_read_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_io_write_bytes(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_write_bytes)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'io_write_bytes')
            return int(ret) if ret is not None else 0

    async def resolve_io_max_scratch_size(self, info):
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_max_scratch_size)
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'io_max_scratch_size')
            return int(ret) if ret is not None else 0

    async def resolve_io_cur_scratch_size(self, info):
        if self.status not in LIVE_STATUS:
            return 0
        async with info.context['redis_stat_pool'].get() as rs:
            ret = await rs.hget(str(self.id), 'io_cur_scratch_size')
            return int(ret) if ret is not None else 0

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        props = {
            'sess_id': row.sess_id,
            'id': row.id,
            'role': row.role,
            'lang': row.lang,
            'status': row.status,
            'status_info': row.status_info,
            'created_at': row.created_at,
            'terminated_at': row.terminated_at,
            'agent': row.agent,
            'container_id': row.container_id,
            'mem_slot': row.mem_slot,
            'cpu_slot': row.cpu_slot,
            'gpu_slot': row.gpu_slot,
            'num_queries': row.num_queries,
            # live statistics
            # NOTE: currently graphene always uses resolve methods!
            'cpu_used': row.cpu_used,
            'mem_max_bytes': row.mem_max_bytes,
            'mem_cur_bytes': 0,
            'net_rx_bytes': row.net_rx_bytes,
            'net_tx_bytes': row.net_tx_bytes,
            'io_read_bytes': row.io_read_bytes,
            'io_write_bytes': row.io_write_bytes,
            'io_max_scratch_size': row.io_max_scratch_size,
            'io_cur_scratch_size': 0,
        }
        return cls(**props)


class ComputeSession(SessionCommons, graphene.ObjectType):
    '''
    Represents a master session.
    '''

    workers = graphene.List(
        lambda: ComputeWorker,
        status=graphene.String(),
    )

    async def resolve_workers(self, info, status=None):
        '''
        Retrieves all children worker sessions.
        '''
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeWorker', status=status)
        return await loader.load(self.sess_id)

    @staticmethod
    async def batch_load(dbpool, access_keys, *, status=None):
        async with dbpool.acquire() as conn:
            query = (sa.select('*')
                       .select_from(kernels)
                       .where((kernels.c.access_key.in_(access_keys)) &
                              (kernels.c.role == 'master'))
                       .order_by(sa.desc(kernels.c.created_at)))
            if status is not None:
                query = query.where(kernels.c.status == status)
            objs_per_key = OrderedDict()
            for k in access_keys:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = ComputeSession.from_row(row)
                objs_per_key[row.access_key].append(o)
        return tuple(objs_per_key.values())


class ComputeWorker(SessionCommons, graphene.ObjectType):
    '''
    Represents a worker session that belongs to a master session.
    '''

    @staticmethod
    async def batch_load(dbpool, sess_ids, *, status=None, access_key=None):
        async with dbpool.acquire() as conn:
            query = (sa.select('*')
                       .select_from(kernels)
                       .where((kernels.c.sess_id.in_(sess_ids)) &
                              (kernels.c.role == 'worker'))
                       .order_by(sa.desc(kernels.c.created_at)))
            if status is not None:
                query = query.where(kernels.c.status == status)
            if access_key is not None:
                # For user queries, ensure only the user's own workers
                # even when he/she knows other users' session IDs.
                query = query.where(kernels.c.access_key == access_key)
            objs_per_key = OrderedDict()
            for k in sess_ids:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = ComputeWorker.from_row(row)
                objs_per_key[row.sess_id].append(o)
        return tuple(objs_per_key.values())


class Computation(SessionCommons, graphene.ObjectType):
    '''
    Any kind of computation: either a session master or a worker.
    '''

    @staticmethod
    async def batch_load_by_agent_id(dbpool, agent_ids, *, status=None):
        async with dbpool.acquire() as conn:
            query = (sa.select('*')
                       .select_from(kernels)
                       .where(kernels.c.agent.in_(agent_ids))
                       .order_by(sa.desc(kernels.c.created_at)))
            if status is not None:
                status = KernelStatus[status]
                query = query.where(kernels.c.status == status)
            objs_per_key = OrderedDict()
            for k in agent_ids:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = Computation.from_row(row)
                objs_per_key[row.agent].append(o)
        return tuple(objs_per_key.values())
