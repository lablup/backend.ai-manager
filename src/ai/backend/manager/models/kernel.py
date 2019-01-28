from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

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
    sa.Column('image', sa.String(length=512)),
    sa.Column('registry', sa.String(length=512)),
    sa.Column('tag', sa.String(length=64), nullable=True),

    # Resource occupation
    sa.Column('container_id', sa.String(length=64)),
    sa.Column('occupied_slots', pgsql.JSONB(), nullable=False),
    sa.Column('occupied_shares', pgsql.JSONB(), nullable=False),
    sa.Column('environ', sa.ARRAY(sa.String), nullable=True),

    # Port mappings
    # If kernel_host is NULL, it is assumed to be same to the agent host or IP.
    sa.Column('kernel_host', sa.String(length=128), nullable=True),
    sa.Column('repl_in_port', sa.Integer(), nullable=False),
    sa.Column('repl_out_port', sa.Integer(), nullable=False),
    sa.Column('stdin_port', sa.Integer(), nullable=False),   # legacy for stream_pty
    sa.Column('stdout_port', sa.Integer(), nullable=False),  # legacy for stream_pty
    sa.Column('service_ports', pgsql.JSONB(), nullable=True),

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
    sa.Index('ix_kernels_unique_sess_token', 'access_key', 'sess_id',
             unique=True,
             postgresql_where=sa.text(
                 "status != 'TERMINATED' and "
                 "role = 'master'")),
)


class SessionCommons:
    sess_id = graphene.String()
    id = graphene.UUID()
    role = graphene.String()
    image = graphene.String()
    registry = graphene.String()

    status = graphene.String()
    status_info = graphene.String()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()

    agent = graphene.String()
    container_id = graphene.String()
    service_ports = graphene.JSONString()

    occupied_slots = graphene.JSONString()
    occupied_shares = graphene.JSONString()
    # TODO: add dynamic stats for intrinsic/accelerator metrics

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
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.cpu_used)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'cpu_used')
        return float(ret) if ret is not None else 0

    async def resolve_mem_max_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.mem_max_bytes)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'mem_max_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_mem_cur_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return 0
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'mem_cur_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_net_rx_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.net_rx_bytes)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'net_rx_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_net_tx_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.net_tx_bytes)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'net_tx_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_io_read_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_read_bytes)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'io_read_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_io_write_bytes(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_write_bytes)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'io_write_bytes')
        return int(ret) if ret is not None else 0

    async def resolve_io_max_scratch_size(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return zero_if_none(self.io_max_scratch_size)
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'io_max_scratch_size')
        return int(ret) if ret is not None else 0

    async def resolve_io_cur_scratch_size(self, info):
        if not hasattr(self, 'status'):
            return None
        if self.status not in LIVE_STATUS:
            return 0
        rs = info.context['redis_stat']
        ret = await rs.hget(str(self.id), 'io_cur_scratch_size')
        return int(ret) if ret is not None else 0

    @classmethod
    def parse_row(cls, row):
        assert row is not None
        return {
            'sess_id': row['sess_id'],
            'id': row['id'],
            'role': row['role'],
            'image': row['image'],
            'registry': row['registry'],
            'status': row['status'],
            'status_info': row['status_info'],
            'created_at': row['created_at'],
            'terminated_at': row['terminated_at'],
            'agent': row['agent'],
            'container_id': row['container_id'],
            'service_ports': row['service_ports'],
            'occupied_slots': row['occupied_slots'],
            'occupied_shares': row['occupied_shares'],
            'num_queries': row['num_queries'],
            # live statistics
            # NOTE: currently graphene always uses resolve methods!
            'cpu_used': row['cpu_used'],
            'mem_max_bytes': row['mem_max_bytes'],
            'mem_cur_bytes': 0,
            'net_rx_bytes': row['net_rx_bytes'],
            'net_tx_bytes': row['net_tx_bytes'],
            'io_read_bytes': row['io_read_bytes'],
            'io_write_bytes': row['io_write_bytes'],
            'io_max_scratch_size': row['io_max_scratch_size'],
            'io_cur_scratch_size': 0,
        }

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        props = cls.parse_row(row)
        return cls(**props)


class ComputeSession(SessionCommons, graphene.ObjectType):
    '''
    Represents a master session.
    '''

    tag = graphene.String()  # Only for ComputeSession

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
                       .order_by(sa.desc(kernels.c.created_at))
                       .limit(100))
            if status is not None:
                query = query.where(kernels.c.status == status)
            objs_per_key = OrderedDict()
            for k in access_keys:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = ComputeSession.from_row(row)
                objs_per_key[row.access_key].append(o)
        return tuple(objs_per_key.values())

    @staticmethod
    async def batch_load_detail(dbpool, sess_ids, *, access_key=None, status=None):
        async with dbpool.acquire() as conn:
            # TODO: Extend to return terminated sessions (we need unique identifier).
            status = KernelStatus[status] if status else KernelStatus['RUNNING']
            query = (sa.select('*')
                       .select_from(kernels)
                       .where((kernels.c.role == 'master') &
                              (kernels.c.status == status) &
                              (kernels.c.sess_id.in_(sess_ids))))
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            sess_info = []
            async for row in conn.execute(query):
                o = ComputeSession.from_row(row)
                sess_info.append(o)
        if len(sess_info) != 0:
            return tuple(sess_info)
        else:
            sess_info = OrderedDict()
            for s in sess_ids:
                sess_info[s] = list()
            async for row in conn.execute(query):
                o = ComputeSession.from_row(row)
                sess_info[row.sess_id].append(o)
            return tuple(sess_info.values())

    @classmethod
    def parse_row(cls, row):
        common_props = super().parse_row(row)
        return {**common_props, 'tag': row['tag']}


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
