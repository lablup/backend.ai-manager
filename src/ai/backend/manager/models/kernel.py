from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.types import BinarySize
from ai.backend.common import msgpack
from .base import (
    metadata,
    BigInt, GUID, IDColumn, EnumType,
    ResourceSlotColumn,
    Item, PaginatedList,
)
from .group import groups
from .user import UserRole

__all__ = (
    'kernels', 'KernelStatus',
    'ComputeSessionList', 'ComputeSession', 'ComputeWorker', 'Computation',
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

    # Resource metrics measured upon termination
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('last_stat', pgsql.JSONB(), nullable=True, default=sa.null()),

    sa.Index('ix_kernels_sess_id_role', 'sess_id', 'role', unique=False),
    sa.Index('ix_kernels_unique_sess_token', 'access_key', 'sess_id',
             unique=True,
             postgresql_where=sa.text(
                 "status != 'TERMINATED' and "
                 "role = 'master'")),
)


class SessionCommons:
    sess_id = graphene.String()
    id = graphene.ID()
    role = graphene.String()
    image = graphene.String()
    registry = graphene.String()
    domain_name = graphene.String()
    group_id = graphene.UUID()
    user_uuid = graphene.UUID()
    access_key = graphene.String()

    status = graphene.String()
    status_info = graphene.String()
    created_at = GQLDateTime()
    terminated_at = GQLDateTime()

    agent = graphene.String()
    container_id = graphene.String()
    service_ports = graphene.JSONString()

    occupied_slots = graphene.JSONString()
    occupied_shares = graphene.JSONString()

    num_queries = BigInt()
    live_stat = graphene.JSONString()
    last_stat = graphene.JSONString()
    group_name = graphene.String()

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
    async def _resolve_live_stat(cls, redis_stat, kernel_id):
        cstat = await redis_stat.get(kernel_id, encoding=None)
        if cstat is not None:
            cstat = msgpack.unpackb(cstat)
        return cstat

    async def resolve_live_stat(self, info):
        rs = info.context['redis_stat']
        return await type(self)._resolve_live_stat(rs, str(self.id))

    async def _resolve_legacy_metric(self, info, metric_key, metric_field, convert_type):
        if not hasattr(self, 'status'):
            return None
        rs = info.context['redis_stat']
        if self.status not in LIVE_STATUS:
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
            kstat = await type(self)._resolve_live_stat(rs, str(self.id))
            if kstat is None:
                return convert_type(0)
            metric = kstat.get(metric_key)
            if metric is None:
                return convert_type(0)
            value = metric.get(metric_field)
            if value is None:
                return convert_type(0)
            return convert_type(value)

    async def resolve_cpu_used(self, info):
        return await self._resolve_legacy_metric(info, 'cpu_used', 'current', float)

    async def resolve_cpu_using(self, info):
        return await self._resolve_legacy_metric(info, 'cpu_util', 'pct', float)

    async def resolve_mem_max_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'mem', 'stats.max', int)

    async def resolve_mem_cur_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'mem', 'current', int)

    async def resolve_net_rx_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'net_rx', 'stats.rate', int)

    async def resolve_net_tx_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'net_tx', 'stats.rate', int)

    async def resolve_io_read_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'io_read', 'current', int)

    async def resolve_io_write_bytes(self, info):
        return await self._resolve_legacy_metric(info, 'io_write', 'current', int)

    async def resolve_io_max_scratch_size(self, info):
        return await self._resolve_legacy_metric(info, 'io_scratch_size', 'stats.max', int)

    async def resolve_io_cur_scratch_size(self, info):
        return await self._resolve_legacy_metric(info, 'io_scratch_size', 'current', int)

    @classmethod
    def parse_row(cls, context, row):
        assert row is not None
        mega = 2 ** 20
        return {
            'sess_id': row['sess_id'],
            'id': row['id'],
            'role': row['role'],
            'image': row['image'],
            'registry': row['registry'],
            'domain_name': row['domain_name'],
            'group_id': row['group_id'],
            'user_uuid': row['user_uuid'],
            'access_key': row['access_key'],
            'status': row['status'].name,
            'status_info': row['status_info'],
            'created_at': row['created_at'],
            'terminated_at': row['terminated_at'],
            'agent': row['agent'],
            'container_id': row['container_id'],
            'service_ports': row['service_ports'],
            'occupied_slots': row['occupied_slots'].to_json(),
            'occupied_shares': row['occupied_shares'],
            'num_queries': row['num_queries'],
            # live_stat is resolved by Graphene
            'last_stat': row['last_stat'],
            'group_name': row['name'],  # group.name (group is omitted since use_labels=True is not used)
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
            'mem_slot': BinarySize.from_str(
                row['occupied_slots'].get('mem', 0)) // mega,
            'cpu_slot': float(row['occupied_slots'].get('cpu', 0)),
            'gpu_slot': float(row['occupied_slots'].get('cuda.device', 0)),
            'tpu_slot': float(row['occupied_slots'].get('tpu.device', 0)),
        }

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        props = cls.parse_row(context, row)
        return cls(**props)


class ComputeSession(SessionCommons, graphene.ObjectType):
    '''
    Represents a master session.
    '''
    class Meta:
        interfaces = (Item, )

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
    async def load_count(context, access_key=None, status=None, group_id=None):
        async with context['dbpool'].acquire() as conn:
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            query = (sa.select([sa.func.count(kernels.c.sess_id)])
                       .select_from(kernels)
                       .where(kernels.c.role == 'master')
                       .as_scalar())
            if status is not None:
                status = KernelStatus[status]
                query = query.where(kernels.c.status == status)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            result = await conn.execute(query)
            count = await result.fetchone()
            return count[0]

    @staticmethod
    async def load_slice(context, limit, offset, access_key=None, status=None, group_id=None):
        async with context['dbpool'].acquire() as conn:
            # TODO: optimization for pagination using subquery, join
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            order_col = (kernels.c.terminated_at
                         if status is not None and status == KernelStatus.TERMINATED
                         else kernels.c.created_at)
            j = kernels.join(groups, groups.c.id == kernels.c.group_id)
            query = (sa.select([kernels, groups.c.name])
                       .select_from(j)
                       .where(kernels.c.role == 'master')
                       .order_by(sa.desc(order_col))
                       .limit(limit)
                       .offset(offset))
            if status is not None:
                status = KernelStatus[status]
                query = query.where(kernels.c.status == status)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [ComputeSession.from_row(context, r) for r in rows]

    @staticmethod
    async def load_all(context, status=None, group_id=None):
        async with context['dbpool'].acquire() as conn:
            # status = status if status else KernelStatus['RUNNING']
            if isinstance(status, str):
                status = KernelStatus[status]  # for legacy
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            order_col = (kernels.c.terminated_at
                         if status is not None and status == KernelStatus.TERMINATED
                         else kernels.c.created_at)
            j = kernels.join(groups, groups.c.id == kernels.c.group_id)
            query = (sa.select([kernels, groups.c.name])
                       .select_from(j)
                       .where(kernels.c.role == 'master')
                       .order_by(sa.desc(order_col))
                       .limit(100))
            if status is not None:
                query = query.where(kernels.c.status == status)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [ComputeSession.from_row(context, r) for r in rows]

    @staticmethod
    async def batch_load(context, access_keys, *, status=None, group_id=None):
        async with context['dbpool'].acquire() as conn:
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            order_col = (kernels.c.terminated_at
                         if status is not None and status == KernelStatus.TERMINATED
                         else kernels.c.created_at)
            j = kernels.join(groups, groups.c.id == kernels.c.group_id)
            query = (sa.select([kernels, groups.c.name])
                       .select_from(j)
                       .where((kernels.c.access_key.in_(access_keys)) &
                              (kernels.c.role == 'master'))
                       .order_by(sa.desc(order_col))
                       .limit(100))
            if status is not None:
                query = query.where(kernels.c.status == status)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(kernels.c.group_id == group_id)
            objs_per_key = OrderedDict()
            for k in access_keys:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = ComputeSession.from_row(context, row)
                objs_per_key[row.access_key].append(o)
        return tuple(objs_per_key.values())

    @staticmethod
    async def batch_load_detail(context, sess_ids, *, access_key=None, status=None):
        async with context['dbpool'].acquire() as conn:
            # TODO: Extend to return terminated sessions (we need unique identifier).
            status = KernelStatus[status] if status else KernelStatus['RUNNING']
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            j = kernels.join(groups, groups.c.id == kernels.c.group_id)
            query = (sa.select([kernels, groups.c.name])
                       .select_from(j)
                       .where((kernels.c.role == 'master') &
                              (kernels.c.sess_id.in_(sess_ids))))
            if status is not None:
                query = query.where(kernels.c.status == status)
            if access_key is not None:
                query = query.where(kernels.c.access_key == access_key)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            sess_info = []
            async for row in conn.execute(query):
                o = ComputeSession.from_row(context, row)
                sess_info.append(o)
        if len(sess_info) != 0:
            return tuple(sess_info)
        else:
            sess_info = OrderedDict()
            for s in sess_ids:
                sess_info[s] = list()
            async for row in conn.execute(query):
                o = ComputeSession.from_row(context, row)
                sess_info[row.sess_id].append(o)
            return tuple(sess_info.values())

    @classmethod
    def parse_row(cls, context, row):
        common_props = super().parse_row(context, row)
        return {**common_props, 'tag': row['tag']}


class ComputeSessionList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(ComputeSession, required=True)


class ComputeWorker(SessionCommons, graphene.ObjectType):
    '''
    Represents a worker session that belongs to a master session.
    '''

    @staticmethod
    async def batch_load(context, sess_ids, *, status=None, access_key=None):
        async with context['dbpool'].acquire() as conn:
            domain_name = context['user']['domain_name']
            query = (sa.select([kernels])
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
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            for k in sess_ids:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = ComputeWorker.from_row(context, row)
                objs_per_key[row.sess_id].append(o)
        return tuple(objs_per_key.values())


class Computation(SessionCommons, graphene.ObjectType):
    '''
    Any kind of computation: either a session master or a worker.
    '''

    @staticmethod
    async def batch_load_by_agent_id(context, agent_ids, *, status=None):
        async with context['dbpool'].acquire() as conn:
            domain_name = None if context['user']['role'] == UserRole.SUPERADMIN \
                    else context['user']['domain_name']
            query = (sa.select([kernels])
                       .select_from(kernels)
                       .where(kernels.c.agent.in_(agent_ids))
                       .order_by(sa.desc(kernels.c.created_at)))
            if status is not None:
                status = KernelStatus[status]
                query = query.where(kernels.c.status == status)
            if domain_name is not None:
                query = query.where(kernels.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            for k in agent_ids:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = Computation.from_row(context, row)
                objs_per_key[row.agent].append(o)
        return tuple(objs_per_key.values())
