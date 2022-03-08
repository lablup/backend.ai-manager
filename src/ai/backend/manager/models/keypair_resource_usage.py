from __future__ import annotations

from typing import (
    Sequence,
    TYPE_CHECKING,
)

import graphene
import sqlalchemy as sa
from sqlalchemy.engine.row import Row

from ai.backend.common import redis

if TYPE_CHECKING:
    from .gql import GraphQueryContext
    from .vfolder import VirtualFolder

from .base import (
    Item,
    metadata,
)
from .minilang.queryfilter import QueryFilterParser

__all__: Sequence[str] = (
    'keypair_resource_usages',
    'KeyPairResourceUsage',
)


keypair_resource_usages = sa.Table(
    'keypair_resource_usages', metadata,
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key', onupdate='CASCADE', ondelete='CASCADE'),
              primary_key=True, nullable=False, index=True),
    sa.Column('concurrency_used', sa.Integer),
)


class KeyPairResourceUsage(graphene.ObjectType):

    class Meta:
        interfaces = (Item, )

    access_key = graphene.String()
    concurrency_used = graphene.Int()

    @classmethod
    def from_row(
        cls,
        ctx: GraphQueryContext,
        row: Row,
    ) -> KeyPairResourceUsage:
        return cls(
            access_key=row['access_key'],
            concurrency_used=row['concurrency_used'],
        )

    async def resolve_num_queries(self, info: graphene.ResolveInfo) -> int:
        ctx: GraphQueryContext = info.context
        n = await redis.execute(ctx.redis_stat, lambda r: r.get(f"kp:{self.access_key}:num_queries"))
        if n is not None:
            return n
        return 0

    async def resolve_vfolders(self, info: graphene.ResolveInfo) -> Sequence[VirtualFolder]:
        ctx: GraphQueryContext = info.context
        loader = ctx.dataloader_manager.get_loader(ctx, 'VirtualFolder')
        return await loader.load(self.access_key)

    async def resolve_compute_sessions(self, info: graphene.ResolveInfo, raw_status: str = None):
        ctx: GraphQueryContext = info.context
        from . import KernelStatus  # noqa: avoid circular imports
        if raw_status is not None:
            status = KernelStatus[raw_status]
        loader = ctx.dataloader_manager.get_loader(ctx, 'ComputeSession', status=status)
        return await loader.load(self.access_key)

    @classmethod
    async def load_all(
        cls,
        graph_ctx: GraphQueryContext,
        *,
        limit: int = None,
    ) -> Sequence[KeyPairResourceUsage]:
        query = sa.select([keypair_resource_usages])
        if limit is not None:
            query = query.limit(limit)
        async with graph_ctx.db.begin_readonly() as conn:
            return [
                obj async for row in (await conn.stream(query))
                if (obj := cls.from_row(graph_ctx, row)) is not None
            ]

    _queryfilter_fieldspec = {
        "access_key": ("keypairs_access_key", None),
        "concurrency_used": ("keypair_resource_usages_used", None),
    }

    @classmethod
    async def load_count(
        cls,
        graph_ctx: GraphQueryContext,
        *,
        filter: str = None,
    ) -> int:
        query = sa.select([sa.func.count(keypair_resource_usages.c.access_key)])
        if filter is not None:
            qfparser = QueryFilterParser(cls._queryfilter_fieldspec)
            query = qfparser.append_filter(query, filter)
        async with graph_ctx.db.begin_readonly() as conn:
            result = await conn.execute(query)
            return result.scalar()
