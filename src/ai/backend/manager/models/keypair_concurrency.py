from __future__ import annotations

import base64
import secrets
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
    List, TYPE_CHECKING,
    Tuple,
    TypedDict,
)
import uuid

from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
from dateutil.parser import parse as dtparse
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection
from sqlalchemy.engine.row import Row
from sqlalchemy.sql.expression import false

from ai.backend.common import msgpack, redis
from ai.backend.common.types import (
    AccessKey,
    SecretKey,
)

if TYPE_CHECKING:
    from .gql import GraphQueryContext
    from .vfolder import VirtualFolder

from .base import (
    ForeignKeyIDColumn,
    Item,
    PaginatedList,
    metadata,
    batch_result,
    batch_multiresult,
    set_if_set,
    simple_db_mutate,
    simple_db_mutate_returning_item,
)
from .minilang.queryfilter import QueryFilterParser
from .minilang.ordering import QueryOrderParser
from .user import ModifyUserInput, UserRole
from ..defs import RESERVED_DOTFILES

__all__: Sequence[str] = (
    'keypairs_concurrency',
    'KeyPair', 'KeyPairList',
    'UserInfo',
    'KeyPairInput',
    'CreateKeyPair', 'ModifyKeyPair', 'DeleteKeyPair',
    'query_owned_dotfiles',
    'query_bootstrap_script',
)


keypairs_concurrency = sa.Table(
    'keypairs_concurrency', metadata,
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypair.access_key', onupdate='CASCADE', ondelete='CASCADE'),
              primary_key=True, nullable=False, index=True),
    sa.Column('concurrency_used', sa.Integer),
)


class KeyPair(graphene.ObjectType):

    class Meta:
        interfaces = (Item, )

    access_key = graphene.String()
    concurrency_used = graphene.Int()

    @classmethod
    def from_row(
        cls,
        ctx: GraphQueryContext,
        row: Row,
    ) -> KeyPair:
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
    ) -> Sequence[KeyPair]:
        query = (
            sa.select([keypairs_concurrency])
            .select_from(keypairs_concurrency)
        )
        if limit is not None:
            query = query.limit(limit)
        async with graph_ctx.db.begin_readonly() as conn:
            return [
                obj async for row in (await conn.stream(query))
                if (obj := cls.from_row(graph_ctx, row)) is not None
            ]

    _queryfilter_fieldspec = {
        "access_key": ("keypairs_access_key", None),
        "concurrency_used": ("keypairs_concurrency_used", None),
    }

    @classmethod
    async def load_count(
        cls,
        graph_ctx: GraphQueryContext,
        *,
        filter: str = None,
    ) -> int:
        from .user import users
        query = (
            sa.select([sa.func.count(keypairs_concurrency.c.access_key)])
            .select_from(keypairs_concurrency)
        )
        if filter is not None:
            qfparser = QueryFilterParser(cls._queryfilter_fieldspec)
            query = qfparser.append_filter(query, filter)
        async with graph_ctx.db.begin_readonly() as conn:
            result = await conn.execute(query)
            return result.scalar()
