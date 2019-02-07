from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from .base import metadata, EnumType

__all__ = (
    'user_resource_policies',
    'UserResourcePolicy',
)


class DefaultForUnspecified(enum.Enum):
    LIMITED = 0
    UNLIMITED = 1


user_resource_policies = sa.Table(
    'user_resource_policies', metadata,
    sa.Column('name', sa.String(length=256), primary_key=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('default_for_unspecified',
              EnumType(DefaultForUnspecified),
              default=DefaultForUnspecified.LIMITED,
              nullable=False),
    sa.Column('total_resource_slots', pgsql.JSONB(), nullable=False),
    sa.Column('max_concurrent_sessions', sa.Integer(), nullable=False),
    sa.Column('max_vfolder_count', sa.Integer(), nullable=False),
    sa.Column('max_vfolder_size', sa.BigInteger(), nullable=False),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False),
    # TODO: implement with a many-to-many association table
    # sa.Column('allowed_scaling_groups', sa.Array(sa.String), nullable=False),
)


class UserResourcePolicy(graphene.ObjectType):
    name = graphene.String()
    created_at = GQLDateTime()
    default_for_unspecified = graphene.String()
    total_resource_slots = graphene.JSONString()
    max_concurrent_sessions = graphene.Int()
    max_vfolder_count = graphene.Int()
    max_vfolder_size = graphene.Int()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            created_at=row['created_at'],
            default_for_unspecified=row['default_for_unspecified'],
            total_resource_slots=row['total_resource_slots'],
            max_concurrent_sessions=row['max_concurrent_sessions'],
            max_vfolder_count=row['max_vfolder_count'],
            max_vfolder_size=row['max_vfolder_size'],
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
        )

    @staticmethod
    async def load_all(context):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(user_resource_policies))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [UserResourcePolicy.from_row(context, r) for r in rows]

    @staticmethod
    async def batch_load_by_name(context, names):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(user_resource_policies)
                       .where(user_resource_policies.c.name.in_(names))
                       .order_by(user_resource_policies.c.id))
            objs_per_key = OrderedDict()
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = UserResourcePolicy.from_row(context, row)
                objs_per_key[row.id] = o
        return tuple(objs_per_key.values())
