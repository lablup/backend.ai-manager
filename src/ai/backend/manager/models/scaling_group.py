from collections import OrderedDict
from typing import Union
import uuid

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
import graphene
from graphene.types.datetime import DateTime as GQLDateTime

from .base import metadata

__all__ = (
    # table defs
    'scaling_groups',
    'sgroups_for_domains',
    'sgroups_for_groups',
    'sgroups_for_keypairs',
    # functions
    'query_allowed_sgroups',
    'ScalingGroup',
)


scaling_groups = sa.Table(
    'scaling_groups', metadata,
    sa.Column('name', sa.String(length=64), primary_key=True),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, index=True, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('driver', sa.String(length=64), nullable=False),
    sa.Column('driver_opts', pgsql.JSONB(), nullable=False, default={}),
    sa.Column('scheduler', sa.String(length=64), nullable=False),
    sa.Column('scheduler_opts', pgsql.JSONB(), nullable=False, default={}),
)


# When scheduling, we take the union of allowed scaling groups for
# each domain, group, and keypair.


sgroups_for_domains = sa.Table(
    'sgroups_for_domains', metadata,
    sa.Column('scaling_group',
              sa.ForeignKey('scaling_groups.name',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.Column('domain',
              sa.ForeignKey('domains.name',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.UniqueConstraint('scaling_group', 'domain', name='uq_sgroup_domain'),
)


sgroups_for_groups = sa.Table(
    'sgroups_for_groups', metadata,
    sa.Column('scaling_group',
              sa.ForeignKey('scaling_groups.name',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.Column('group',
              sa.ForeignKey('groups.id',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.UniqueConstraint('scaling_group', 'group', name='uq_sgroup_ugroup'),
)


sgroups_for_keypairs = sa.Table(
    'sgroups_for_keypairs', metadata,
    sa.Column('scaling_group',
              sa.ForeignKey('scaling_groups.name',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.Column('access_key',
              sa.ForeignKey('keypairs.access_key',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              index=True, nullable=False),
    sa.UniqueConstraint('scaling_group', 'access_key', name='uq_sgroup_akey'),
)


async def query_allowed_sgroups(db_conn: object,
                                domain: str,
                                group: Union[uuid.UUID, str],
                                access_key: str):
    query = (sa.select([sgroups_for_domains])
               .where(sgroups_for_domains.c.domain == domain))
    result = await db_conn.execute(query)
    from_domain = {row['scaling_group'] async for row in result}

    if isinstance(group, uuid.UUID):
        query = (sa.select([sgroups_for_groups])
                   .where(sgroups_for_groups.c.group == group))
    elif isinstance(group, str):
        j = sa.join(sgroups_for_groups, scaling_groups,
                    sgroups_for_groups.c.group == scaling_groups.c.id)
        query = (sa.select([sgroups_for_groups])
                   .select_from(j)
                   .where(scaling_groups.c.name == group))
    else:
        raise ValueError('unexpected type for group', group)
    result = await db_conn.execute(query)
    from_group = {row['scaling_group'] async for row in result}

    query = (sa.select([sgroups_for_keypairs])
               .where(sgroups_for_keypairs.c.access_key == access_key))
    result = await db_conn.execute(query)
    from_keypair = {row['scaling_group'] async for row in result}

    sgroups = from_domain | from_group | from_keypair
    query = (sa.select([scaling_groups])
               .where(
                   (scaling_groups.c.name.in_(sgroups)) &
                   (scaling_groups.c.is_active == True)   # noqa: E712
               ))
    result = await db_conn.execute(query)
    return [row async for row in result]


class ScalingGroup(graphene.ObjectType):
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    driver = graphene.String()
    driver_opts = graphene.JSONString()
    scheduler = graphene.String()
    scheduler_opts = graphene.JSONString()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            driver=row['driver'],
            driver_opts=row['driver_opts'],
            scheduler=row['scheduler'],
            scheduler_opts=row['scheduler_opts'],
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([scaling_groups]).select_from(scaling_groups)
            if is_active is not None:
                query = query.where(scaling_groups.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = ScalingGroup.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_name(context, names):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([scaling_groups])
                       .select_from(scaling_groups)
                       .where(scaling_groups.c.name.in_(names)))
            objs_per_key = OrderedDict()
            # For each access key, there is only one keypair.
            # So we don't build lists in objs_per_key variable.
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = ScalingGroup.from_row(row)
                objs_per_key[row.access_key] = o
        return tuple(objs_per_key.values())
