import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from typing import Union
import uuid

from .base import metadata

__all__ = (
    'scaling_groups',
    'sgroups_for_domains',
    'sgroups_for_groups',
    'sgroups_for_keypairs',
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
               .where(sgroups_for_domains.c.name == domain))
    result = await db_conn.execute(query)
    from_domain = {row['scaling_group'] async for row in result}

    if isinstance(group, uuid.UUID):
        query = (sa.select([sgroups_for_groups])
                   .where(sgroups_for_groups.c.id == group))
    elif isinstance(group, str):
        query = (sa.select([sgroups_for_groups])
                   .where(sgroups_for_groups.c.name == group))
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
