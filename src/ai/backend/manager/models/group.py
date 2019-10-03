import asyncio
from collections import OrderedDict
import re
from typing import (
    Optional, Union,
    Sequence,
)
import uuid

from aiopg.sa.connection import SAConnection
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.types import ResourceSlot
from .base import (
    metadata, GUID, IDColumn, ResourceSlotColumn,
    privileged_mutation,
    set_if_set,
)
from .user import UserRole


__all__: Sequence[str] = (
    'groups', 'association_groups_users',
    'resolve_group_name_or_id',
    'Group', 'GroupInput', 'ModifyGroupInput',
    'CreateGroup', 'ModifyGroup', 'DeleteGroup',
)

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')

association_groups_users = sa.Table(
    'association_groups_users', metadata,
    sa.Column('user_id', GUID,
              sa.ForeignKey('users.uuid', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.Column('group_id', GUID,
              sa.ForeignKey('groups.id', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.UniqueConstraint('user_id', 'group_id', name='uq_association_user_id_group_id')
)


groups = sa.Table(
    'groups', metadata,
    IDColumn('id'),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),
    #: Field for synchronization with external services.
    sa.Column('integration_id', sa.String(length=512)),

    sa.Column('domain_name', sa.String(length=64),
              sa.ForeignKey('domains.name', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False, index=True),
    # TODO: separate resource-related fields with new domain resource policy table when needed.
    sa.Column('total_resource_slots', ResourceSlotColumn(), default='{}'),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False, default='{}'),
    sa.UniqueConstraint('name', 'domain_name', name='uq_groups_name_domain_name')
)


async def resolve_group_name_or_id(db_conn: SAConnection,
                                   domain_name: str,
                                   value: Union[str, uuid.UUID]) -> Optional[uuid.UUID]:
    if isinstance(value, str):
        query = (
            sa.select([groups.c.id])
            .select_from(groups)
            .where(
                (groups.c.name == value) &
                (groups.c.domain_name == domain_name)
            )
        )
        return await db_conn.scalar(query)
    elif isinstance(value, uuid.UUID):
        query = (
            sa.select([groups.c.id])
            .select_from(groups)
            .where(
                (groups.c.id == value) &
                (groups.c.domain_name == domain_name)
            )
        )
        return await db_conn.scalar(query)
    else:
        raise TypeError('unexpected type for group_name_or_id')


class Group(graphene.ObjectType):
    id = graphene.UUID()
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    domain_name = graphene.String()
    total_resource_slots = graphene.JSONString()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)
    integration_id = graphene.String()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            name=row['name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            modified_at=row['modified_at'],
            domain_name=row['domain_name'],
            total_resource_slots=row['total_resource_slots'].to_json(),
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
            integration_id=row['integration_id'],
        )

    @staticmethod
    async def load_all(context, *,
                       domain_name=None,
                       is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([groups])
                .select_from(groups)
            )
            if domain_name is not None:
                query = query.where(groups.c.domain_name == domain_name)
            if is_active is not None:
                query = query.where(groups.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_id(context, ids, *,
                               domain_name=None):
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([groups])
                .select_from(groups)
                .where(groups.c.id.in_(ids))
            )
            if domain_name is not None:
                query = query.where(groups.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            for k in ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs_per_key[str(row.id)] = o
        return [*objs_per_key.values()]

    @staticmethod
    async def get_groups_for_user(context, user_id):
        async with context['dbpool'].acquire() as conn:
            j = sa.join(groups, association_groups_users,
                        groups.c.id == association_groups_users.c.group_id)
            query = (
                sa.select([groups])
                .select_from(j)
                .where(association_groups_users.c.user_id == user_id)
            )
            objs = []
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs.append(o)
            return objs


class GroupInput(graphene.InputObjectType):
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    domain_name = graphene.String(required=True)
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class ModifyGroupInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    domain_name = graphene.String(required=False)
    total_resource_slots = graphene.JSONString(required=False)
    user_update_mode = graphene.String(required=False)
    user_uuids = graphene.List(lambda: graphene.String, required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class CreateGroup(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = GroupInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    group = graphene.Field(lambda: Group)

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda name, props, **kwargs: (props.domain_name, None)
    )
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            assert _rx_slug.search(name) is not None, 'invalid name format. slug format required.'
            data = {
                'name': name,
                'description': props.description,
                'is_active': props.is_active,
                'domain_name': props.domain_name,
                'total_resource_slots': ResourceSlot.from_user_input(
                    props.total_resource_slots, None),
                'allowed_vfolder_hosts': props.allowed_vfolder_hosts,
                'integration_id': props.integration_id,
            }
            query = (groups.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = groups.select().where((groups.c.name == name) &
                                                   (groups.c.domain_name == props.domain_name))
                    result = await conn.execute(checkq)
                    o = Group.from_row(await result.first())
                    return cls(ok=True, msg='success', group=o)
                else:
                    return cls(ok=False, msg='failed to create group', group=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', group=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', group=None)


class ModifyGroup(graphene.Mutation):

    class Arguments:
        gid = graphene.String(required=True)
        props = ModifyGroupInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    group = graphene.Field(lambda: Group)

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda gid, **kwargs: (None, gid)
    )
    async def mutate(cls, root, info, gid, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}
            set_if_set(props, data, 'name')
            set_if_set(props, data, 'description')
            set_if_set(props, data, 'is_active')
            set_if_set(props, data, 'domain_name')
            set_if_set(props, data, 'total_resource_slots',
                       clean_func=lambda v: ResourceSlot.from_user_input(v, None))
            set_if_set(props, data, 'allowed_vfolder_hosts')
            set_if_set(props, data, 'integration_id')

            if 'name' in data:
                assert _rx_slug.search(data['name']) is not None, \
                    'invalid name format. slug format required.'
            assert props.user_update_mode in (None, 'add', 'remove',), 'invalid user_update_mode'
            if not props.user_uuids:
                props.user_update_mode = None
            if not data and props.user_update_mode is None:
                return cls(ok=False, msg='nothing to update', group=None)

            try:
                if props.user_update_mode == 'add':
                    values = [{'user_id': uuid, 'group_id': gid} for uuid in props.user_uuids]
                    query = sa.insert(association_groups_users).values(values)
                    await conn.execute(query)
                elif props.user_update_mode == 'remove':
                    query = (association_groups_users
                             .delete()
                             .where(association_groups_users.c.user_id.in_(props.user_uuids))
                             .where(association_groups_users.c.group_id == gid))
                    await conn.execute(query)

                if data:
                    query = (groups.update().values(data).where(groups.c.id == gid))
                    result = await conn.execute(query)
                    if result.rowcount > 0:
                        checkq = groups.select().where(groups.c.id == gid)
                        result = await conn.execute(checkq)
                        o = Group.from_row(await result.first())
                        return cls(ok=True, msg='success', group=o)
                    return cls(ok=False, msg='no such group', group=None)
                else:  # updated association_groups_users table
                    return cls(ok=True, msg='success', group=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', group=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', group=None)


class DeleteGroup(graphene.Mutation):

    class Arguments:
        gid = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda gid, **kwargs: (None, gid)
    )
    async def mutate(cls, root, info, gid):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                # query = groups.delete().where(groups.c.id == gid)
                query = groups.update().values(is_active=False,
                                               integration_id=None).where(groups.c.id == gid)
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such group')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}')
