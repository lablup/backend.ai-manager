import asyncio
from collections import OrderedDict
import re

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from passlib.hash import bcrypt
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, VARCHAR

from .base import metadata, EnumValueType, IDColumn


__all__ = (
    'groups',
    # 'Group', 'GroupInput', 'GroupRole',
    # 'CreateGroup', 'ModifyGroup', 'DeleteGroup',
)

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')

groups = sa.Table(
    'groups', metadata,
    IDColumn('id'),
    sa.Column('name', sa.String(length=64)),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),

    sa.Column('domain', sa.String(length=64), sa.ForeignKey('domains.name'), index=True),
    sa.Column('resource_policy', sa.String(length=256),
              sa.ForeignKey('keypair_resource_policy.name'), index=True),
)


class Group(graphene.ObjectType):
    id = graphene.UUID()
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    domain = graphene.String()
    resource_policy = graphene.String()

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
            domain=row['domain'],
            resource_policy=row['resource_policy'],
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([groups]).select_from(groups)
            if is_active is not None:
                query = query.where(groups.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_id(context, ids=None, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([groups])
                       .select_from(groups)
                       .where(groups.c.id.in_(groups)))
            objs_per_key = OrderedDict()
            # For each name, there is only one domain.
            # So we don't build lists in objs_per_key variable.
            for k in ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())


class GroupInput(graphene.InputObjectType):
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    domain = graphene.String(required=True)
    resource_policy = graphene.String(required=True)


class ModifyGroupInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    domain = graphene.String(required=False)
    resource_policy = graphene.String(required=False)


class CreateGroup(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = GroupInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    group = graphene.Field(lambda: Group)

    @classmethod
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            assert _rx_slug.search(name) is not None, 'invalid name format. slug format required.'
            data = {
                'name': name,
                'description': props.description,
                'is_active': props.is_active,
                'domain': props.domain,
                'resource_policy': props.resource_policy,
            }
            query = (groups.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = groups.select().where(groups.c.name == name &
                                                   groups.c.domain == props.domain)
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
    async def mutate(cls, root, info, gid, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    data[name] = v
            assert _rx_slug.search(data['name']) is not None, \
                'invalid name format. slug format required.'

            set_if_set('name')
            set_if_set('description')
            set_if_set('is_active')
            set_if_set('domain')
            set_if_set('resource_policy')

            query = (groups.update().values(data).where(groups.c.id == gid))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = groups.select().where(groups.c.id == gid)
                    result = await conn.execute(checkq)
                    o = Group.from_row(await result.first())
                    return cls(ok=True, msg='success', group=o)
                else:
                    return cls(ok=False, msg='no such group', group=None)
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
    async def mutate(cls, root, info, gid):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                query = groups.delete().where(groups.c.id == gid)
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
