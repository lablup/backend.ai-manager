import asyncio
from collections import OrderedDict
import re

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import psycopg2 as pg
import sqlalchemy as sa

from .base import metadata


__all__ = (
    'domains',
    'Domain', 'DomainInput', 'ModifyDomainInput',
    'CreateDomain', 'ModifyDomain', 'DeleteDomain',
)

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')

domains = sa.Table(
    'domains', metadata,
    sa.Column('name', sa.String(length=64), primary_key=True),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),

    sa.Column('resource_policy', sa.String(length=256),
              sa.ForeignKey('keypair_resource_policy.name'), index=True),
)


class Domain(graphene.ObjectType):
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    resource_policy = graphene.String()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            modified_at=row['modified_at'],
            resource_policy=row['resource_policy'],
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([domains]).select_from(domains)
            if is_active is not None:
                query = query.where(domains.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = Domain.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_name(context, names=None, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([domains])
                       .select_from(domains)
                       .where(domains.c.name.in_(domains)))
            objs_per_key = OrderedDict()
            # For each name, there is only one domain.
            # So we don't build lists in objs_per_key variable.
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Domain.from_row(row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())


class DomainInput(graphene.InputObjectType):
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    resource_policy = graphene.String(required=True)


class ModifyDomainInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    resource_policy = graphene.String(required=False)


class CreateDomain(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = DomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain)

    @classmethod
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            assert _rx_slug.search(name) is not None, 'invalid name format. slug format required.'
            data = {
                'name': name,
                'description': props.description,
                'is_active': props.is_active,
                'resource_policy': props.resource_policy,
            }
            query = (domains.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = domains.select().where(domains.c.name == name)
                    result = await conn.execute(checkq)
                    o = Domain.from_row(await result.first())
                    return cls(ok=True, msg='success', domain=o)
                else:
                    return cls(ok=False, msg='failed to create domain', domain=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', domain=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', domain=None)


class ModifyDomain(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyDomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain)

    @classmethod
    async def mutate(cls, root, info, name, props):
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
            set_if_set('resource_policy')

            query = (domains.update().values(data).where(domains.c.name == name))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = domains.select().where(domains.c.name == name)
                    result = await conn.execute(checkq)
                    o = Domain.from_row(await result.first())
                    return cls(ok=True, msg='success', domain=o)
                else:
                    return cls(ok=False, msg='no such domain', domain=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', domain=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', domain=None)


class DeleteDomain(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                query = domains.delete().where(domains.c.name == name)
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such domain')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}')
