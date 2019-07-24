import asyncio
from collections import OrderedDict
import re

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.types import ResourceSlot
from .base import metadata, ResourceSlotColumn


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
    # TODO: separate resource-related fields with new domain resource policy table when needed.
    sa.Column('total_resource_slots', ResourceSlotColumn(), default='{}'),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False, default='{}'),
    #: Field for synchronization with external services.
    sa.Column('integration_id', sa.String(length=512)),
)


class Domain(graphene.ObjectType):
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    total_resource_slots = graphene.JSONString()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)
    integration_id = graphene.String()

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
            total_resource_slots=row['total_resource_slots'].to_json(),
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
            integration_id=row['integration_id'],
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
                       .where(domains.c.name.in_(names)))
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
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class ModifyDomainInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class DomainMutationMixin:

    @staticmethod
    def check_perm(info):
        from .user import UserRole
        user = info.context['user']
        if user['role'] == UserRole.SUPERADMIN:
            return True
        return False


class CreateDomain(DomainMutationMixin, graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = DomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain)

    @classmethod
    async def mutate(cls, root, info, name, props):
        assert cls.check_perm(info), 'no permission'
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            assert _rx_slug.search(name) is not None, 'invalid name format. slug format required.'
            data = {
                'name': name,
                'description': props.description,
                'is_active': props.is_active,
                'total_resource_slots': ResourceSlot.from_user_input(
                    props.total_resource_slots, None),
                'allowed_vfolder_hosts': props.allowed_vfolder_hosts,
                'integration_id': props.integration_id,
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


class ModifyDomain(DomainMutationMixin, graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyDomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain)

    @classmethod
    async def mutate(cls, root, info, name, props):
        assert cls.check_perm(info), 'no permission'
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name, clean=lambda v: v):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    data[name] = clean(v)

            def clean_resource_slot(v):
                return ResourceSlot.from_user_input(v, None)

            set_if_set('name')  # data['name'] is new domain name
            set_if_set('description')
            set_if_set('is_active')
            set_if_set('total_resource_slots', clean_resource_slot)
            set_if_set('allowed_vfolder_hosts')
            set_if_set('integration_id')

            if 'name' in data:
                assert _rx_slug.search(data['name']) is not None, \
                    'invalid name format. slug format required.'

            query = (domains.update().values(data).where(domains.c.name == name))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    new_name = data['name'] if 'name' in data else name
                    checkq = domains.select().where(domains.c.name == new_name)
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


class DeleteDomain(DomainMutationMixin, graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        assert cls.check_perm(info), 'no permission'
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                # query = domains.delete().where(domains.c.name == name)
                query = domains.update().values(is_active=False).where(domains.c.name == name)
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
