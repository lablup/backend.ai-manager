import asyncio
from collections import OrderedDict
import enum
import logging

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
import psycopg2 as pg

from ai.backend.common.logging import BraceStyleAdapter
from . import keypairs
from .base import metadata, BigInt, EnumType

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.models'))

__all__ = (
    'keypair_resource_policies',
    'KeyPairResourcePolicy',
    'DefaultForUnspecified',
    'CreateKeyPairResourcePolicy',
    'ModifyKeyPairResourcePolicy',
    'DeleteKeyPairResourcePolicy',
)


class DefaultForUnspecified(enum.Enum):
    LIMITED = 0
    UNLIMITED = 1


keypair_resource_policies = sa.Table(
    'keypair_resource_policies', metadata,
    sa.Column('name', sa.String(length=256), primary_key=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('default_for_unspecified',
              EnumType(DefaultForUnspecified),
              default=DefaultForUnspecified.LIMITED,
              nullable=False),
    sa.Column('total_resource_slots', pgsql.JSONB(), nullable=False),
    sa.Column('max_concurrent_sessions', sa.Integer(), nullable=False),
    sa.Column('max_containers_per_session', sa.Integer(), nullable=False),
    sa.Column('max_vfolder_count', sa.Integer(), nullable=False),
    sa.Column('max_vfolder_size', sa.BigInteger(), nullable=False),
    sa.Column('idle_timeout', sa.BigInteger(), nullable=False),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False),
    # TODO: implement with a many-to-many association table
    # sa.Column('allowed_scaling_groups', sa.Array(sa.String), nullable=False),
)


class KeyPairResourcePolicy(graphene.ObjectType):
    name = graphene.String()
    created_at = GQLDateTime()
    default_for_unspecified = graphene.String()
    total_resource_slots = graphene.JSONString()
    max_concurrent_sessions = graphene.Int()
    max_containers_per_session = graphene.Int()
    idle_timeout = BigInt()
    max_vfolder_count = graphene.Int()
    max_vfolder_size = BigInt()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            created_at=row['created_at'],
            default_for_unspecified=row['default_for_unspecified'].name,
            total_resource_slots=row['total_resource_slots'],
            max_concurrent_sessions=row['max_concurrent_sessions'],
            max_containers_per_session=row['max_containers_per_session'],
            idle_timeout=row['idle_timeout'],
            max_vfolder_count=row['max_vfolder_count'],
            max_vfolder_size=row['max_vfolder_size'],
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
        )

    @classmethod
    async def load_all(cls, context):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([keypair_resource_policies])
                       .select_from(keypair_resource_policies))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [cls.from_row(context, r) for r in rows]

    @classmethod
    async def load_all_user(cls, context, access_key):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user_id])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == access_key))
            result = await conn.execute(query)
            row = await result.fetchone()
            user_id = row['user_id']
            j = sa.join(
                keypairs, keypair_resource_policies,
                keypairs.c.resource_policy == keypair_resource_policies.c.name
            )
            query = (sa.select([keypair_resource_policies])
                       .select_from(j)
                       .where((keypairs.c.user_id == user_id)))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [cls.from_row(context, r) for r in rows]

    @classmethod
    async def batch_load_by_name(cls, context, names):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([keypair_resource_policies])
                       .select_from(keypair_resource_policies)
                       .where(keypair_resource_policies.c.name.in_(names))
                       .order_by(keypair_resource_policies.c.name))
            objs_per_key = OrderedDict()
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = cls.from_row(context, row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())

    @classmethod
    async def batch_load_by_name_user(cls, context, names):
        async with context['dbpool'].acquire() as conn:
            access_key = context['access_key']
            j = sa.join(
                keypairs, keypair_resource_policies,
                keypairs.c.resource_policy == keypair_resource_policies.c.name
            )
            query = (sa.select([keypair_resource_policies])
                       .select_from(j)
                       .where((keypair_resource_policies.c.name.in_(names)) &
                              (keypairs.c.access_key == access_key))
                       .order_by(keypair_resource_policies.c.name))
            objs_per_key = OrderedDict()
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = cls.from_row(context, row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())

    @classmethod
    async def batch_load_by_ak(cls, context, access_keys):
        async with context['dbpool'].acquire() as conn:
            j = sa.join(
                keypairs, keypair_resource_policies,
                keypairs.c.resource_policy == keypair_resource_policies.c.name
            )
            query = (sa.select([keypair_resource_policies])
                       .select_from(j)
                       .where((keypairs.c.access_key.in_(access_keys)))
                       .order_by(keypair_resource_policies.c.name))
            objs_per_key = OrderedDict()
            async for row in conn.execute(query):
                o = cls.from_row(context, row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())


class CreateKeyPairResourcePolicyInput(graphene.InputObjectType):
    default_for_unspecified = graphene.String(required=True)
    total_resource_slots = graphene.JSONString(required=True)
    max_concurrent_sessions = graphene.Int(required=True)
    max_containers_per_session = graphene.Int(required=True)
    idle_timeout = BigInt(required=True)
    max_vfolder_count = graphene.Int(required=True)
    max_vfolder_size = BigInt(required=True)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)


class ModifyKeyPairResourcePolicyInput(graphene.InputObjectType):
    default_for_unspecified = graphene.String(required=False)
    total_resource_slots = graphene.JSONString(required=False)
    max_concurrent_sessions = graphene.Int(required=False)
    max_containers_per_session = graphene.Int(required=False)
    idle_timeout = BigInt(required=False)
    max_vfolder_count = graphene.Int(required=False)
    max_vfolder_size = BigInt(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)


class CreateKeyPairResourcePolicy(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = CreateKeyPairResourcePolicyInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    resource_policy = graphene.Field(lambda: KeyPairResourcePolicy)

    @classmethod
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {
                'name': name,
                'default_for_unspecified':
                    DefaultForUnspecified[props.default_for_unspecified],
                'total_resource_slots': props.total_resource_slots,
                'max_concurrent_sessions': props.max_concurrent_sessions,
                'max_containers_per_session': props.max_containers_per_session,
                'idle_timeout': props.idle_timeout,
                'max_vfolder_count': props.max_vfolder_count,
                'max_vfolder_size': props.max_vfolder_size,
                'allowed_vfolder_hosts': props.allowed_vfolder_hosts,
            }
            query = (keypair_resource_policies.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = (
                        keypair_resource_policies.select()
                        .where(keypair_resource_policies.c.name == name))
                    result = await conn.execute(checkq)
                    o = KeyPairResourcePolicy.from_row(
                        info.context, await result.first())
                    return cls(ok=True, msg='success', resource_policy=o)
                else:
                    return cls(ok=False, msg='failed to create resource policy',
                               resource_policy=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}',
                           resource_policy=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(
                    ok=False,
                    msg=f'unexpected error ({type(e).__name__}): {e}',
                    resource_policy=None)


class ModifyKeyPairResourcePolicy(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyKeyPairResourcePolicyInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name, clean=lambda v: v):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    data[name] = clean(v)

            set_if_set('default_for_unspecified', lambda v: DefaultForUnspecified[v])
            set_if_set('total_resource_slots')
            set_if_set('max_concurrent_sessions')
            set_if_set('max_containers_per_session')
            set_if_set('idle_timeout')
            set_if_set('max_vfolder_count')
            set_if_set('max_vfolder_size')
            set_if_set('allowed_vfolder_hosts')

            try:
                query = (
                    keypair_resource_policies.update()
                    .values(data)
                    .where(keypair_resource_policies.c.name == name))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such resource policy')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')


class DeleteKeyPairResourcePolicy(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            query = (
                keypair_resource_policies.delete()
                .where(keypair_resource_policies.c.name == name))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such resource policy')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')
