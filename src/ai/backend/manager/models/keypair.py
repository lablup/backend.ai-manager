import asyncio
import base64
from collections import OrderedDict
import secrets

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.sql.expression import false
import psycopg2 as pg

from .base import metadata

__all__ = (
    'keypairs',
    'KeyPair', 'KeyPairInput',
    'CreateKeyPair', 'ModifyKeyPair', 'DeleteKeyPair',
)


keypairs = sa.Table(
    'keypairs', metadata,
    sa.Column('user_id', sa.String(length=256), index=True),
    sa.Column('access_key', sa.String(length=20), primary_key=True),
    sa.Column('secret_key', sa.String(length=40)),
    sa.Column('is_active', sa.Boolean, index=True),
    sa.Column('is_admin', sa.Boolean, index=True,
              default=False, server_default=false()),
    sa.Column('resource_policy', sa.String(length=256),
              sa.ForeignKey('keypair_resource_policies.name'),
              nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('concurrency_used', sa.Integer),
    sa.Column('rate_limit', sa.Integer),
    sa.Column('num_queries', sa.Integer, server_default='0'),
)


class KeyPair(graphene.ObjectType):
    user_id = graphene.String()
    access_key = graphene.String()
    secret_key = graphene.String()
    is_active = graphene.Boolean()
    is_admin = graphene.Boolean()
    resource_policy = graphene.String()
    created_at = GQLDateTime()
    last_used = GQLDateTime()
    concurrency_used = graphene.Int()
    rate_limit = graphene.Int()
    num_queries = graphene.Int()

    vfolders = graphene.List('ai.backend.manager.models.VirtualFolder')
    compute_sessions = graphene.List(
        'ai.backend.manager.models.ComputeSession',
        status=graphene.String(),
    )

    # Deprecated
    concurrency_limit = graphene.Int(
        deprecation_reason='Moved to KeyPairResourcePolicy object as '
                           'max_concurrent_sessions field.')

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            user_id=row['user_id'],
            access_key=row['access_key'],
            secret_key=row['secret_key'],
            is_active=row['is_active'],
            is_admin=row['is_admin'],
            resource_policy=row['resource_policy'],
            created_at=row['created_at'],
            last_used=row['last_used'],
            concurrency_limit=0,  # moved to resource policy
            concurrency_used=row['concurrency_used'],
            rate_limit=row['rate_limit'],
            num_queries=row['num_queries'],
        )

    async def resolve_vfolders(self, info):
        manager = info.context['dlmgr']
        loader = manager.get_loader('VirtualFolder')
        return await loader.load(self.access_key)

    async def resolve_compute_sessions(self, info, status=None):
        manager = info.context['dlmgr']
        from . import KernelStatus  # noqa: avoid circular imports
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession', status=status)
        return await loader.load(self.access_key)

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select('*').select_from(keypairs)
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = KeyPair.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_uid(context, user_ids, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(keypairs)
                       .where(keypairs.c.user_id.in_(user_ids)))
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            objs_per_key = OrderedDict()
            for k in user_ids:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = KeyPair.from_row(row)
                objs_per_key[row.user_id].append(o)
        return tuple(objs_per_key.values())

    @staticmethod
    async def batch_load_by_ak(context, access_keys):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(keypairs)
                       .where(keypairs.c.access_key.in_(access_keys)))
            objs_per_key = OrderedDict()
            # For each access key, there is only one keypair.
            # So we don't build lists in objs_per_key variable.
            for k in access_keys:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = KeyPair.from_row(row)
                objs_per_key[row.access_key] = o
        return tuple(objs_per_key.values())


class KeyPairInput(graphene.InputObjectType):
    is_active = graphene.Boolean(required=False, default=True)
    is_admin = graphene.Boolean(required=False, default=False)
    resource_policy = graphene.String(required=True)
    concurrency_limit = graphene.Int(required=False)  # deprecated and ignored
    rate_limit = graphene.Int(required=True)

    # When creating, you MUST set all fields.
    # When modifying, set the field to "None" to skip setting the value.


class ModifyKeyPairInput(graphene.InputObjectType):
    is_active = graphene.Boolean(required=False)
    is_admin = graphene.Boolean(required=False)
    resource_policy = graphene.String(required=False)
    concurrency_limit = graphene.Int(required=False)  # deprecated and ignored
    rate_limit = graphene.Int(required=False)


class CreateKeyPair(graphene.Mutation):

    class Arguments:
        user_id = graphene.String(required=True)
        props = KeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    keypair = graphene.Field(lambda: KeyPair)

    @classmethod
    async def mutate(cls, root, info, user_id, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            ak = 'AKIA' + base64.b32encode(secrets.token_bytes(10)).decode('ascii')
            sk = secrets.token_urlsafe(30)
            data = {
                'user_id': user_id,
                'access_key': ak,
                'secret_key': sk,
                'is_active': bool(props.is_active),
                'is_admin': bool(props.is_admin),
                'resource_policy': props.resource_policy,
                'concurrency_used': 0,
                'rate_limit': props.rate_limit,
                'num_queries': 0,
            }
            query = (keypairs.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    # Read the created key data from DB.
                    checkq = keypairs.select().where(keypairs.c.access_key == ak)
                    result = await conn.execute(checkq)
                    o = KeyPair.from_row(await result.first())
                    return cls(ok=True, msg='success', keypair=o)
                else:
                    return cls(ok=False, msg='failed to create keypair',
                               keypair=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}',
                           keypair=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}',
                           keypair=None)


class ModifyKeyPair(graphene.Mutation):

    class Arguments:
        access_key = graphene.String(required=True)
        props = ModifyKeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, access_key, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    data[name] = v

            set_if_set('is_active')
            set_if_set('is_admin')
            set_if_set('resource_policy')
            set_if_set('rate_limit')

            try:
                query = (keypairs.update()
                                 .values(data)
                                 .where(keypairs.c.access_key == access_key))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such keypair')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')


class DeleteKeyPair(graphene.Mutation):

    class Arguments:
        access_key = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, access_key):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                query = (keypairs.delete()
                                 .where(keypairs.c.access_key == access_key))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such keypair')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')
