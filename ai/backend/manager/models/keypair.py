import base64
from collections import OrderedDict
import secrets

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.sql.expression import false

from .base import metadata

__all__ = (
    'keypairs',
    'KeyPair', 'KeyPairInput',
    'CreateKeyPair', 'ModifyKeyPair', 'DeleteKeyPair',
)


keypairs = sa.Table(
    'keypairs', metadata,
    sa.Column('user_id', sa.Integer(), index=True),  # foreign key
    sa.Column('access_key', sa.String(length=20), primary_key=True),
    sa.Column('secret_key', sa.String(length=40)),
    sa.Column('is_active', sa.Boolean, index=True),
    sa.Column('is_admin', sa.Boolean, index=True,
              default=False, server_default=false()),
    sa.Column('resource_policy', sa.String, nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('concurrency_limit', sa.Integer),
    sa.Column('concurrency_used', sa.Integer),
    sa.Column('rate_limit', sa.Integer),
    sa.Column('num_queries', sa.Integer, server_default='0'),
)


class KeyPair(graphene.ObjectType):
    access_key = graphene.String()
    secret_key = graphene.String()
    is_active = graphene.Boolean()
    is_admin = graphene.Boolean()
    resource_policy = graphene.String()
    created_at = GQLDateTime()
    last_used = GQLDateTime()
    concurrency_limit = graphene.Int()
    concurrency_used = graphene.Int()
    rate_limit = graphene.Int()
    num_queries = graphene.Int()

    vfolders = graphene.List('ai.backend.manager.models.VirtualFolder')
    compute_sessions = graphene.List(
        'ai.backend.manager.models.ComputeSession',
        status=graphene.String(),
    )

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            access_key=row.access_key,
            secret_key=row.secret_key,
            is_active=row.is_active,
            is_admin=row.is_admin,
            resource_policy=row.resource_policy,
            created_at=row.created_at,
            last_used=row.last_used,
            concurrency_limit=row.concurrency_limit,
            concurrency_used=row.concurrency_used,
            rate_limit=row.rate_limit,
            num_queries=row.num_queries,
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
    async def batch_load_by_uid(dbpool, user_ids, *, is_active=None):
        async with dbpool.acquire() as conn:
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
    async def batch_load_by_ak(dbpool, access_keys):
        async with dbpool.acquire() as conn:
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
    is_active = graphene.Boolean()
    resource_policy = graphene.String()
    concurrency_limit = graphene.Int()
    rate_limit = graphene.Int()

    # When creating, you MUST set all fields.
    # When modifying, set the field to "None" to skip setting the value.


class CreateKeyPair(graphene.Mutation):

    class Arguments:
        user_id = graphene.Int(required=True)
        props = KeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    keypair = graphene.Field(lambda: KeyPair)

    @staticmethod
    async def mutate(root, info, user_id, props):
        async with info.context['dbpool'] as conn, conn.begin():
            ak = 'AKIA' + base64.b32encode(secrets.token_bytes(10)).decode('ascii')
            sk = secrets.token_urlsafe(30)
            data = {
                'user_id': user_id,
                'access_key': ak,
                'secret_key': sk,
                'is_active': props.is_active,
                'resource_policy': props.resource_policy,
                'concurrency_limit': props.concurrency_limit,
                'concurrency_used': 0,
                'rate_limit': props.rate_limit,
                'num_queries': 0,
            }
            query = (keypairs.insert().values(data))
            result = await conn.execute(query)
            if result.rowcount > 0:
                o = KeyPair.from_row(data)
                return CreateKeyPair(ok=True, msg='success', keypair=o)
            else:
                return CreateKeyPair(ok=False, msg='failed to create keypair',
                                     keypair=None)


class ModifyKeyPair(graphene.Mutation):

    class Arguments:
        access_key = graphene.String(required=True)
        props = KeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, access_key, props):
        async with info.context['dbpool'] as conn, conn.begin():
            data = {}

            def set_if_set(name):
                v = getattr(props, name)
                if v is not None:
                    data[name] = v

            set_if_set('is_active')
            set_if_set('resource_policy')
            set_if_set('concurrency_limit')
            set_if_set('rate_limit')

            query = (keypairs.update()
                             .values(data)
                             .where(keypairs.c.access_key == access_key))
            result = await conn.execute(query)
            if result.rowcount > 0:
                return ModifyKeyPair(ok=True, msg='success')
            else:
                return ModifyKeyPair(ok=False, msg='failed to modify keypair')


class DeleteKeyPair(graphene.Mutation):

    class Arguments:
        access_key = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, access_key):
        async with info.context['dbpool'] as conn, conn.begin():
            query = (keypairs.delete()
                             .where(keypairs.c.access_key == access_key))
            result = await conn.execute(query)
            if result.rowcount > 0:
                return DeleteKeyPair(ok=True, msg='success')
            else:
                return DeleteKeyPair(ok=False, msg='failed to delete keypair')
