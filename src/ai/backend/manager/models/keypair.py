from __future__ import annotations

import asyncio
import base64
import secrets
from typing import (
    Any,
    Optional,
    Mapping,
    Sequence,
    List,
    Tuple,
    TypedDict,
)
import uuid

from aiopg.sa.connection import SAConnection
from aiopg.sa.result import RowProxy
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.sql.expression import false
import psycopg2 as pg

from ai.backend.common import msgpack
from ai.backend.common.types import (
    AccessKey,
    SecretKey,
)

from .base import (
    ForeignKeyIDColumn,
    Item,
    PaginatedList,
    metadata,
    batch_result,
    batch_multiresult,
    set_if_set,
    simple_db_mutate,
)
from .user import UserRole
from ..defs import RESERVED_DOTFILES

__all__: Sequence[str] = (
    'keypairs',
    'KeyPair', 'KeyPairList',
    'UserInfo',
    'KeyPairInput',
    'CreateKeyPair', 'ModifyKeyPair', 'DeleteKeyPair',
    'Dotfile', 'MAXIMUM_DOTFILE_SIZE',
    'query_owned_dotfiles',
    'query_bootstrap_script',
    'verify_dotfile_name'
)


MAXIMUM_DOTFILE_SIZE = 64 * 1024  # 61 KiB

keypairs = sa.Table(
    'keypairs', metadata,
    sa.Column('user_id', sa.String(length=256), index=True),
    sa.Column('access_key', sa.String(length=20), primary_key=True),
    sa.Column('secret_key', sa.String(length=40)),
    sa.Column('is_active', sa.Boolean, index=True),
    sa.Column('is_admin', sa.Boolean, index=True,
              default=False, server_default=false()),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('concurrency_used', sa.Integer),
    sa.Column('rate_limit', sa.Integer),
    sa.Column('num_queries', sa.Integer, server_default='0'),

    # SSH Keypairs.
    sa.Column('ssh_public_key', sa.String(length=750), nullable=True),
    sa.Column('ssh_private_key', sa.String(length=2000), nullable=True),

    ForeignKeyIDColumn('user', 'users.uuid', nullable=False),
    sa.Column('resource_policy', sa.String(length=256),
              sa.ForeignKey('keypair_resource_policies.name'),
              nullable=False),
    # dotfiles column, \x90 means empty list in msgpack
    sa.Column('dotfiles', sa.LargeBinary(length=MAXIMUM_DOTFILE_SIZE), nullable=False, default=b'\x90'),
    sa.Column('bootstrap_script', sa.String(length=MAXIMUM_DOTFILE_SIZE), nullable=False, default=''),
)


class UserInfo(graphene.ObjectType):
    email = graphene.String()
    full_name = graphene.String()

    @classmethod
    def from_row(
        cls,
        context: Mapping[str, Any],
        row: RowProxy,
    ) -> Optional[UserInfo]:
        if row is None:
            return None
        return cls(email=row['email'], full_name=row['full_name'])

    @classmethod
    async def batch_load_by_uuid(
        cls,
        context: Mapping[str, Any],
        user_uuids: List[uuid.UUID],
    ) -> Sequence[Optional[UserInfo]]:
        async with context['dbpool'].acquire() as conn:
            from .user import users
            query = (
                sa.select([users.c.uuid, users.c.email, users.c.full_name])
                .select_from(users)
                .where(users.c.uuid.in_(user_uuids))
            )
            return await batch_result(
                context, conn, query, cls,
                user_uuids, lambda row: row['uuid'],
            )


class KeyPair(graphene.ObjectType):

    class Meta:
        interfaces = (Item, )

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
    user = graphene.UUID()

    ssh_public_key = graphene.String()

    vfolders = graphene.List('ai.backend.manager.models.VirtualFolder')
    compute_sessions = graphene.List(
        'ai.backend.manager.models.ComputeSession',
        status=graphene.String(),
    )

    user_info = graphene.Field(lambda: UserInfo)

    # Deprecated
    concurrency_limit = graphene.Int(
        deprecation_reason='Moved to KeyPairResourcePolicy object as '
                           'max_concurrent_sessions field.')

    async def resolve_user_info(
        self,
        info: graphene.ResolveInfo,
    ) -> UserInfo:
        manager = info.context['dlmgr']
        loader = manager.get_loader('UserInfo.by_uuid')
        return await loader.load(self.user)

    @classmethod
    def from_row(
        cls,
        context: Mapping[str, Any],
        row: RowProxy,
    ) -> KeyPair:
        return cls(
            id=row['access_key'],
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
            user=row['user'],
            ssh_public_key=row['ssh_public_key'],
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

    @classmethod
    async def load_all(
        cls, context, *,
        domain_name=None,
        is_active=None,
        limit=None,
    ) -> Sequence[KeyPair]:
        from .user import users
        async with context['dbpool'].acquire() as conn:
            j = sa.join(keypairs, users, keypairs.c.user == users.c.uuid)
            query = (
                sa.select([keypairs])
                .select_from(j)
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            if limit is not None:
                query = query.limit(limit)
            return [
                cls.from_row(context, row) async for row in conn.execute(query)
            ]

    @staticmethod
    async def load_count(
        context, *,
        domain_name=None,
        email=None,
        is_active=None,
    ) -> int:
        from .user import users
        async with context['dbpool'].acquire() as conn:
            j = sa.join(keypairs, users, keypairs.c.user == users.c.uuid)
            query = (
                sa.select([sa.func.count(keypairs.c.access_key)])
                .select_from(j)
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if email is not None:
                query = query.where(keypairs.c.user_id == email)
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            result = await conn.execute(query)
            return await result.scalar()

    @classmethod
    async def load_slice(
        cls, context, limit, offset, *,
        domain_name=None,
        email=None,
        is_active=None,
        order_key=None,
        order_asc=True,
    ) -> Sequence[KeyPair]:
        from .user import users
        async with context['dbpool'].acquire() as conn:
            if order_key is None:
                _ordering = sa.desc(keypairs.c.created_at)
            else:
                _order_func = sa.asc if order_asc else sa.desc
                _ordering = _order_func(getattr(keypairs.c, order_key))
            j = sa.join(keypairs, users, keypairs.c.user == users.c.uuid)
            query = (
                sa.select([keypairs])
                .select_from(j)
                .order_by(_ordering)
                .limit(limit)
                .offset(offset)
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if email is not None:
                query = query.where(keypairs.c.user_id == email)
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            return [
                cls.from_row(context, row) async for row in conn.execute(query)
            ]

    @classmethod
    async def batch_load_by_email(
        cls, context, user_ids, *,
        domain_name=None, is_active=None,
    ) -> Sequence[Sequence[Optional[KeyPair]]]:
        from .user import users
        async with context['dbpool'].acquire() as conn:
            j = sa.join(keypairs, users, keypairs.c.user == users.c.uuid)
            query = (sa.select([keypairs])
                       .select_from(j)
                       .where(keypairs.c.user_id.in_(user_ids)))
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if is_active is not None:
                query = query.where(keypairs.c.is_active == is_active)
            return await batch_multiresult(
                context, conn, query, cls,
                user_ids, lambda row: row['user_id'],
            )

    @classmethod
    async def batch_load_by_ak(
        cls, context, access_keys, *,
        domain_name=None,
    ) -> Sequence[Optional[KeyPair]]:
        async with context['dbpool'].acquire() as conn:
            from .user import users
            j = sa.join(keypairs, users, keypairs.c.user == users.c.uuid)
            query = (
                sa.select([keypairs])
                .select_from(j)
                .where(
                    keypairs.c.access_key.in_(access_keys)
                )
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            return await batch_result(
                context, conn, query, cls,
                access_keys, lambda row: row['access_key'],
            )


class KeyPairList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(KeyPair, required=True)


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

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        user_id = graphene.String(required=True)
        props = KeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    keypair = graphene.Field(lambda: KeyPair, required=False)

    @classmethod
    async def mutate(cls, root, info, user_id, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            # Check if user exists with requested email (user_id for legacy).
            from .user import users  # noqa
            query = (sa.select([users.c.uuid])
                       .select_from(users)
                       .where(users.c.email == user_id))
            try:
                result = await conn.execute(query)
                user_uuid = await result.scalar()
                if user_uuid is None:
                    return cls(ok=False, msg=f'User not found: {user_id}', keypair=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', keypair=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', keypair=None)

            # Create keypair.
            ak, sk = generate_keypair()
            pubkey, privkey = generate_ssh_keypair()
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
                'user': user_uuid,
                'ssh_public_key': pubkey,
                'ssh_private_key': privkey,
            }
            insert_query = (keypairs.insert().values(data))
            try:
                result = await conn.execute(insert_query)
                if result.rowcount > 0:
                    # Read the created key data from DB.
                    checkq = keypairs.select().where(keypairs.c.access_key == ak)
                    result = await conn.execute(checkq)
                    o = KeyPair.from_row(info.context, await result.first())
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

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        access_key = graphene.String(required=True)
        props = ModifyKeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, access_key, props):
        data = {}
        set_if_set(props, data, 'is_active')
        set_if_set(props, data, 'is_admin')
        set_if_set(props, data, 'resource_policy')
        set_if_set(props, data, 'rate_limit')
        update_query = (
            keypairs.update()
            .values(data)
            .where(keypairs.c.access_key == access_key)
        )
        return await simple_db_mutate(cls, info.context, update_query)


class DeleteKeyPair(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        access_key = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, access_key):
        delete_query = (
            keypairs.delete()
            .where(keypairs.c.access_key == access_key)
        )
        return await simple_db_mutate(cls, info.context, delete_query)


class Dotfile(TypedDict):
    data: str
    path: str
    perm: str


def generate_keypair() -> Tuple[AccessKey, SecretKey]:
    '''
    AWS-like access key and secret key generation.
    '''
    ak = 'AKIA' + base64.b32encode(secrets.token_bytes(10)).decode('ascii')
    sk = secrets.token_urlsafe(30)
    return AccessKey(ak), SecretKey(sk)


def generate_ssh_keypair() -> Tuple[str, str]:
    '''
    Generate RSA keypair for SSH/SFTP connection.
    '''
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )
    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.TraditionalOpenSSL,
        crypto_serialization.NoEncryption()
    ).decode("utf-8")
    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH,
        crypto_serialization.PublicFormat.OpenSSH
    ).decode("utf-8")
    return (public_key, private_key)


async def query_owned_dotfiles(
    conn: SAConnection,
    access_key: AccessKey,
) -> Tuple[List[Dotfile], int]:
    query = (sa.select([keypairs.c.dotfiles])
               .select_from(keypairs)
               .where(keypairs.c.access_key == access_key))
    packed_dotfile = await conn.scalar(query)
    rows = msgpack.unpackb(packed_dotfile)
    return rows, MAXIMUM_DOTFILE_SIZE - len(packed_dotfile)


async def query_bootstrap_script(
    conn: SAConnection,
    access_key: AccessKey,
) -> Tuple[str, int]:
    query = (sa.select([keypairs.c.bootstrap_script])
               .select_from(keypairs)
               .where(keypairs.c.access_key == access_key))
    script = await conn.scalar(query)
    return script, MAXIMUM_DOTFILE_SIZE - len(script)


def verify_dotfile_name(dotfile: str) -> bool:
    if dotfile in RESERVED_DOTFILES:
        return False
    return True
