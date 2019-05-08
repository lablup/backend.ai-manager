import asyncio
from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from passlib.hash import bcrypt
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, VARCHAR

from .base import metadata, EnumValueType, IDColumn


__all__ = (
    'users',
    'User', 'UserInput', 'ModifyUserInput', 'UserRole',
    'CreateUser', 'ModifyUser', 'DeleteUser',
)


def _hash_password(password):
    return bcrypt.hash(password, rounds=12)


def _verify_password(guess, hashed):
    return bcrypt.verify(guess, hashed)


class PasswordColumn(TypeDecorator):
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        return _hash_password(value)


class UserRole(str, enum.Enum):
    '''
    User's role.
    '''
    ADMIN = 'admin'
    USER = 'user'
    MONITOR = 'monitor'


users = sa.Table(
    'users', metadata,
    IDColumn('uuid'),
    sa.Column('username', sa.String(length=64), unique=True),
    sa.Column('email', sa.String(length=64), index=True,
              nullable=False, unique=True),
    sa.Column('password', PasswordColumn()),
    sa.Column('need_password_change', sa.Boolean),
    sa.Column('first_name', sa.String(length=32)),
    sa.Column('last_name', sa.String(length=32)),
    sa.Column('description', sa.String(length=500)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),

    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'),
              nullable=False, index=True),
    sa.Column('role', EnumValueType(UserRole), default=UserRole.USER),
)


class User(graphene.ObjectType):
    uuid = graphene.UUID()
    username = graphene.String()
    email = graphene.String()
    password = graphene.String()
    need_password_change = graphene.Boolean()
    first_name = graphene.String()
    last_name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    domain_name = graphene.String()
    role = graphene.String()
    # Dynamic properties
    full_name = graphene.String()
    # Authentication
    password_correct = graphene.Boolean()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            uuid=row['uuid'],
            username=row['username'],
            email=row['email'],
            need_password_change=row['need_password_change'],
            first_name=row['first_name'],
            last_name=row['last_name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            domain_name=row['domain_name'],
            role=row['role'],
            # Dynamic properties
            full_name=row['first_name'] + ' ' + row['last_name'],
        )

    @classmethod
    def from_row_authenticate(cls, row):
        if row is None:
            return None
        return cls(
            password_correct=row
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([users]).select_from(users)
            if is_active is not None:
                query = query.where(users.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = User.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_email(context, emails=None, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([users])
                       .select_from(users)
                       .where(users.c.email.in_(emails)))
            objs_per_key = OrderedDict()
            # For each email, there is only one user.
            # So we don't build lists in objs_per_key variable.
            for k in emails:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = User.from_row(row)
                objs_per_key[row.email] = o
        return tuple(objs_per_key.values())

    @classmethod
    async def check_password(cls, context, email=None, password=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([users])
                       .select_from(users)
                       .where(users.c.email == email))
            result = await conn.execute(query)
            row = await result.first()
            User.from_row_authenticate(row)
            correct = _verify_password(password, row.password)
        return cls(password_correct=correct)


class UserInput(graphene.InputObjectType):
    username = graphene.String(required=True)
    password = graphene.String(required=True)
    need_password_change = graphene.Boolean(required=True)
    first_name = graphene.String(required=False)
    last_name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    domain_name = graphene.String(required=True, default='default')
    role = graphene.String(required=False, default=UserRole.USER)

    # When creating, you MUST set all fields.
    # When modifying, set the field to "None" to skip setting the value.


class ModifyUserInput(graphene.InputObjectType):
    username = graphene.String(required=False)
    password = graphene.String(required=False)
    need_password_change = graphene.Boolean(required=False)
    first_name = graphene.String(required=False)
    last_name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    domain_name = graphene.String(required=False)
    role = graphene.String(required=False)


class CreateUser(graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = UserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    async def mutate(cls, root, info, email, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            username = props.username if props.username else email
            data = {
                'username': username,
                'email': email,
                'password': props.password,
                'need_password_change': props.need_password_change,
                'first_name': props.first_name,
                'last_name': props.last_name,
                'description': props.description,
                'is_active': props.is_active,
                'domain_name': props.domain_name,
                'role': UserRole(props.role),
            }
            query = (users.insert().values(data))

            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    # Read the created user data from DB.
                    checkq = users.select().where(users.c.email == email)
                    result = await conn.execute(checkq)
                    o = User.from_row(await result.first())
                    return cls(ok=True, msg='success', user=o)
                else:
                    return cls(ok=False, msg='failed to create user', user=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', user=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', user=None)


class ModifyUser(graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = ModifyUserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    async def mutate(cls, root, info, email, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    if name == 'role':
                        v = UserRole(v)
                    data[name] = v

            set_if_set('username')
            set_if_set('password')
            set_if_set('need_password_change')
            set_if_set('first_name')
            set_if_set('last_name')
            set_if_set('description')
            set_if_set('is_active')
            set_if_set('domain_name')
            set_if_set('role')

            query = (users.update().values(data).where(users.c.email == email))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = users.select().where(users.c.email == email)
                    result = await conn.execute(checkq)
                    o = User.from_row(await result.first())
                    return cls(ok=True, msg='success', user=o)
                else:
                    return cls(ok=False, msg='no such user', user=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', user=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', user=None)


class DeleteUser(graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, email):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                query = (users.delete().where(users.c.email == email))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such user')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}')
