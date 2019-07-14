import asyncio
from collections import OrderedDict
import enum
from typing import Any

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


class PasswordColumn(TypeDecorator):
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        return _hash_password(value)


class UserRole(str, enum.Enum):
    '''
    User's role.
    '''
    SUPERADMIN = 'superadmin'
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
    sa.Column('full_name', sa.String(length=64)),
    sa.Column('description', sa.String(length=500)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    #: Field for synchronization with external services.
    sa.Column('integration_id', sa.String(length=512)),

    # Note: admins without domain_name is global admin.
    sa.Column('domain_name', sa.String(length=64),
              sa.ForeignKey('domains.name'), index=True),
    sa.Column('role', EnumValueType(UserRole), default=UserRole.USER),
)


class UserGroup(graphene.ObjectType):
    id = graphene.UUID()
    name = graphene.String()


class User(graphene.ObjectType):
    uuid = graphene.UUID()
    username = graphene.String()
    email = graphene.String()
    password = graphene.String()
    need_password_change = graphene.Boolean()
    full_name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    domain_name = graphene.String()
    role = graphene.String()
    # Dynamic fields
    groups = graphene.List(lambda: UserGroup)

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        if 'id' in row and row.id is not None and 'name' in row and row.name is not None:
            groups = [UserGroup(id=row['id'], name=row['name'])]
        else:
            groups = None
        return cls(
            uuid=row['uuid'],
            username=row['username'],
            email=row['email'],
            need_password_change=row['need_password_change'],
            full_name=row['full_name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            domain_name=row['domain_name'],
            role=row['role'],
            # Dynamic fields
            groups=groups,  # group information
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        '''
        Load user's information. Group names associated with the user are also returned.
        '''
        async with context['dbpool'].acquire() as conn:
            from .group import groups, association_groups_users as agus
            j = (users.join(agus, agus.c.user_id == users.c.uuid, isouter=True)
                      .join(groups, agus.c.group_id == groups.c.id, isouter=True))
            query = sa.select([users, groups.c.name, groups.c.id]).select_from(j)
            if context['user']['role'] != UserRole.SUPERADMIN:
                query = query.where(users.c.domain_name == context['user']['domain_name'])
            if is_active is not None:
                query = query.where(users.c.is_active == is_active)
            objs_per_key = OrderedDict()
            async for row in conn.execute(query):
                if row.email in objs_per_key:
                    # If same user is already saved, just append group information.
                    objs_per_key[row.email].groups.append(UserGroup(id=row.id, name=row.name))
                    continue
                o = User.from_row(row)
                objs_per_key[row.email] = o
            objs = list(objs_per_key.values())
        return objs

    @staticmethod
    async def batch_load_by_email(context, emails=None, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            try:  # determine whether email is given as uuid
                import uuid
                uuid.UUID(emails[0])
                pk_type = 'uuid'
            except ValueError:
                pk_type = 'email'
            from .group import groups, association_groups_users as agus
            j = (users.join(agus, agus.c.user_id == users.c.uuid, isouter=True)
                      .join(groups, agus.c.group_id == groups.c.id, isouter=True))
            query = (sa.select([users, groups.c.name, groups.c.id])
                       .select_from(j))
            if pk_type == 'uuid':
                query = query.where(users.c.uuid.in_(emails))
            else:
                query = query.where(users.c.email.in_(emails))
            if context['user']['role'] != UserRole.SUPERADMIN:
                query = query.where(users.c.domain_name == context['user']['domain_name'])
            objs_per_key = OrderedDict()
            # For each email, there is only one user.
            # So we don't build lists in objs_per_key variable.
            for k in emails:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                key = str(row.uuid) if pk_type == 'uuid' else row.email
                if objs_per_key[key] is not None:
                    objs_per_key[key].groups.append({'id': str(row.id), 'name': row.name})
                    continue
                o = User.from_row(row)
                objs_per_key[key] = o
        return tuple(objs_per_key.values())


class UserInput(graphene.InputObjectType):
    username = graphene.String(required=True)
    password = graphene.String(required=True)
    need_password_change = graphene.Boolean(required=True)
    full_name = graphene.String(required=False, default='')
    description = graphene.String(required=False, default='')
    is_active = graphene.Boolean(required=False, default=True)
    domain_name = graphene.String(required=True, default='default')
    role = graphene.String(required=False, default=UserRole.USER)
    group_ids = graphene.List(lambda: graphene.String, required=False)

    # When creating, you MUST set all fields.
    # When modifying, set the field to "None" to skip setting the value.


class ModifyUserInput(graphene.InputObjectType):
    username = graphene.String(required=False)
    password = graphene.String(required=False)
    need_password_change = graphene.Boolean(required=False)
    full_name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    domain_name = graphene.String(required=False)
    role = graphene.String(required=False)
    group_ids = graphene.List(lambda: graphene.String, required=False)


class UserMutationMixin:

    @staticmethod
    def check_perm(info):
        from .user import UserRole
        user = info.context['user']
        if user['role'] == UserRole.SUPERADMIN:
            return True  # only superadmin is allowed to mutate, currently
        return False


class CreateUser(UserMutationMixin, graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = UserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    async def mutate(cls, root, info, email, props):
        assert cls.check_perm(info), 'no permission'
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            username = props.username if props.username else email
            data = {
                'username': username,
                'email': email,
                'password': props.password,
                'need_password_change': props.need_password_change,
                'full_name': props.full_name,
                'description': props.description,
                'is_active': props.is_active,
                'domain_name': props.domain_name,
                'role': UserRole(props.role),
            }
            try:
                query = (users.insert().values(data))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    # Read the created user data from DB.
                    checkq = users.select().where(users.c.email == email)
                    result = await conn.execute(checkq)
                    o = User.from_row(await result.first())

                    # Create user's first access_key and secret_key.
                    from .keypair import generate_keypair, keypairs
                    ak, sk = generate_keypair()
                    is_admin = True if data['role'] in [UserRole.SUPERADMIN, UserRole.ADMIN] else False
                    kp_data = {
                        'user_id': email,
                        'access_key': ak,
                        'secret_key': sk,
                        'is_active': True,
                        'is_admin': is_admin,
                        'resource_policy': 'default',
                        'concurrency_used': 0,
                        'rate_limit': 1000,
                        'num_queries': 0,
                        'user': o.uuid,
                    }
                    query = (keypairs.insert().values(kp_data))
                    await conn.execute(query)

                    # Add user to groups if group_ids parameter is provided.
                    from .group import association_groups_users, groups
                    if props.group_ids:
                        query = (sa.select([groups.c.id])
                                   .select_from(groups)
                                   .where(groups.c.domain_name == info.context['user']['domain_name'])
                                   .where(groups.c.id.in_(props.group_ids)))
                        result = await conn.execute(query)
                        grps = await result.fetchall()
                        if grps:
                            values = [{'user_id': o.uuid, 'group_id': grp.id} for grp in grps]
                            query = association_groups_users.insert().values(values)
                            await conn.execute(query)
                    return cls(ok=True, msg='success', user=o)
                else:
                    return cls(ok=False, msg='failed to create user', user=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', user=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', user=None)


class ModifyUser(UserMutationMixin, graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = ModifyUserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    async def mutate(cls, root, info, email, props):
        assert cls.check_perm(info), 'no permission'
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
            set_if_set('full_name')
            set_if_set('description')
            set_if_set('is_active')
            # set_if_set('domain_name')  # prevent changing domain_name
            set_if_set('role')

            if not data and not props.group_ids:
                return cls(ok=False, msg='nothing to update', user=None)

            try:
                query = (users.update().values(data).where(users.c.email == email))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = users.select().where(users.c.email == email)
                    result = await conn.execute(checkq)
                    o = User.from_row(await result.first())
                else:
                    return cls(ok=False, msg='no such user', user=None)
                # Update user's group if group_ids parameter is provided.
                if props.group_ids and o is not None:
                    from .group import association_groups_users, groups
                    # TODO: isn't it dangerous if second execution breaks,
                    #       which results in user lost all of groups?
                    # Clear previous groups associated with the user.
                    query = (association_groups_users
                             .delete()
                             .where(association_groups_users.c.user_id == o.uuid))
                    await conn.execute(query)
                    # Add user to new groups.
                    query = (sa.select([groups.c.id])
                               .select_from(groups)
                               .where(groups.c.domain_name == info.context['user']['domain_name'])
                               .where(groups.c.id.in_(props.group_ids)))
                    result = await conn.execute(query)
                    grps = await result.fetchall()
                    if grps:
                        values = [{'user_id': o.uuid, 'group_id': grp.id} for grp in grps]
                        query = association_groups_users.insert().values(values)
                        await conn.execute(query)
                return cls(ok=True, msg='success', user=o)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', user=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', user=None)


class DeleteUser(UserMutationMixin, graphene.Mutation):
    '''
    Instead of deleting user, just inactive the account.

    All related keypairs will also be inactivated.
    '''

    class Arguments:
        email = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, email):
        assert cls.check_perm(info), 'no permission'
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                # query = (users.delete().where(users.c.email == email))
                # Make all user keypairs inactive.
                from ai.backend.manager.models import keypairs
                query = (keypairs.update()
                                 .values(is_active=False)
                                 .where(keypairs.c.user_id == email))
                await conn.execute(query)
                # Inactivate user.
                query = (users.update().values(is_active=False).where(users.c.email == email))
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


def _hash_password(password):
    return bcrypt.hash(password, rounds=12)


def _verify_password(guess, hashed):
    return bcrypt.verify(guess, hashed)


async def check_credential(dbpool, domain: str, email: str, password: str) \
                          -> Any:
    async with dbpool.acquire() as conn:
        query = (sa.select([users])
                   .select_from(users)
                   .where((users.c.email == email) &
                          (users.c.domain_name == domain)))
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            return None
        if row['password'] is None:
            # user password is not set.
            return None
        try:
            if _verify_password(password, row['password']):
                return row
        except ValueError:
            return None
        return None
