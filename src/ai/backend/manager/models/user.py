import asyncio
from collections import OrderedDict
import enum
from typing import Any, Sequence

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from passlib.hash import bcrypt
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, VARCHAR

from .base import (
    metadata, EnumValueType, IDColumn,
    privileged_mutation,
    set_if_set,
)


__all__: Sequence[str] = (
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
    async def load_all(context, *,
                       domain_name=None, group_id=None,
                       is_active=None):
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
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(groups.c.id == group_id)
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
    async def batch_load_by_email(context, emails=None, *,
                                  domain_name=None,
                                  is_active=None):
        async with context['dbpool'].acquire() as conn:
            from .group import groups, association_groups_users as agus
            j = (users.join(agus, agus.c.user_id == users.c.uuid, isouter=True)
                      .join(groups, agus.c.group_id == groups.c.id, isouter=True))
            query = (
                sa.select([users, groups.c.name, groups.c.id])
                .select_from(j)
                .where(users.c.email.in_(emails))
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            # For each email, there is only one user.
            # So we don't build lists in objs_per_key variable.
            for k in emails:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                key = row.email
                if objs_per_key[key] is not None:
                    objs_per_key[key].groups.append(UserGroup(id=row.id, name=row.name))
                    continue
                o = User.from_row(row)
                objs_per_key[key] = o
        return tuple(objs_per_key.values())

    @staticmethod
    async def batch_load_by_uuid(context, user_ids=None, *,
                                 domain_name=None,
                                 is_active=None):
        async with context['dbpool'].acquire() as conn:
            from .group import groups, association_groups_users as agus
            j = (users.join(agus, agus.c.user_id == users.c.uuid, isouter=True)
                      .join(groups, agus.c.group_id == groups.c.id, isouter=True))
            query = (
                sa.select([users, groups.c.name, groups.c.id])
                .select_from(j)
                .where(users.c.uuid.in_(user_ids))
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            # For each uuid, there is only one user.
            # So we don't build lists in objs_per_key variable.
            for k in user_ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                key = str(row.uuid)
                if objs_per_key[key] is not None:
                    objs_per_key[key].groups.append(UserGroup(id=row.id, name=row.name))
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


class CreateUser(graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = UserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, email, props):
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
                        'rate_limit': 10000,
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
                                   .where(groups.c.domain_name == props.domain_name)
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


class ModifyUser(graphene.Mutation):

    class Arguments:
        email = graphene.String(required=True)
        props = ModifyUserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User)

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, email, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():

            data = {}
            set_if_set(props, data, 'username')
            set_if_set(props, data, 'password')
            set_if_set(props, data, 'need_password_change')
            set_if_set(props, data, 'full_name')
            set_if_set(props, data, 'description')
            set_if_set(props, data, 'is_active')
            set_if_set(props, data, 'domain_name')
            set_if_set(props, data, 'role')
            if 'role' in data:
                data['role'] = UserRole(data['role'])

            if not data and not props.group_ids:
                return cls(ok=False, msg='nothing to update', user=None)

            try:
                # Get previous domain name of the user.
                query = (sa.select([users.c.domain_name, users.c.role])
                           .select_from(users)
                           .where(users.c.email == email))
                result = await conn.execute(query)
                row = await result.fetchone()
                prev_domain_name = row.domain_name
                prev_role = row.role

                # Update user.
                query = (users.update().values(data).where(users.c.email == email))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = users.select().where(users.c.email == email)
                    result = await conn.execute(checkq)
                    o = User.from_row(await result.first())
                else:
                    return cls(ok=False, msg='no such user', user=None)

                # Update keypair if user's role is updated.
                # NOTE: This assumes that user have only one keypair.
                if 'role' in data and data['role'] != prev_role:
                    from ai.backend.manager.models import keypairs
                    query = (sa.select([keypairs.c.user,
                                        keypairs.c.is_active,
                                        keypairs.c.is_admin])
                               .select_from(keypairs)
                               .where(keypairs.c.user == o.uuid)
                               .order_by(sa.desc(keypairs.c.is_admin))
                               .order_by(sa.desc(keypairs.c.is_active)))
                    result = await conn.execute(query)
                    if data['role'] in [UserRole.SUPERADMIN, UserRole.ADMIN]:
                        # User's becomes admin. Set the keypair as active admin.
                        kp = await result.fetchone()
                        kp_data = dict()
                        if not kp.is_admin:
                            kp_data['is_admin'] = True
                        if not kp.is_active:
                            kp_data['is_active'] = True
                        if len(kp_data.keys()) > 0:
                            query = (keypairs.update()
                                             .values(kp_data)
                                             .where(keypairs.c.user == o.uuid))
                            await conn.execute(query)
                    else:
                        # User becomes non-admin. Make the keypair non-admin as well.
                        # If there are multiple admin keypairs, inactivate them.
                        rows = await result.fetchall()
                        cnt = 0
                        for row in rows:
                            kp_data = dict()
                            if cnt == 0:
                                kp_data['is_admin'] = False
                            elif row.is_admin and row.is_active:
                                kp_data['is_active'] = False
                            if len(kp_data.keys()) > 0:
                                query = (keypairs.update()
                                                 .values(kp_data)
                                                 .where(keypairs.c.user == row.user))
                                await conn.execute(query)
                            cnt += 1

                # If domain is changed and no group is associated, clear previous domain's group.
                if prev_domain_name != o.domain_name and not props.group_ids:
                    from .group import association_groups_users, groups
                    query = (association_groups_users
                             .delete()
                             .where(association_groups_users.c.user_id == o.uuid))
                    await conn.execute(query)

                # Update user's group if group_ids parameter is provided.
                if props.group_ids and o is not None:
                    from .group import association_groups_users, groups  # noqa
                    # Clear previous groups associated with the user.
                    query = (association_groups_users
                             .delete()
                             .where(association_groups_users.c.user_id == o.uuid))
                    await conn.execute(query)
                    # Add user to new groups.
                    query = (sa.select([groups.c.id])
                               .select_from(groups)
                               .where(groups.c.domain_name == o.domain_name)
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


class DeleteUser(graphene.Mutation):
    '''
    Instead of deleting user, just inactive the account.

    All related keypairs will also be inactivated.
    '''

    class Arguments:
        email = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, email):
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
