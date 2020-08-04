import asyncio
from collections import OrderedDict
import enum
import logging
from pathlib import Path
import shutil
from typing import (
    Any,
    Sequence,
)
import uuid

from aiopg.sa.connection import SAConnection
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from passlib.hash import bcrypt
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, VARCHAR

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.utils import current_loop
from .base import (
    metadata, EnumValueType, IDColumn,
    set_if_set,
)

log = BraceStyleAdapter(logging.getLogger(__file__))


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
    """
    User's role.
    """
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
        """
        Load user's information. Group names associated with the user are also returned.
        """
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


class PurgeUserInput(graphene.InputObjectType):
    purge_shared_vfolders = graphene.Boolean(required=False, default=False)


class CreateUser(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        email = graphene.String(required=True)
        props = UserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    user = graphene.Field(lambda: User, required=False)

    @classmethod
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
                    from .keypair import generate_keypair, generate_ssh_keypair, keypairs
                    ak, sk = generate_keypair()
                    pubkey, privkey = generate_ssh_keypair()
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
                        'ssh_public_key': pubkey,
                        'ssh_private_key': privkey,
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

    allowed_roles = (UserRole.SUPERADMIN,)

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
    """
    Instead of really deleting user, just mark the account as deleted status.

    All related keypairs will also be inactivated.
    """

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        email = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
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


class PurgeUser(graphene.Mutation):
    """
    Delete user as well as all user-related DB informations such as keypairs, kernels, etc.

    If target user has virtual folders, they can be purged together or migrated to the superadmin.

    vFolder treatment policy:
      User-type:
      - vfolder is not shared: delete
      - vfolder is shared:
        + if purge_shared_vfolder is True: delete
        + else: change vfolder's owner to requested admin

    This action cannot be undone.
    """
    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        email = graphene.String(required=True)
        props = PurgeUserInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, email, props):
        purge_shared_vfolders = props.purge_shared_vfolders if props.purge_shared_vfolders else False
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                query = (
                    sa.select([users.c.uuid])
                    .select_from(users)
                    .where(users.c.email == email)
                )
                user_uuid = await conn.scalar(query)
                log.info('completly deleting user {0}...', email)

                if not purge_shared_vfolders:
                    await cls.migrate_shared_vfolders(
                        conn,
                        deleted_user_uuid=user_uuid,
                        target_user_uuid=info.context['user']['uuid'],
                        target_user_email=info.context['user']['email'],
                    )
                await cls.delete_vfolders(conn, user_uuid, info.context['config_server'])
                await cls.delete_kernels(conn, user_uuid)
                await cls.delete_keypairs(conn, user_uuid)

                query = (
                    users.delete()
                    .where(users.c.email == email)
                )
                result = await conn.execute(query)
                if result.rowcount > 0:
                    log.info('user is deleted: {0}', email)
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such user')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}')

    @classmethod
    async def migrate_shared_vfolders(
        cls,
        conn: SAConnection,
        deleted_user_uuid: uuid.UUID,
        target_user_uuid: uuid.UUID,
        target_user_email: str,
    ) -> int:
        """
        Migrate shared virtual folders' ownership to a target user.

        If migrating virtual folder's name collides with target user's already
        existing folder, append random string to the migrating one.

        :param conn: DB connection
        :param deleted_user_uuid: user's UUID who will be deleted
        :param target_user_uuid: user's UUID who will get the ownership of virtual folders

        :return: number of deleted rows
        """
        from . import vfolders, vfolder_invitations, vfolder_permissions
        # Gather target user's virtual folders' names.
        query = (
            sa.select([vfolders.c.name])
            .select_from(vfolders)
            .where(vfolders.c.user == target_user_uuid)
        )
        existing_vfolder_names = [row.name async for row in conn.execute(query)]

        # Migrate shared virtual folders.
        # If virtual folder's name collides with target user's folder,
        # append random string to the name of the migrating folder.
        j = vfolder_permissions.join(
            vfolders,
            vfolder_permissions.c.vfolder == vfolders.c.id
        )
        query = (
            sa.select([vfolders.c.id, vfolders.c.name])
            .select_from(j)
            .where(vfolders.c.user == deleted_user_uuid)
        )
        migrate_updates = []
        async for row in conn.execute(query):
            name = row.name
            if name in existing_vfolder_names:
                name += f'-{uuid.uuid4().hex[:10]}'
            migrate_updates.append({'vid': row.id, 'vname': name})
        if migrate_updates:
            # Remove invitations and vfolder_permissions from target user.
            # Target user will be the new owner, and it does not make sense to have
            # invitation and shared permission for its own folder.
            migrate_vfolder_ids = [item['vid'] for item in migrate_updates]
            query = (
                vfolder_invitations.delete()
                .where((vfolder_invitations.c.invitee == target_user_email) &
                       (vfolder_invitations.c.vfolder.in_(migrate_vfolder_ids)))
            )
            await conn.execute(query)
            query = (
                vfolder_permissions.delete()
                .where((vfolder_permissions.c.user == target_user_uuid) &
                       (vfolder_permissions.c.vfolder.in_(migrate_vfolder_ids)))
            )
            await conn.execute(query)

            rowcount = 0
            for item in migrate_updates:
                query = (
                    vfolders.update()
                    .values(
                        user=target_user_uuid,
                        name=item['vname'],
                    )
                    .where(vfolders.c.id == item['vid'])
                )
                result = await conn.execute(query)
                rowcount += result.rowcount
            if rowcount > 0:
                log.info('{0} shared folders detected. migrated to user {1}',
                         rowcount, target_user_uuid)
            return rowcount
        else:
            return 0

    @classmethod
    async def delete_vfolders(
        cls,
        conn: SAConnection,
        user_uuid: uuid.UUID,
        config_server,
    ) -> int:
        """
        Delete user's all virtual folders as well as their physical data.

        :param conn: DB connection
        :param user_uuid: user's UUID to delete virtual folders

        :return: number of deleted rows
        """
        from . import vfolders
        mount_prefix = Path(await config_server.get('volumes/_mount'))
        fs_prefix = await config_server.get('volumes/_fsprefix')
        fs_prefix = Path(fs_prefix.lstrip('/'))
        query = (
            sa.select([vfolders.c.id, vfolders.c.host])
            .select_from(vfolders)
            .where(vfolders.c.user == user_uuid)
        )
        async for row in conn.execute(query):
            folder_path = (mount_prefix / row['host'] / fs_prefix / row['id'].hex)
            log.info('deleting physical files: {0}', folder_path)
            try:
                loop = current_loop()
                await loop.run_in_executor(None, lambda: shutil.rmtree(folder_path))  # type: ignore
            except IOError:
                pass
        query = (
            vfolders.delete()
            .where(vfolders.c.user == user_uuid)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} user\'s virtual folders ({1})', result.rowcount, user_uuid)
        return result.rowcount

    @classmethod
    async def delete_kernels(
        cls,
        conn: SAConnection,
        user_uuid: uuid.UUID,
    ) -> int:
        """
        Delete user's all kernels.

        :param conn: DB connection
        :param user_uuid: user's UUID to delete kernels
        :return: number of deleted rows
        """
        from . import kernels
        query = (
            kernels.delete()
            .where(kernels.c.user_uuid == user_uuid)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} user\'s kernels ({1})', result.rowcount, user_uuid)
        return result.rowcount

    @classmethod
    async def delete_keypairs(
        cls,
        conn: SAConnection,
        user_uuid: uuid.UUID,
    ) -> int:
        """
        Delete user's all keypairs.

        :param conn: DB connection
        :param user_uuid: user's UUID to delete keypairs
        :return: number of deleted rows
        """
        from . import keypairs
        query = (
            keypairs.delete()
            .where(keypairs.c.user == user_uuid)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} user\'s keypairs ({1})', result.rowcount, user_uuid)
        return result.rowcount


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
