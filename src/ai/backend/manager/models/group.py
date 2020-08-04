import asyncio
from collections import OrderedDict
import logging
from pathlib import Path
import re
from typing import (
    Optional, Union,
    Sequence,
)
import uuid
import shutil

from aiopg.sa.connection import SAConnection
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from ai.backend.common.utils import current_loop
from .base import (
    metadata, GUID, IDColumn, ResourceSlotColumn,
    privileged_mutation,
    set_if_set,
    simple_db_mutate,
    simple_db_mutate_returning_item,
)
from .user import UserRole

log = BraceStyleAdapter(logging.getLogger(__file__))


__all__: Sequence[str] = (
    'groups', 'association_groups_users',
    'resolve_group_name_or_id',
    'Group', 'GroupInput', 'ModifyGroupInput',
    'CreateGroup', 'ModifyGroup', 'DeleteGroup',
)

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')

association_groups_users = sa.Table(
    'association_groups_users', metadata,
    sa.Column('user_id', GUID,
              sa.ForeignKey('users.uuid', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.Column('group_id', GUID,
              sa.ForeignKey('groups.id', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.UniqueConstraint('user_id', 'group_id', name='uq_association_user_id_group_id')
)


groups = sa.Table(
    'groups', metadata,
    IDColumn('id'),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),
    #: Field for synchronization with external services.
    sa.Column('integration_id', sa.String(length=512)),

    sa.Column('domain_name', sa.String(length=64),
              sa.ForeignKey('domains.name', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False, index=True),
    # TODO: separate resource-related fields with new domain resource policy table when needed.
    sa.Column('total_resource_slots', ResourceSlotColumn(), default='{}'),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False, default='{}'),
    sa.UniqueConstraint('name', 'domain_name', name='uq_groups_name_domain_name')
)


async def resolve_group_name_or_id(db_conn: SAConnection,
                                   domain_name: str,
                                   value: Union[str, uuid.UUID]) -> Optional[uuid.UUID]:
    if isinstance(value, str):
        query = (
            sa.select([groups.c.id])
            .select_from(groups)
            .where(
                (groups.c.name == value) &
                (groups.c.domain_name == domain_name)
            )
        )
        return await db_conn.scalar(query)
    elif isinstance(value, uuid.UUID):
        query = (
            sa.select([groups.c.id])
            .select_from(groups)
            .where(
                (groups.c.id == value) &
                (groups.c.domain_name == domain_name)
            )
        )
        return await db_conn.scalar(query)
    else:
        raise TypeError('unexpected type for group_name_or_id')


class Group(graphene.ObjectType):
    id = graphene.UUID()
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    domain_name = graphene.String()
    total_resource_slots = graphene.JSONString()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)
    integration_id = graphene.String()

    scaling_groups = graphene.List(lambda: graphene.String)

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            name=row['name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            modified_at=row['modified_at'],
            domain_name=row['domain_name'],
            total_resource_slots=row['total_resource_slots'].to_json(),
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
            integration_id=row['integration_id'],
        )

    async def resolve_scaling_groups(self, info):
        from .scaling_group import ScalingGroup
        sgroups = await ScalingGroup.load_by_group(info.context, self.id)
        return [sg.name for sg in sgroups]

    @staticmethod
    async def load_all(context, *,
                       domain_name=None,
                       is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([groups])
                .select_from(groups)
            )
            if domain_name is not None:
                query = query.where(groups.c.domain_name == domain_name)
            if is_active is not None:
                query = query.where(groups.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_id(context, ids, *,
                               domain_name=None):
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([groups])
                .select_from(groups)
                .where(groups.c.id.in_(ids))
            )
            if domain_name is not None:
                query = query.where(groups.c.domain_name == domain_name)
            objs_per_key = OrderedDict()
            for k in ids:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs_per_key[str(row.id)] = o
        return [*objs_per_key.values()]

    @staticmethod
    async def get_groups_for_user(context, user_id):
        async with context['dbpool'].acquire() as conn:
            j = sa.join(groups, association_groups_users,
                        groups.c.id == association_groups_users.c.group_id)
            query = (
                sa.select([groups])
                .select_from(j)
                .where(association_groups_users.c.user_id == user_id)
            )
            objs = []
            async for row in conn.execute(query):
                o = Group.from_row(row)
                objs.append(o)
            return objs


class GroupInput(graphene.InputObjectType):
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    domain_name = graphene.String(required=True)
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class ModifyGroupInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    domain_name = graphene.String(required=False)
    total_resource_slots = graphene.JSONString(required=False)
    user_update_mode = graphene.String(required=False)
    user_uuids = graphene.List(lambda: graphene.String, required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class CreateGroup(graphene.Mutation):

    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        name = graphene.String(required=True)
        props = GroupInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    group = graphene.Field(lambda: Group, required=False)

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda name, props, **kwargs: (props.domain_name, None)
    )
    async def mutate(cls, root, info, name, props):
        if _rx_slug.search(name) is None:
            raise ValueError('invalid name format. slug format required.')
        data = {
            'name': name,
            'description': props.description,
            'is_active': props.is_active,
            'domain_name': props.domain_name,
            'total_resource_slots': ResourceSlot.from_user_input(
                props.total_resource_slots, None),
            'allowed_vfolder_hosts': props.allowed_vfolder_hosts,
            'integration_id': props.integration_id,
        }
        insert_query = groups.insert().values(data)
        item_query = (
            groups.select()
            .where((groups.c.name == name) &
                   (groups.c.domain_name == props.domain_name))
        )
        return await simple_db_mutate_returning_item(
            cls, info.context, insert_query,
            item_query=item_query, item_cls=Group)


class ModifyGroup(graphene.Mutation):

    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        gid = graphene.String(required=True)
        props = ModifyGroupInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    group = graphene.Field(lambda: Group, required=False)

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda gid, **kwargs: (None, gid)
    )
    async def mutate(cls, root, info, gid, props):
        data = {}
        set_if_set(props, data, 'name')
        set_if_set(props, data, 'description')
        set_if_set(props, data, 'is_active')
        set_if_set(props, data, 'domain_name')
        set_if_set(props, data, 'total_resource_slots',
                   clean_func=lambda v: ResourceSlot.from_user_input(v, None))
        set_if_set(props, data, 'allowed_vfolder_hosts')
        set_if_set(props, data, 'integration_id')

        if 'name' in data and _rx_slug.search(data['name']) is None:
            raise ValueError('invalid name format. slug format required.')
        if props.user_update_mode not in (None, 'add', 'remove'):
            raise ValueError('invalid user_update_mode')
        if not props.user_uuids:
            props.user_update_mode = None
        if not data and props.user_update_mode is None:
            return cls(ok=False, msg='nothing to update', group=None)
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            try:
                if props.user_update_mode == 'add':
                    values = [{'user_id': uuid, 'group_id': gid} for uuid in props.user_uuids]
                    query = sa.insert(association_groups_users).values(values)
                    await conn.execute(query)
                elif props.user_update_mode == 'remove':
                    query = (association_groups_users
                             .delete()
                             .where(association_groups_users.c.user_id.in_(props.user_uuids))
                             .where(association_groups_users.c.group_id == gid))
                    await conn.execute(query)

                if data:
                    query = (groups.update().values(data).where(groups.c.id == gid))
                    result = await conn.execute(query)
                    if result.rowcount > 0:
                        checkq = groups.select().where(groups.c.id == gid)
                        result = await conn.execute(checkq)
                        o = Group.from_row(await result.first())
                        return cls(ok=True, msg='success', group=o)
                    return cls(ok=False, msg='no such group', group=None)
                else:  # updated association_groups_users table
                    return cls(ok=True, msg='success', group=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}', group=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False, msg=f'unexpected error: {e}', group=None)


class DeleteGroup(graphene.Mutation):
    """
    Instead of deleting the group, just mark it as inactive.
    """
    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        gid = graphene.UUID(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda gid, **kwargs: (None, gid)
    )
    async def mutate(cls, root, info, gid):
        query = groups.update().values(
            is_active=False,
            integration_id=None
        ).where(groups.c.id == gid)
        return simple_db_mutate(cls, info.context, query)


class PurgeGroup(graphene.Mutation):
    """
    Completely deletes a group from DB.

    Group's vfolders and their data will also be lost
    as well as the kernels run from the group.
    There is no migration of the ownership for group folders.
    """
    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        gid = graphene.UUID(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(
        UserRole.ADMIN,
        lambda gid, **kwargs: (None, gid)
    )
    async def mutate(cls, root, info, gid):
        async with info.context['dbpool'].acquire() as conn:
            await cls.delete_vfolders(conn, gid, info.context['config_server'])
        await cls.delete_kernels(conn, gid)
        query = groups.delete().where(groups.c.id == gid)
        return simple_db_mutate(cls, info.context, query)

    @classmethod
    async def delete_vfolders(
        cls,
        conn: SAConnection,
        group_id: uuid.UUID,
        config_server,
    ) -> int:
        """
        Delete group's all virtual folders as well as their physical data.

        :param conn: DB connection
        :param group_id: group's UUID to delete virtual folders

        :return: number of deleted rows
        """
        from . import vfolders
        mount_prefix = Path(await config_server.get('volumes/_mount'))
        fs_prefix = await config_server.get('volumes/_fsprefix')
        fs_prefix = Path(fs_prefix.lstrip('/'))
        query = (
            sa.select([vfolders.c.id, vfolders.c.host])
            .select_from(vfolders)
            .where(vfolders.c.group == group_id)
        )
        async for row in conn.execute(query):
            folder_path = (mount_prefix / row['host'] / fs_prefix / row['id'].hex)
            try:
                loop = current_loop()
                await loop.run_in_executor(None, lambda: shutil.rmtree(folder_path))  # type: ignore
            except IOError:
                pass
        query = (
            vfolders.delete()
            .where(vfolders.c.group == group_id)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} group\'s virtual folders ({1})', result.rowcount, group_id)
        return result.rowcount

    @classmethod
    async def delete_kernels(
        cls,
        conn: SAConnection,
        group_id: uuid.UUID,
    ) -> int:
        """
        Delete all kernels run from the target groups.

        :param conn: DB connection
        :param group_id: group's UUID to delete kernels

        :return: number of deleted rows
        """
        from . import kernels
        query = (
            kernels.delete()
            .where(kernels.c.group_id == group_id)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} group\'s kernels ({1})', result.rowcount, group_id)
        return result.rowcount
