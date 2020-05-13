from collections import OrderedDict
import enum
from typing import Any, Sequence

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
import trafaret as t

from ai.backend.gateway.config import RESERVED_VFOLDER_PATTERNS, RESERVED_VFOLDERS
from .base import (
    metadata, EnumValueType, GUID, IDColumn,
    Item, PaginatedList,
)
from .user import UserRole

__all__: Sequence[str] = (
    'vfolders',
    'vfolder_invitations',
    'vfolder_permissions',
    'VirtualFolder',
    'VFolderInvitationState',
    'VFolderPermission',
    'VFolderPermissionValidator',
    'query_accessible_vfolders',
    'get_allowed_vfolder_hosts_by_group',
    'get_allowed_vfolder_hosts_by_user',
    'verify_vfolder_name'
)


class VFolderPermission(str, enum.Enum):
    '''
    Permissions for a virtual folder given to a specific access key.
    RW_DELETE includes READ_WRITE and READ_WRITE includes READ_ONLY.
    '''
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    RW_DELETE = 'wd'


class VFolderPermissionValidator(t.Trafaret):
    def check_and_return(self, value: Any) -> str:
        if value not in ['ro', 'rw', 'wd']:
            self._failure('one of "ro", "rw", or "wd" required', value=value)
        return value


class VFolderInvitationState(str, enum.Enum):
    '''
    Virtual Folder invitation state.
    '''
    PENDING = 'pending'
    CANCELED = 'canceled'  # canceled by inviter
    ACCEPTED = 'accepted'
    REJECTED = 'rejected'  # rejected by invitee


'''
This is the default permission when the vfolder is owned by the requesting user.
(In such cases there is no explicit vfolder_permissions table entry!)
It is added after creation of the VFolderPermission class to avoid becoming
one of the regular enumeration entries.
'''
setattr(VFolderPermission, 'OWNER_PERM', VFolderPermission.RW_DELETE)


vfolders = sa.Table(
    'vfolders', metadata,
    IDColumn('id'),
    sa.Column('host', sa.String(length=128), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('max_files', sa.Integer(), default=1000),
    sa.Column('max_size', sa.Integer(), default=1048576),  # in KBytes
    sa.Column('num_files', sa.Integer(), default=0),
    sa.Column('cur_size', sa.Integer(), default=0),  # in KBytes
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    # To store creator information (email) for group vfolder.
    sa.Column('creator', sa.String(length=128), nullable=True),

    sa.Column('user', GUID, sa.ForeignKey('users.uuid'), nullable=True),
    sa.Column('group', GUID, sa.ForeignKey('groups.id'), nullable=True),
)


vfolder_attachment = sa.Table(
    'vfolder_attachment', metadata,
    sa.Column('vfolder', GUID,
              sa.ForeignKey('vfolders.id', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.Column('kernel', GUID,
              sa.ForeignKey('kernels.id', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
    sa.PrimaryKeyConstraint('vfolder', 'kernel'),
)


vfolder_invitations = sa.Table(
    'vfolder_invitations', metadata,
    IDColumn('id'),
    sa.Column('permission', EnumValueType(VFolderPermission),
              default=VFolderPermission.READ_WRITE),
    sa.Column('inviter', sa.String(length=256)),  # email
    sa.Column('invitee', sa.String(length=256), nullable=False),  # email
    sa.Column('state', EnumValueType(VFolderInvitationState),
              default=VFolderInvitationState.PENDING),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('vfolder', GUID,
              sa.ForeignKey('vfolders.id',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              nullable=False),
)


vfolder_permissions = sa.Table(
    'vfolder_permissions', metadata,
    sa.Column('permission', EnumValueType(VFolderPermission),
              default=VFolderPermission.READ_WRITE),
    sa.Column('vfolder', GUID,
              sa.ForeignKey('vfolders.id',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
              nullable=False),
    sa.Column('user', GUID, sa.ForeignKey('users.uuid'), nullable=False),
)


def verify_vfolder_name(folder: str) -> bool:
    if folder in RESERVED_VFOLDERS:
        return False
    for pattern in RESERVED_VFOLDER_PATTERNS:
        if pattern.match(folder):
            return False
    return True


async def query_accessible_vfolders(conn, user_uuid, *,
                                    user_role=None, domain_name=None,
                                    allowed_vfolder_types=None,
                                    extra_vf_conds=None,
                                    extra_vfperm_conds=None):
    from ai.backend.manager.models import groups, users, association_groups_users as agus
    if allowed_vfolder_types is None:
        allowed_vfolder_types = ['user']  # legacy default
    entries = []
    # User vfolders.
    if 'user' in allowed_vfolder_types:
        # Scan my owned vfolders.
        j = (vfolders.join(users, vfolders.c.user == users.c.uuid))
        query = (sa.select([
                       vfolders.c.name,
                       vfolders.c.id,
                       vfolders.c.host,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       users.c.email,
                   ])
                   .select_from(j)
                   .where(vfolders.c.user == user_uuid))
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        result = await conn.execute(query)
        async for row in result:
            entries.append({
                'name': row.name,
                'id': row.id,
                'host': row.host,
                'created_at': row.created_at,
                'last_used': row.last_used,
                'max_size': row.max_size,
                'max_files': row.max_files,
                'user': str(row.user) if row.user else None,
                'group': str(row.group) if row.group else None,
                'creator': row.creator,
                'user_email': row.email,
                'group_name': None,
                'is_owner': True,
                'permission': VFolderPermission.OWNER_PERM,
            })
        # Scan vfolders shared with me.
        j = (
            vfolders.join(
                vfolder_permissions,
                vfolders.c.id == vfolder_permissions.c.vfolder,
                isouter=True)
            .join(
                users,
                vfolders.c.user == users.c.uuid,
                isouter=True)
        )
        query = (sa.select([
                       vfolders.c.name,
                       vfolders.c.id,
                       vfolders.c.host,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       vfolder_permissions.c.permission,
                       users.c.email,
                   ])
                   .select_from(j)
                   .where(vfolder_permissions.c.user == user_uuid))
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        if extra_vfperm_conds is not None:
            query = query.where(extra_vfperm_conds)
        result = await conn.execute(query)
        async for row in result:
            entries.append({
                'name': row.name,
                'id': row.id,
                'host': row.host,
                'created_at': row.created_at,
                'last_used': row.last_used,
                'max_size': row.max_size,
                'max_files': row.max_files,
                'user': str(row.user) if row.user else None,
                'group': str(row.group) if row.group else None,
                'creator': row.creator,
                'user_email': row.email,
                'group_name': None,
                'is_owner': False,
                'permission': row.permission,
            })

    if 'group' in allowed_vfolder_types:
        # Scan group vfolders.
        if user_role == UserRole.ADMIN or user_role == 'admin':
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == domain_name))
            result = await conn.execute(query)
            grps = await result.fetchall()
            group_ids = [g.id for g in grps]
        else:
            j = sa.join(agus, users, agus.c.user_id == users.c.uuid)
            query = (sa.select([agus.c.group_id])
                       .select_from(j)
                       .where(agus.c.user_id == user_uuid))
            result = await conn.execute(query)
            grps = await result.fetchall()
            group_ids = [g.group_id for g in grps]
        j = (vfolders.join(groups, vfolders.c.group == groups.c.id))
        query = (sa.select([
                       vfolders.c.name,
                       vfolders.c.id,
                       vfolders.c.host,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       groups.c.name,
                   ], use_labels=True)
                   .select_from(j)
                   .where(vfolders.c.group.in_(group_ids)))
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        result = await conn.execute(query)
        is_owner = (user_role == UserRole.ADMIN or user_role == 'admin')
        perm = VFolderPermission.OWNER_PERM if is_owner else VFolderPermission.READ_WRITE
        async for row in result:
            entries.append({
                'name': row.vfolders_name,
                'id': row.vfolders_id,
                'host': row.vfolders_host,
                'created_at': row.vfolders_created_at,
                'last_used': row.vfolders_last_used,
                'max_size': row.vfolders_max_size,
                'max_files': row.vfolders_max_files,
                'user': str(row.vfolders_user) if row.vfolders_user else None,
                'group': str(row.vfolders_group) if row.vfolders_group else None,
                'creator': row.vfolders_creator,
                'user_email': None,
                'group_name': row.groups_name,
                'is_owner': is_owner,
                'permission': perm,
            })
    return entries


async def get_allowed_vfolder_hosts_by_group(conn, resource_policy,
                                             domain_name, group_id=None, domain_admin=False):
    '''
    Union `allowed_vfolder_hosts` from domain, group, and keypair_resource_policy.

    If `group_id` is not None, `allowed_vfolder_hosts` from the group is also merged.
    If the requester is a domain admin, gather all `allowed_vfolder_hosts` of the domain groups.
    '''
    from . import domains, groups
    # Domain's allowed_vfolder_hosts.
    allowed_hosts = set()
    query = (sa.select([domains.c.allowed_vfolder_hosts])
               .where((domains.c.name == domain_name) &
                       domains.c.is_active))
    allowed_hosts.update(await conn.scalar(query))
    # Group's allowed_vfolder_hosts.
    if group_id is not None:
        query = (sa.select([groups.c.allowed_vfolder_hosts])
                   .where(groups.c.domain_name == domain_name)
                   .where((groups.c.id == group_id) &
                          (groups.c.is_active)))
        allowed_hosts.update(await conn.scalar(query))
    elif domain_admin:
        query = (sa.select([groups.c.allowed_vfolder_hosts])
                   .where((groups.c.domain_name == domain_name) &
                          (groups.c.is_active)))
        async for row in conn.execute(query):
            allowed_hosts.update(row.allowed_vfolder_hosts)
    # Keypair Resource Policy's allowed_vfolder_hosts
    allowed_hosts.update(resource_policy['allowed_vfolder_hosts'])
    return allowed_hosts


async def get_allowed_vfolder_hosts_by_user(conn, resource_policy,
                                            domain_name, user_uuid):
    '''
    Union `allowed_vfolder_hosts` from domain, groups, and keypair_resource_policy.

    All available `allowed_vfolder_hosts` of groups which requester associated will be merged.
    '''
    from . import association_groups_users, domains, groups
    # Domain's allowed_vfolder_hosts.
    allowed_hosts = set()
    query = (sa.select([domains.c.allowed_vfolder_hosts])
               .where((domains.c.name == domain_name) &
                      (domains.c.is_active)))
    allowed_hosts.update(await conn.scalar(query))
    # User's Groups' allowed_vfolder_hosts.
    j = groups.join(association_groups_users,
                    ((groups.c.id == association_groups_users.c.group_id) &
                     (association_groups_users.c.user_id == user_uuid)))
    query = (sa.select([groups.c.allowed_vfolder_hosts])
               .select_from(j)
               .where((domains.c.name == domain_name) &
                      (groups.c.is_active)))
    result = await conn.execute(query)
    rows = await result.fetchall()
    for row in rows:
        allowed_hosts.update(row['allowed_vfolder_hosts'])
    # Keypair Resource Policy's allowed_vfolder_hosts
    allowed_hosts.update(resource_policy['allowed_vfolder_hosts'])
    return allowed_hosts


class VirtualFolder(graphene.ObjectType):
    class Meta:
        interfaces = (Item, )

    host = graphene.String()
    name = graphene.String()
    max_files = graphene.Int()
    max_size = graphene.Int()
    created_at = GQLDateTime()
    last_used = GQLDateTime()

    num_files = graphene.Int()
    cur_size = graphene.Int()
    # num_attached = graphene.Int()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            host=row['host'],
            name=row['name'],
            max_files=row['max_files'],
            max_size=row['max_size'],    # in KiB
            created_at=row['created_at'],
            last_used=row['last_used'],
            # num_attached=row['num_attached'],
        )

    async def resolve_num_files(self, info):
        # TODO: measure on-the-fly
        return 0

    async def resolve_cur_size(self, info):
        # TODO: measure on-the-fly
        return 0

    @staticmethod
    async def load_count(context, *,
                         domain_name=None, group_id=None, user_id=None):
        from .user import users
        async with context['dbpool'].acquire() as conn:
            j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
            query = (
                sa.select([sa.func.count(vfolders.c.id)])
                .select_from(j)
                .as_scalar()
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(vfolders.c.group == group_id)
            if user_id is not None:
                query = query.where(vfolders.c.user == user_id)
            result = await conn.execute(query)
            count = await result.fetchone()
            return count[0]

    @staticmethod
    async def load_slice(context, limit, offset, *,
                         domain_name=None, group_id=None, user_id=None,
                         order_key=None, order_asc=None):
        from .user import users
        async with context['dbpool'].acquire() as conn:
            if order_key is None:
                _ordering = vfolders.c.created_at
            else:
                _order_func = sa.asc if order_asc else sa.desc
                _ordering = _order_func(getattr(vfolders.c, order_key))
            j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
            query = (
                sa.select([vfolders])
                .select_from(j)
                .order_by(_ordering)
                .limit(limit)
                .offset(offset)
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(vfolders.c.group == group_id)
            if user_id is not None:
                query = query.where(vfolders.c.user == user_id)
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [VirtualFolder.from_row(context, r) for r in rows]

    @staticmethod
    async def load_all(context, *,
                       domain_name=None, group_id=None, user_id=None):
        from .user import users
        async with context['dbpool'].acquire() as conn:
            j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
            query = (
                sa.select([vfolders])
                .select_from(j)
                .order_by(sa.desc(vfolders.c.created_at))
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(vfolders.c.group == group_id)
            if user_id is not None:
                query = query.where(vfolders.c.user == user_id)
            objs = []
            async for row in conn.execute(query):
                o = VirtualFolder.from_row(row)
                objs.append(o)
        return objs

    @staticmethod
    async def batch_load_by_user(context, user_uuids, *,
                                 domain_name=None, group_id=None):
        from .user import users
        async with context['dbpool'].acquire() as conn:
            # TODO: num_attached count group-by
            j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
            query = (
                sa.select([vfolders])
                .select_from(j)
                .where(vfolders.c.user.in_(user_uuids))
                .order_by(sa.desc(vfolders.c.created_at))
            )
            if domain_name is not None:
                query = query.where(users.c.domain_name == domain_name)
            if group_id is not None:
                query = query.where(vfolders.c.group == group_id)
            objs_per_key = OrderedDict()
            for u in user_uuids:
                objs_per_key[u] = list()
            async for row in conn.execute(query):
                o = VirtualFolder.from_row(row)
                objs_per_key[row.user].append(o)
        return tuple(objs_per_key.values())


class VirtualFolderList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(VirtualFolder, required=True)
