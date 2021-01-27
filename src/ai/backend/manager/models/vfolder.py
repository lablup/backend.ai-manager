import enum
from typing import Any, Sequence

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
import trafaret as t

from ..defs import RESERVED_VFOLDER_PATTERNS, RESERVED_VFOLDERS
from .base import (
    metadata, EnumValueType, GUID, IDColumn,
    Item, PaginatedList, BigInt,
    batch_multiresult,
)
from .user import UserRole

__all__: Sequence[str] = (
    'vfolders',
    'vfolder_invitations',
    'vfolder_permissions',
    'VirtualFolder',
    'VFolderUsageMode',
    'VFolderOwnershipType',
    'VFolderInvitationState',
    'VFolderPermission',
    'VFolderPermissionValidator',
    'query_accessible_vfolders',
    'get_allowed_vfolder_hosts_by_group',
    'get_allowed_vfolder_hosts_by_user',
    'verify_vfolder_name'
)


class VFolderUsageMode(str, enum.Enum):
    '''
    Usage mode of virtual folder.

    GENERAL: normal virtual folder
    MODEL: virtual folder which provides shared models
    DATA: virtual folder which provides shared data
    '''
    GENERAL = 'general'
    MODEL = 'model'
    DATA = 'data'


class VFolderOwnershipType(str, enum.Enum):
    '''
    Ownership type of virtual folder.
    '''
    USER = 'user'
    GROUP = 'group'


class VFolderPermission(str, enum.Enum):
    '''
    Permissions for a virtual folder given to a specific access key.
    RW_DELETE includes READ_WRITE and READ_WRITE includes READ_ONLY.
    '''
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    RW_DELETE = 'wd'
    OWNER_PERM = 'wd'  # resolved as RW_DELETE


class VFolderPermissionValidator(t.Trafaret):
    def check_and_return(self, value: Any) -> VFolderPermission:
        if value not in ['ro', 'rw', 'wd']:
            self._failure('one of "ro", "rw", or "wd" required', value=value)
        return VFolderPermission(value)


class VFolderInvitationState(str, enum.Enum):
    '''
    Virtual Folder invitation state.
    '''
    PENDING = 'pending'
    CANCELED = 'canceled'  # canceled by inviter
    ACCEPTED = 'accepted'
    REJECTED = 'rejected'  # rejected by invitee


vfolders = sa.Table(
    'vfolders', metadata,
    IDColumn('id'),
    # host will be '' if vFolder is unmanaged
    sa.Column('host', sa.String(length=128), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False, index=True),
    sa.Column('usage_mode', EnumValueType(VFolderUsageMode),
              default=VFolderUsageMode.GENERAL, nullable=False),
    sa.Column('permission', EnumValueType(VFolderPermission),
              default=VFolderPermission.READ_WRITE),
    sa.Column('max_files', sa.Integer(), default=1000),
    sa.Column('max_size', sa.Integer(), default=1048576),  # in KBytes
    sa.Column('num_files', sa.Integer(), default=0),
    sa.Column('cur_size', sa.Integer(), default=0),  # in KBytes
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    # To store creator information (email) for group vfolder.
    sa.Column('creator', sa.String(length=128), nullable=True),
    # For unmanaged vFolder only.
    sa.Column('unmanaged_path', sa.String(length=512), nullable=True),
    sa.Column('ownership_type', EnumValueType(VFolderOwnershipType),
              default=VFolderOwnershipType.USER, nullable=False),
    sa.Column('user', GUID, sa.ForeignKey('users.uuid'), nullable=True),
    sa.Column('group', GUID, sa.ForeignKey('groups.id'), nullable=True),
    sa.Column('cloneable', sa.Boolean, default=False, nullable=False),

    sa.CheckConstraint(
        '(ownership_type = \'user\' AND "user" IS NOT NULL) OR '
        '(ownership_type = \'group\' AND "group" IS NOT NULL)',
        name='ownership_type_match_with_user_or_group',
    ),
    sa.CheckConstraint(
        '("user" IS NULL AND "group" IS NOT NULL) OR ("user" IS NOT NULL AND "group" IS NULL)',
        name='either_one_of_user_or_group',
    )
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
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True), nullable=True,
              onupdate=sa.func.current_timestamp()),
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
                                    extra_vfperm_conds=None,
                                    extra_vf_user_conds=None,
                                    extra_vf_group_conds=None):
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
                       vfolders.c.usage_mode,
                       vfolders.c.permission,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.ownership_type,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       vfolders.c.unmanaged_path,
                       vfolders.c.cloneable,
                       users.c.email,
                   ])
                   .select_from(j)
                   .where(vfolders.c.user == user_uuid))
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        if extra_vf_user_conds is not None:
            query = query.where(extra_vf_user_conds)
        result = await conn.execute(query)
        async for row in result:
            entries.append({
                'name': row.name,
                'id': row.id,
                'host': row.host,
                'usage_mode': row.usage_mode,
                'created_at': row.created_at,
                'last_used': row.last_used,
                'max_size': row.max_size,
                'max_files': row.max_files,
                'ownership_type': row.ownership_type,
                'user': str(row.user) if row.user else None,
                'group': str(row.group) if row.group else None,
                'creator': row.creator,
                'user_email': row.email,
                'group_name': None,
                'is_owner': True,
                'permission': row.permission,
                'unmanaged_path': row.get('unmanaged_path'),
                'cloneable': row.cloneable,
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
                       vfolders.c.usage_mode,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.ownership_type,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       vfolders.c.unmanaged_path,
                       # vfolders.c.permission,
                       vfolder_permissions.c.permission,  # override vfolder perm
                       vfolders.c.cloneable,
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
                'usage_mode': row.usage_mode,
                'created_at': row.created_at,
                'last_used': row.last_used,
                'max_size': row.max_size,
                'max_files': row.max_files,
                'ownership_type': row.ownership_type,
                'user': str(row.user) if row.user else None,
                'group': str(row.group) if row.group else None,
                'creator': row.creator,
                'user_email': row.email,
                'group_name': None,
                'is_owner': False,
                'permission': row.permission,  # not vfolders.c.permission!
                'unmanaged_path': row.get('unmanaged_path'),
                'cloneable': row.cloneable,
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
                       vfolders.c.usage_mode,
                       vfolders.c.created_at,
                       vfolders.c.last_used,
                       vfolders.c.max_files,
                       vfolders.c.max_size,
                       vfolders.c.ownership_type,
                       vfolders.c.user,
                       vfolders.c.group,
                       vfolders.c.creator,
                       vfolders.c.permission,
                       vfolders.c.unmanaged_path,
                       vfolders.c.cloneable,
                       groups.c.name,
                   ], use_labels=True)
                   .select_from(j))
        if user_role != UserRole.SUPERADMIN:
            query = query.where(vfolders.c.group.in_(group_ids))
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        if extra_vf_group_conds is not None:
            query = query.where(extra_vf_group_conds)
        result = await conn.execute(query)
        is_owner = ((user_role == UserRole.ADMIN or user_role == 'admin') or
                    (user_role == UserRole.SUPERADMIN or user_role == 'superadmin'))
        async for row in result:
            entries.append({
                'name': row.vfolders_name,
                'id': row.vfolders_id,
                'host': row.vfolders_host,
                'usage_mode': row.vfolders_usage_mode,
                'created_at': row.vfolders_created_at,
                'last_used': row.vfolders_last_used,
                'max_size': row.vfolders_max_size,
                'max_files': row.vfolders_max_files,
                'ownership_type': row.vfolders_ownership_type,
                'user': str(row.vfolders_user) if row.vfolders_user else None,
                'group': str(row.vfolders_group) if row.vfolders_group else None,
                'creator': row.vfolders_creator,
                'user_email': None,
                'group_name': row.groups_name,
                'is_owner': is_owner,
                'permission': row.vfolders_permission,
                'unmanaged_path': row.get('unmanaged_path'),
                'cloneable': row.vfolders_cloneable,
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
    user = graphene.UUID()       # User.id
    group = graphene.UUID()      # Group.id
    creator = graphene.String()  # User.email
    unmanaged_path = graphene.String()
    usage_mode = graphene.String()
    permission = graphene.String()
    ownership_type = graphene.String()
    max_files = graphene.Int()
    max_size = graphene.Int()
    created_at = GQLDateTime()
    last_used = GQLDateTime()

    num_files = graphene.Int()
    cur_size = BigInt()
    # num_attached = graphene.Int()
    cloneable = graphene.Boolean()

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            host=row['host'],
            name=row['name'],
            user=row['user'],
            group=row['group'],
            creator=row['creator'],
            unmanaged_path=row['unmanaged_path'],
            usage_mode=row['usage_mode'],
            permission=row['permission'],
            ownership_type=row['ownership_type'],
            max_files=row['max_files'],
            max_size=row['max_size'],    # in KiB
            created_at=row['created_at'],
            last_used=row['last_used'],
            # num_attached=row['num_attached'],
            cloneable=row['cloneable'],
        )

    async def resolve_num_files(self, info):
        # TODO: measure on-the-fly
        return 0

    async def resolve_cur_size(self, info):
        # TODO: measure on-the-fly
        return 0

    @classmethod
    async def load_count(cls, context, *,
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
            return await result.scalar()

    @classmethod
    async def load_slice(cls, context, limit, offset, *,
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
            return [cls.from_row(context, r) async for r in conn.execute(query)]

    @classmethod
    async def batch_load_by_user(cls, context, user_uuids, *,
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
            return await batch_multiresult(
                context, conn, query, cls,
                user_uuids, lambda row: row['user']
            )


class VirtualFolderList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(VirtualFolder, required=True)
