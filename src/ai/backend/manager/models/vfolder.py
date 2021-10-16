from __future__ import annotations

import enum
from typing import (
    Any,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    TYPE_CHECKING,
)
import uuid

from dateutil.parser import parse as dtparse
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection
import trafaret as t

from ..defs import RESERVED_VFOLDER_PATTERNS, RESERVED_VFOLDERS
from .base import (
    metadata, EnumValueType, GUID, IDColumn,
    Item, PaginatedList, BigInt,
    batch_multiresult,
)
from .minilang.queryfilter import QueryFilterParser
from .minilang.ordering import QueryOrderParser
from .user import UserRole
if TYPE_CHECKING:
    from .gql import GraphQueryContext

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


async def query_accessible_vfolders(
    conn: SAConnection,
    user_uuid: uuid.UUID,
    *,
    user_role=None,
    domain_name=None,
    allowed_vfolder_types=None,
    extra_vf_conds=None,
    extra_vfperm_conds=None,
    extra_vf_user_conds=None,
    extra_vf_group_conds=None,
) -> Sequence[Mapping[str, Any]]:
    from ai.backend.manager.models import groups, users, association_groups_users as agus
    if allowed_vfolder_types is None:
        allowed_vfolder_types = ['user']  # legacy default

    vfolders_selectors = [
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
        vfolders.c.cloneable,
        # vfolders.c.permission,
        # users.c.email,
    ]

    async def _append_entries(_query, _is_owner=True):
        if extra_vf_conds is not None:
            _query = _query.where(extra_vf_conds)
        if extra_vf_user_conds is not None:
            _query = _query.where(extra_vf_user_conds)
        result = await conn.execute(_query)
        for row in result:
            row_keys = row.keys()
            _perm = row.vfolder_permissions_permission \
                if 'vfolder_permissions_permission' in row_keys \
                else row.vfolders_permission
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
                'user_email': row.users_email if 'users_email' in row_keys else None,
                'group_name': row.groups_name if 'groups_name' in row_keys else None,
                'is_owner': _is_owner,
                'permission': _perm,
                'unmanaged_path': row.vfolders_unmanaged_path,
                'cloneable': row.vfolders_cloneable,
            })

    entries: List[dict] = []
    # User vfolders.
    if 'user' in allowed_vfolder_types:
        # Scan my owned vfolders.
        j = (vfolders.join(users, vfolders.c.user == users.c.uuid))
        query = (
            sa.select(vfolders_selectors + [vfolders.c.permission, users.c.email], use_labels=True)
            .select_from(j)
            .where(vfolders.c.user == user_uuid)
        )
        await _append_entries(query)

        # Scan vfolders shared with me.
        j = (
            vfolders.join(
                vfolder_permissions,
                vfolders.c.id == vfolder_permissions.c.vfolder,
                isouter=True,
            )
            .join(
                users,
                vfolders.c.user == users.c.uuid,
                isouter=True,
            )
        )
        query = (
            sa.select(
                vfolders_selectors + [vfolder_permissions.c.permission, users.c.email],
                use_labels=True
            )
            .select_from(j)
            .where(
                (vfolder_permissions.c.user == user_uuid) &
                (vfolders.c.ownership_type == VFolderOwnershipType.USER)
            )
        )
        await _append_entries(query, _is_owner=False)

    if 'group' in allowed_vfolder_types:
        # Scan group vfolders.
        if user_role == UserRole.ADMIN or user_role == 'admin':
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == domain_name))
            result = await conn.execute(query)
            grps = result.fetchall()
            group_ids = [g.id for g in grps]
        else:
            j = sa.join(agus, users, agus.c.user_id == users.c.uuid)
            query = (sa.select([agus.c.group_id])
                       .select_from(j)
                       .where(agus.c.user_id == user_uuid))
            result = await conn.execute(query)
            grps = result.fetchall()
            group_ids = [g.group_id for g in grps]
        j = (vfolders.join(groups, vfolders.c.group == groups.c.id))
        query = (
            sa.select(vfolders_selectors + [vfolders.c.permission, groups.c.name], use_labels=True)
            .select_from(j)
        )
        if user_role != UserRole.SUPERADMIN:
            query = query.where(vfolders.c.group.in_(group_ids))
        is_owner = ((user_role == UserRole.ADMIN or user_role == 'admin') or
                    (user_role == UserRole.SUPERADMIN or user_role == 'superadmin'))
        await _append_entries(query, is_owner)

        # Override permissions, if exists, for group vfolders.
        j = sa.join(
            vfolders, vfolder_permissions, vfolders.c.id == vfolder_permissions.c.vfolder,
        )
        query = (
            sa.select(vfolder_permissions.c.permission, vfolder_permissions.c.vfolder)
            .select_from(j)
            .where(
                (vfolders.c.group.in_(group_ids)) &
                (vfolder_permissions.c.user == user_uuid)
            )
        )
        if extra_vf_conds is not None:
            query = query.where(extra_vf_conds)
        if extra_vf_user_conds is not None:
            query = query.where(extra_vf_user_conds)
        result = await conn.execute(query)
        overriding_permissions: dict = {row.vfolder: row.permission for row in result}
        for entry in entries:
            if entry['id'] in overriding_permissions and \
                    entry['ownership_type'] == VFolderOwnershipType.GROUP:
                entry['permission'] = overriding_permissions[entry['id']]

    return entries


async def get_allowed_vfolder_hosts_by_group(
    conn: SAConnection,
    resource_policy,
    domain_name: str,
    group_id: uuid.UUID = None,
    domain_admin: bool = False,
) -> Set[str]:
    '''
    Union `allowed_vfolder_hosts` from domain, group, and keypair_resource_policy.

    If `group_id` is not None, `allowed_vfolder_hosts` from the group is also merged.
    If the requester is a domain admin, gather all `allowed_vfolder_hosts` of the domain groups.
    '''
    from . import domains, groups
    # Domain's allowed_vfolder_hosts.
    allowed_hosts = set()
    query = (
        sa.select([domains.c.allowed_vfolder_hosts])
        .where(
            (domains.c.name == domain_name) &
            (domains.c.is_active)
        )
    )
    allowed_hosts.update(await conn.scalar(query))
    # Group's allowed_vfolder_hosts.
    if group_id is not None:
        query = (
            sa.select([groups.c.allowed_vfolder_hosts])
            .where(
                (groups.c.domain_name == domain_name) &
                (groups.c.id == group_id) &
                (groups.c.is_active)
            )
        )
        allowed_hosts.update(await conn.scalar(query))
    elif domain_admin:
        query = (
            sa.select([groups.c.allowed_vfolder_hosts])
            .where(
                (groups.c.domain_name == domain_name) &
                (groups.c.is_active)
            )
        )
        result = await conn.execute(query)
        for row in result:
            allowed_hosts.update(row.allowed_vfolder_hosts)
    # Keypair Resource Policy's allowed_vfolder_hosts
    allowed_hosts.update(resource_policy['allowed_vfolder_hosts'])
    return allowed_hosts


async def get_allowed_vfolder_hosts_by_user(
    conn: SAConnection,
    resource_policy,
    domain_name: str,
    user_uuid: uuid.UUID,
) -> Set[str]:
    '''
    Union `allowed_vfolder_hosts` from domain, groups, and keypair_resource_policy.

    All available `allowed_vfolder_hosts` of groups which requester associated will be merged.
    '''
    from . import association_groups_users, domains, groups
    # Domain's allowed_vfolder_hosts.
    allowed_hosts = set()
    query = (
        sa.select([domains.c.allowed_vfolder_hosts])
        .where(
            (domains.c.name == domain_name) &
            (domains.c.is_active)
        )
    )
    allowed_hosts.update(await conn.scalar(query))
    # User's Groups' allowed_vfolder_hosts.
    j = groups.join(
        association_groups_users,
        (
            (groups.c.id == association_groups_users.c.group_id) &
            (association_groups_users.c.user_id == user_uuid)
        )
    )
    query = (
        sa.select([groups.c.allowed_vfolder_hosts])
        .select_from(j)
        .where(
            (domains.c.name == domain_name) &
            (groups.c.is_active)
        )
    )
    result = await conn.execute(query)
    rows = result.fetchall()
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
    def from_row(cls, ctx: GraphQueryContext, row: Row) -> Optional[VirtualFolder]:
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

    async def resolve_num_files(self, info: graphene.ResolveInfo) -> int:
        # TODO: measure on-the-fly
        return 0

    async def resolve_cur_size(self, info: graphene.ResolveInfo) -> int:
        # TODO: measure on-the-fly
        return 0

    _queryfilter_fieldspec = {
        "id": ("vfolders_id", uuid.UUID),
        "host": ("vfolders_host", None),
        "name": ("vfolders_name", None),
        "group": ("vfolders_group", uuid.UUID),
        "user": ("vfolders_user", uuid.UUID),
        "user_email": ("users_email", None),
        "creator": ("vfolders_creator", None),
        "unmanaged_path": ("vfolders_unmanaged_path", None),
        "usage_mode": ("vfolders_usage_mode", lambda s: VFolderUsageMode[s]),
        "permission": ("vfolders_permission", lambda s: VFolderPermission[s]),
        "ownership_type": ("vfolders_ownership_type", lambda s: VFolderOwnershipType[s]),
        "max_files": ("vfolders_max_files", None),
        "max_size": ("vfolders_max_size", None),
        "created_at": ("vfolders_created_at", dtparse),
        "last_used": ("vfolders_last_used", dtparse),
        "cloneable": ("vfolders_cloneable", None),
    }

    _queryorder_colmap = {
        "id": "vfolders_id",
        "host": "vfolders_host",
        "name": "vfolders_name",
        "group": "vfolders_group",
        "user": "vfolders_user",
        "user_email": "users_email",
        "usage_mode": "vfolders_usage_mode",
        "permission": "vfolders_permission",
        "ownership_type": "vfolders_ownership_type",
        "max_files": "vfolders_max_files",
        "max_size": "vfolders_max_size",
        "created_at": "vfolders_created_at",
        "last_used": "vfolders_last_used",
        "cloneable": "vfolders_cloneable",
    }

    @classmethod
    async def load_count(
        cls,
        graph_ctx: GraphQueryContext,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        user_id: uuid.UUID = None,
        filter: str = None,
    ) -> int:
        from .user import users
        j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
        query = (
            sa.select([sa.func.count(vfolders.c.id)])
            .select_from(j)
        )
        if domain_name is not None:
            query = query.where(users.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(vfolders.c.group == group_id)
        if user_id is not None:
            query = query.where(vfolders.c.user == user_id)
        if filter is not None:
            qfparser = QueryFilterParser(cls._queryfilter_fieldspec)
            query = qfparser.append_filter(query, filter)
        async with graph_ctx.db.begin_readonly() as conn:
            result = await conn.execute(query)
            return result.scalar()

    @classmethod
    async def load_slice(
        cls,
        graph_ctx: GraphQueryContext,
        limit: int,
        offset: int,
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
        user_id: uuid.UUID = None,
        filter: str = None,
        order: str = None,
    ) -> Sequence[VirtualFolder]:
        from .user import users
        j = sa.join(vfolders, users, vfolders.c.user == users.c.uuid)
        query = (
            sa.select([vfolders])
            .select_from(j)
            .limit(limit)
            .offset(offset)
        )
        if domain_name is not None:
            query = query.where(users.c.domain_name == domain_name)
        if group_id is not None:
            query = query.where(vfolders.c.group == group_id)
        if user_id is not None:
            query = query.where(vfolders.c.user == user_id)
        if filter is not None:
            qfparser = QueryFilterParser(cls._queryfilter_fieldspec)
            query = qfparser.append_filter(query, filter)
        if order is not None:
            qoparser = QueryOrderParser(cls._queryorder_colmap)
            query = qoparser.append_ordering(query, order)
        else:
            query = query.order_by(vfolders.c.created_at.desc())
        async with graph_ctx.db.begin_readonly() as conn:
            return [
                obj async for r in (await conn.stream(query))
                if (obj := cls.from_row(graph_ctx, r)) is not None
            ]

    @classmethod
    async def batch_load_by_user(
        cls,
        graph_ctx: GraphQueryContext,
        user_uuids: Sequence[uuid.UUID],
        *,
        domain_name: str = None,
        group_id: uuid.UUID = None,
    ) -> Sequence[Sequence[VirtualFolder]]:
        from .user import users
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
        async with graph_ctx.db.begin_readonly() as conn:
            return await batch_multiresult(
                graph_ctx, conn, query, cls,
                user_uuids, lambda row: row['user']
            )


class VirtualFolderList(graphene.ObjectType):
    class Meta:
        interfaces = (PaginatedList, )

    items = graphene.List(VirtualFolder, required=True)
