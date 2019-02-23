from collections import OrderedDict
import enum

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa

from .base import metadata, EnumValueType, GUID, IDColumn

__all__ = (
    'vfolders',
    'vfolder_invitations',
    'vfolder_permissions',
    'VirtualFolder',
    'VFolderPermission',
    'query_accessible_vfolders',
)


class VFolderPermission(str, enum.Enum):
    '''
    Permissions for a virtual folder given to a specific access key.
    RW_DELETE includes READ_WRITE and READ_WRITE includes READ_ONLY.
    '''
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    RW_DELETE = 'wd'


'''
This is the default permission when the vfolder is owned by the requesting user.
(In such cases there is no explicit vfolder_permissions table entry!)
It is added after creation of the VFolderPermission class to avoid becoming
one of the regular enumeration entries.
'''
VFolderPermission.OWNER_PERM = VFolderPermission.RW_DELETE


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
    sa.Column('belongs_to', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key'), nullable=False),
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
    sa.Column('inviter', sa.String(length=256)),
    sa.Column('invitee', sa.String(length=256), nullable=False),
    # State of the infitation: pending, accepted, rejected
    sa.Column('state', sa.String(length=10), default='pending'),
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
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key'),
              nullable=False)
)


async def query_accessible_vfolders(conn, access_key, *,
                                    extra_vf_conds=None,
                                    extra_vfperm_conds=None):
    entries = []
    # Scan my owned vfolders.
    query = (sa.select([
                   vfolders.c.name,
                   vfolders.c.id,
                   vfolders.c.host,
                   vfolders.c.created_at,
                   vfolders.c.last_used,
                   vfolders.c.max_files,
                   vfolders.c.max_size,
               ])
               .select_from(vfolders)
               .where(vfolders.c.belongs_to == access_key))
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
            'is_owner': True,
            'permission': VFolderPermission.OWNER_PERM,
        })
    # Scan vfolders shared with me.
    j = sa.join(vfolders, vfolder_permissions,
                vfolders.c.id == vfolder_permissions.c.vfolder)
    query = (sa.select([
                   vfolders.c.name,
                   vfolders.c.id,
                   vfolders.c.host,
                   vfolders.c.created_at,
                   vfolders.c.last_used,
                   vfolders.c.max_files,
                   vfolders.c.max_size,
                   vfolder_permissions.c.permission,
               ])
               .select_from(j)
               .where(vfolder_permissions.c.access_key == access_key))
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
            'is_owner': False,
            'permission': row.permission,
        })
    return entries


class VirtualFolder(graphene.ObjectType):
    id = graphene.UUID()
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
    async def batch_load(context, access_keys):
        async with context['dbpool'].acquire() as conn:
            # TODO: num_attached count group-by
            query = (sa.select('*')
                       .select_from(vfolders)
                       .where(vfolders.c.belongs_to.in_(access_keys))
                       .order_by(sa.desc(vfolders.c.created_at)))
            objs_per_key = OrderedDict()
            for k in access_keys:
                objs_per_key[k] = list()
            async for row in conn.execute(query):
                o = VirtualFolder.from_row(row)
                objs_per_key[row.belongs_to].append(o)
        return tuple(objs_per_key.values())
