from collections import OrderedDict

import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa

from .base import metadata, GUID, IDColumn

__all__ = (
    'vfolders',
    'VirtualFolder',
)


vfolders = sa.Table(
    'vfolders', metadata,
    IDColumn('id'),
    sa.Column('host', sa.String(length=128), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('max_files', sa.Integer(), default=512),
    sa.Column('max_size', sa.Integer(), default=1024),  # in KBytes
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
            id=row.id,
            host=row.host,
            name=row.name,
            max_files=row.max_files,
            max_size=row.max_size,    # in KiB
            created_at=row.created_at,
            last_used=row.last_used,
            num_attached=row.num_attached,
        )

    async def resolve_num_files(self, info):
        # TODO: measure on-the-fly
        return 0

    async def resolve_cur_size(self, info):
        # TODO: measure on-the-fly
        return 0

    @staticmethod
    async def batch_load(dbpool, access_keys):
        async with dbpool.acquire() as conn:
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
