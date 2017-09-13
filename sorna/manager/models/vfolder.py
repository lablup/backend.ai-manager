from collections import OrderedDict
import enum

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
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('belongs_to', sa.String(length=20), sa.ForeignKey('keypairs.access_key'), nullable=False),
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
    num_files = graphene.Int()
    cur_size = graphene.Int()  # virtual value
    created_at = GQLDateTime()
    last_used = GQLDateTime()

    # num_attached = graphene.Int()

    @classmethod
    async def to_obj(cls, row):
        return cls(
            id=row.id,
            host=row.host,
            name=row.name,
            max_files=row.max_files,
            max_size=row.max_size,    # in KiB
            num_files=row.num_files,  # TODO: measure on-the-fly?
            cur_size=0,               # TODO: measure on-the-fly
            created_at=row.created_at,
            last_used=row.last_used,
            num_attached=row.num_attached,
        )

    @staticmethod
    async def batch_load(conn, access_keys):
        # TODO: num_attached count group-by
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where(vfolders.c.belongs_to.in_(access_keys)))
        objs_per_key = OrderedDict()
        for k in access_keys:
            objs_per_key[k] = list()
        async for row in conn.execute(query):
            o = await VirtualFolder.to_obj(row)
            objs_per_key[row.belongs_to].append(o)
        return tuple(objs_per_key.values())
