import enum
import sqlalchemy as sa
from .base import metadata, GUID, IDColumn

__all__ = ('vfolders', )


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
    sa.Column('vfolder', GUID, sa.ForeignKey('vfolders.id'), nullable=False),
    sa.Column('kernel', GUID, sa.ForeignKey('kernels.sess_id'), nullable=False),
    sa.PrimaryKeyConstraint('vfolder', 'kernel'),
)
