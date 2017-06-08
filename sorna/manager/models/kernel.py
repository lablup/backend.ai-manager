import sqlalchemy as sa
from .base import metadata, IDColumn

__all__ = ('kernels', )


kernels = sa.Table(
    'kernels', metadata,
    IDColumn('sess_id'),
    sa.Column('lang', sa.String(length=64)),
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key')),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('terminated_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('status', sa.String()),
    sa.Column('agent_id', sa.String()),
    sa.Column('container_id', sa.String()),
    #sa.Column('attached_folders', sa.String()),
    #sa.Column('allocated_cores', sa.String()),
)
