import sqlalchemy as sa
from .base import metadata, IDColumn

__all__ = ('usage', )


usage = sa.Table(
    'usage', metadata,
    IDColumn('id'),
    sa.Column('access_key_id', sa.ForeignKey('keypairs.access_key')),
    sa.Column('kernel_type', sa.String),
    sa.Column('kernel_id', sa.String),
    sa.Column('started_at', sa.DateTime(timezone=True)),
    sa.Column('terminated_at', sa.DateTime(timezone=True)),
    sa.Column('cpu_used', sa.Integer, server_default='0'),
    sa.Column('mem_used', sa.Integer, server_default='0'),
    sa.Column('io_used', sa.Integer, server_default='0'),
    sa.Column('net_used', sa.Integer, server_default='0'),
)

