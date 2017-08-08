import enum
import sqlalchemy as sa
from .base import metadata, IDColumn

__all__ = ('kernels', 'kernel_status')


class kernel_status(enum.Enum):
    preparing = 10
    # ---
    building = 20
    # ---
    running = 30
    restarting = 31
    resizing = 32
    suspended = 33
    # ---
    terminating = 40
    success = 41
    error = 42


kernels = sa.Table(
    'kernels', metadata,
    IDColumn('sess_id'),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id')),
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key')),
    sa.Column('lang', sa.String(length=64)),
    sa.Column('container_id', sa.String(length=64)),
    sa.Column('allocated_cores', sa.ARRAY(sa.Integer)),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), index=True),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
              nullable=True, index=True),
    sa.Column('status', sa.Enum(kernel_status),
              default=kernel_status.preparing, index=True),
    sa.Column('status_info', sa.Unicode(), nullable=True),

    # Live stats
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('cpu_used', sa.BigInteger(), default=0),       # msec
    sa.Column('max_mem_bytes', sa.BigInteger(), default=0),  # bytes
    sa.Column('cur_mem_bytes', sa.BigInteger(), default=0),  # bytes
    sa.Column('net_rx_bytes', sa.BigInteger(), default=0),
    sa.Column('net_tx_bytes', sa.BigInteger(), default=0),
    sa.Column('io_read_bytes', sa.BigInteger(), default=0),
    sa.Column('io_write_bytes', sa.BigInteger(), default=0),
)
