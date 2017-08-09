import enum
import sqlalchemy as sa
from .base import metadata

__all__ = ('kernels', 'KernelStatus')


class KernelStatus(enum.Enum):
    PREPARING = 10
    # ---
    BUILDING = 20
    # ---
    RUNNING = 30
    RESTARTING = 31
    RESIZING = 32
    SUSPENDED = 33
    # ---
    TERMINATING = 40
    SUCCESS = 41
    ERROR = 42


kernels = sa.Table(
    'kernels', metadata,
    sa.Column('sess_id', sa.String(length=64), primary_key=True),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id')),
    sa.Column('agent_addr', sa.String(length=128), nullable=False),
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
    sa.Column('status', sa.Enum(KernelStatus),
              default=KernelStatus.PREPARING, index=True),
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
