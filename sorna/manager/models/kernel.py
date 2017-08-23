import enum
import sqlalchemy as sa
from .base import metadata, EnumType, IDColumn

__all__ = ('kernels', 'KernelStatus')


class KernelStatus(enum.Enum):
    # values are only meaningful inside the gateway
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
    TERMINATED = 41
    ERROR = 42


kernels = sa.Table(
    'kernels', metadata,
    IDColumn(),
    sa.Column('sess_id', sa.String(length=64), unique=False, index=True),
    sa.Column('role', sa.String(length=16), nullable=False, default='master'),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id')),
    sa.Column('agent_addr', sa.String(length=128), nullable=False),
    sa.Column('access_key', sa.String(length=20),
              sa.ForeignKey('keypairs.access_key')),
    sa.Column('lang', sa.String(length=64)),

    # Resource occupation
    sa.Column('container_id', sa.String(length=64)),
    sa.Column('cpu_set', sa.ARRAY(sa.Integer)),
    sa.Column('gpu_set', sa.ARRAY(sa.Integer)),
    sa.Column('mem_slot', sa.Integer(), nullable=False),
    sa.Column('cpu_slot', sa.Integer(), nullable=False),
    sa.Column('gpu_slot', sa.Integer(), nullable=False),

    # Port mappings
    sa.Column('repl_in_port', sa.Integer(), nullable=False),
    sa.Column('repl_out_port', sa.Integer(), nullable=False),
    sa.Column('stdin_port', sa.Integer(), nullable=False),
    sa.Column('stdout_port', sa.Integer(), nullable=False),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), index=True),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null(), index=True),
    sa.Column('status', EnumType(KernelStatus),
              default=KernelStatus.PREPARING, index=True),
    sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null()),

    # Live stats
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('cpu_used', sa.BigInteger(), default=0),       # msec
    sa.Column('max_mem_bytes', sa.BigInteger(), default=0),  # bytes
    sa.Column('cur_mem_bytes', sa.BigInteger(), default=0),  # bytes
    sa.Column('net_rx_bytes', sa.BigInteger(), default=0),
    sa.Column('net_tx_bytes', sa.BigInteger(), default=0),
    sa.Column('io_read_bytes', sa.BigInteger(), default=0),
    sa.Column('io_write_bytes', sa.BigInteger(), default=0),

    sa.Index('ix_kernels_sess_id_role', 'sess_id', 'role', unique=False),
)
