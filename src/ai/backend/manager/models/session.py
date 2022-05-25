
import enum
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import (
    relationship,
    selectinload,
)

from ai.backend.common.types import (
    AccessKey,
    BinarySize,
    ClusterMode,
    KernelId,
    RedisConnectionInfo,
    SessionId,
    SessionTypes,
    SessionResult,
    VFolderMount,
)

from .base import (
    EnumType, GUID, ForeignKeyIDColumn, SessionIDColumn,
    IDColumn, ResourceSlotColumn, URLColumn, StructuredJSONObjectListColumn,
    KVPair, ResourceLimit, KVPairInput, ResourceLimitInput,
    Base, StructuredJSONColumn, set_if_set,
)

__all__ = (
    'SessionStatus',
    'SessionRow',
    'SessionDependencyRow',
)


class SessionStatus(enum.Enum):
    # values are only meaningful inside the manager
    PENDING = 0
    # ---
    SCHEDULED = 5
    PREPARING = 10
    # ---
    BUILDING = 20
    PULLING = 21
    # ---
    RUNNING = 30
    RESTARTING = 31
    RESIZING = 32
    SUSPENDED = 33
    # ---
    TERMINATING = 40
    TERMINATED = 41
    ERROR = 42
    CANCELLED = 43


class SessionRow(Base):
    __tablename__ = 'sessions'
    id = SessionIDColumn()
    creation_id = sa.Column('creation_id', sa.String(length=32), unique=False, index=False)
    name = sa.Column('name', sa.String(length=64), unique=False, index=True)
    session_type = sa.Column('session_type', EnumType(SessionTypes), index=True, nullable=False,  # previously sess_type
              default=SessionTypes.INTERACTIVE, server_default=SessionTypes.INTERACTIVE.name)

    cluster_mode = sa.Column('cluster_mode', sa.String(length=16), nullable=False,
              default=ClusterMode.SINGLE_NODE, server_default=ClusterMode.SINGLE_NODE.name)
    cluster_size = sa.Column('cluster_size', sa.Integer, nullable=False, default=1)
    kernels = relationship('KernelRow', backref='session')

    # Resource ownership
    scaling_group_name = sa.Column('scaling_group_name', sa.ForeignKey('scaling_groups.name'), index=True, nullable=True)
    domain_name = sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False)
    group_id = ForeignKeyIDColumn('group_id', 'groups.id', nullable=False)
    user_uuid = ForeignKeyIDColumn('user_uuid', 'users.uuid', nullable=False)
    kp_access_key = sa.Column('kp_access_key', sa.String(length=20), sa.ForeignKey('keypairs.access_key'))

    # if image_id is null, should find a image field from related kernel row.
    image_id = ForeignKeyIDColumn('image_id', 'images.id')
    tag = sa.Column('tag', sa.String(length=64), nullable=True)

    # Resource occupation
    # occupied_slots = sa.Column('occupied_slots', ResourceSlotColumn(), nullable=False)
    occupying_slots = sa.Column('occupying_slots', ResourceSlotColumn(), nullable=False)
    requested_slots = sa.Column('requested_slots', ResourceSlotColumn(), nullable=False)
    vfolder_mounts = sa.Column('vfolder_mounts', StructuredJSONObjectListColumn(VFolderMount), nullable=True)
    resource_opts = sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={})
    bootstrap_script= sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True)

    # Lifecycle
    created_at = sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), index=True)
    terminated_at = sa.Column('terminated_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null(), index=True)
    starts_at = sa.Column('starts_at', sa.DateTime(timezone=True),
              nullable=True, default=sa.null())
    status = sa.Column('status', EnumType(SessionStatus),
              default=SessionStatus.PENDING,
              server_default=SessionStatus.PENDING.name,
              nullable=False, index=True)
    status_changed = sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True, index=True)
    status_info = sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null())
    status_data = sa.Column('status_data', pgsql.JSONB(), nullable=True, default=sa.null())
    callback_url = sa.Column('callback_url', URLColumn, nullable=True, default=sa.null())

    startup_command = sa.Column('startup_command', sa.Text, nullable=True)
    result = sa.Column('result', EnumType(SessionResult),
              default=SessionResult.UNDEFINED,
              server_default=SessionResult.UNDEFINED.name,
              nullable=False, index=True)

    # Resource metrics measured upon termination
    num_queries = sa.Column('num_queries', sa.BigInteger(), default=0)
    last_stat = sa.Column('last_stat', pgsql.JSONB(), nullable=True, default=sa.null())

    __table_args__ = (
        # indexing
        sa.Index(
            'ix_sessions_updated_order',
            sa.func.greatest('created_at', 'terminated_at', 'status_changed'),
            unique=False,
        ),
    )


class SessionDependencyRow(Base):
    __tablename__ = 'session_dependencies'
    session_id = sa.Column('session_id', GUID,
              sa.ForeignKey('sessions.id', onupdate='CASCADE', ondelete='CASCADE'),
              index=True, nullable=False)
    depends_on = sa.Column('depends_on', GUID,
              sa.ForeignKey('sessions.id', onupdate='CASCADE', ondelete='CASCADE'),
              index=True, nullable=False)

    __table_args__ = (
        # constraint
        sa.PrimaryKeyConstraint(
            'session_id', 'depends_on',
            name='sess_dep_pk'),
    )
