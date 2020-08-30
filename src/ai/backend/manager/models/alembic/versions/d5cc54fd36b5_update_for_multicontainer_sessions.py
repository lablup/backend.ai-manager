"""Update for multi-container sessions.

Revision ID: d5cc54fd36b5
Revises: 0d553d59f369
Create Date: 2020-01-06 13:56:50.885635

"""
from alembic import op
import sqlalchemy as sa

from ai.backend.manager.models.base import GUID

# revision identifiers, used by Alembic.
revision = 'd5cc54fd36b5'
down_revision = '0d553d59f369'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    op.add_column(
        'kernels',
        sa.Column('idx', sa.Integer, nullable=True, default=None))
    op.add_column(
        'kernels',
        sa.Column('cluster_mode', sa.String(16), nullable=True, default=None))

    # Convert "master" to "main"
    # NOTE: "main" is defined from ai.backend.manager.defs.DEFAULT_ROLE
    op.alter_column('kernels', 'role', server_default='main')
    query = "UPDATE kernels SET role = 'main' WHERE role = 'master'"
    conn.execute(query)

    # Clear up the column namings:
    #   sess_id -> session_name
    #     => client-provided alias
    #   (new) -> session_id
    #     => for single-container sessions, it may be derived from the kernel id.
    #   sess_type -> session_type
    # First a session_id column as nullable and fill it up before setting it non-nullable.
    op.add_column('kernels', sa.Column('session_id', GUID, nullable=True))
    query = "UPDATE kernels SET session_id = kernels.id WHERE role = 'main'"
    conn.execute(query)
    op.alter_column('kernels', 'session_id', nullable=False)

    op.alter_column('kernels', 'sess_id', new_column_name='session_name')
    op.alter_column('kernels', 'sess_type', new_column_name='session_type')


def downgrade():
    op.alter_column('kernels', 'session_type', new_column_name='sess_type')
    op.alter_column('kernels', 'session_name', new_column_name='sess_id')
    op.drop_column('kernels', 'session_id')

    # Convert "main" to "master" for backward compatibility
    op.alter_column('kernels', 'role', server_default='master')
    conn = op.get_bind()
    query = "UPDATE kernels SET role = 'master' WHERE role = 'main'"
    conn.execute(query)

    op.drop_column('kernels', 'cluster_mode')
    op.drop_column('kernels', 'idx')
