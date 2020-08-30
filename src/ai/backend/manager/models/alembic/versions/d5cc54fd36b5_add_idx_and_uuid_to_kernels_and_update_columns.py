"""New idx / session_uuid columns to kernels table and refactor some columns.

Revision ID: d5cc54fd36b5
Revises: 0d553d59f369
Create Date: 2020-01-06 13:56:50.885635

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd5cc54fd36b5'
down_revision = '0d553d59f369'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('kernels', sa.Column('idx', sa.Integer, nullable=True, default=None))

    # Convert "master" to "main"
    # NOTE: "main" is defined from ai.backend.manager.defs.DEFAULT_ROLE
    op.alter_column('kernels', 'role', server_default='main')
    conn = op.get_bind()
    query = "UPDATE kernels SET role = 'main' WHERE role = 'master'"
    conn.execute(query)

    op.alter_column('kernels', 'sess_id', new_column_name='session_id')
    op.alter_column('kernels', 'sess_type', new_column_name='session_type')


def downgrade():
    op.alter_column('kernels', 'session_type', new_column_name='sess_type')
    op.alter_column('kernels', 'session_id', new_column_name='sess_id')

    # Convert "main" to "master" for backward compatibility
    op.alter_column('kernels', 'role', server_default='master')
    conn = op.get_bind()
    query = "UPDATE kernels SET role = 'master' WHERE role = 'main'"
    conn.execute(query)

    op.drop_column('kernels', 'idx')
