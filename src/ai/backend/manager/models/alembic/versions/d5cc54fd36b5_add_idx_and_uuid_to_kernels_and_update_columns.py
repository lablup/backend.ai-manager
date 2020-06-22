"""New idx / session_uuid columns to kernels table and refactor some columns.

Revision ID: d5cc54fd36b5
Revises: 529113b08c2c
Create Date: 2020-01-06 13:56:50.885635

"""
from alembic import op
import sqlalchemy as sa

from ai.backend.manager.models.base import GUID
from ai.backend.manager.models import kernels

# revision identifiers, used by Alembic.
revision = 'd5cc54fd36b5'
down_revision = '529113b08c2c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('kernels', sa.Column('idx', sa.Integer, nullable=True, default=None))
    op.add_column('kernels', sa.Column('session_uuid', GUID, nullable=True))

    op.alter_column('kernels', 'role', server_default='master')
    op.alter_column('kernels', 'sess_id', new_column_name='session_id')
    op.alter_column('kernels', 'sess_type', new_column_name='session_type')

    # Fill in session_uuid for all legacy kernels.
    conn = op.get_bind()
    query = sa.select([kernels.c.id]).select_from(kernels)
    all_kernels = conn.execute(query).fetchall()
    for kernel in all_kernels:
        query = f"UPDATE kernels SET session_uuid = uuid_generate_v4() WHERE id = '{kernel['id']}'"
        conn.execute(query)
    op.alter_column('kernels', 'session_uuid', nullable=False)

def downgrade():
    op.alter_column('kernels', 'session_type', new_column_name='sess_type')
    op.alter_column('kernels', 'session_id', new_column_name='sess_id')
    op.alter_column('kernels', 'role', server_default='standalone')

    op.drop_column('kernels', 'session_uuid')
    op.drop_column('kernels', 'idx')
