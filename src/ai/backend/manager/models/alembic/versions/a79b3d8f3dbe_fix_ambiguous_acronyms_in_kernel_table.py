"""Fix ambiguous acronyms in kernel table

Revision ID: a79b3d8f3dbe
Revises: 28e5b8c16f5b
Create Date: 2020-01-14 15:53:18.063040

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a79b3d8f3dbe'
down_revision = '28e5b8c16f5b'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('kernels', 'sess_id', new_column_name='session_id')
    op.alter_column('kernels', 'sess_uuid', new_column_name='session_uuid')
    op.alter_column('kernels', 'sess_type', new_column_name='session_type')


def downgrade():
    op.alter_column('kernels', 'session_id', new_column_name='sess_id')
    op.alter_column('kernels', 'session_uuid', new_column_name='sess_uuid')
    op.alter_column('kernels', 'session_type', new_column_name='sess_type')
