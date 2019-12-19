"""Add bootstrap_script column to kernel table

Revision ID: 54582e232de9
Revises: ce209920f654
Create Date: 2019-12-17 21:04:48.889813

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '54582e232de9'
down_revision = 'ce209920f654'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'kernels',
        sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True)
    )

def downgrade():
    op.drop_column('kernels', 'bootstrap_script')
