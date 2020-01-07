"""Change default value of role column

Revision ID: d5cc54fd36b5
Revises: ce209920f654
Create Date: 2020-01-06 13:56:50.885635

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd5cc54fd36b5'
down_revision = 'ce209920f654'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('kernels', 'role',
                    server_default='master')

def downgrade():
    op.alter_column('kernels', 'role',
                    server_default='standalone')
