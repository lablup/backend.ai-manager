"""add architecture column on agents

Revision ID: 911023380bc9
Revises: 015d84d5a5ef
Create Date: 2022-02-16 00:54:23.261212

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '911023380bc9'
down_revision = '015d84d5a5ef'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'agents',
        sa.Column('architecture', sa.CHAR(length=32), default='x86_64'))
    op.execute('UPDATE agents SET architecture=\'x86_64\'')
    op.alter_column('agents', 'architecture', nullable=False)


def downgrade():
    op.drop_column('agents', 'architecture')
