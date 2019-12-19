"""Add name column to task_template table

Revision ID: f2130004a149
Revises: 54582e232de9
Create Date: 2019-12-18 14:00:17.733913

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f2130004a149'
down_revision = '54582e232de9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'task_templates',
        sa.Column('name', sa.String(length=128), nullable=True)
    )


def downgrade():
    op.drop_column('task_templates', 'name')
