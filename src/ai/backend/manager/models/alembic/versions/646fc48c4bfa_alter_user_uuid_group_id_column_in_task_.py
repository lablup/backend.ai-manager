"""Alter group_id column in task_template to nullable column

Revision ID: 646fc48c4bfa
Revises: f2130004a149
Create Date: 2019-12-18 14:46:38.870502

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '646fc48c4bfa'
down_revision = 'f2130004a149'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('task_templates', 'group_id', nullable=True)


def downgrade():
    op.alter_column('task_templates', 'group_id', nullable=False)
