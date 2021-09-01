"""add kernel pull progress

Revision ID: 48389041a712
Revises: 8679d0a7e22b
Create Date: 2021-08-30 19:38:08.974019

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '48389041a712'
down_revision = '8679d0a7e22b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('kernels', sa.Column('current_progress', sa.Integer, default=0))
    op.add_column('kernels', sa.Column('total_progress', sa.Integer, default=0))
    pass


def downgrade():
    pass
