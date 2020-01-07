"""add idx column to kernels

Revision ID: 28e5b8c16f5b
Revises: 1e673659b283
Create Date: 2020-01-07 21:06:07.007391

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '28e5b8c16f5b'
down_revision = '1e673659b283'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('kernels',
                  sa.Column('idx', sa.Integer, nullable=True, default=None))


def downgrade():
    op.drop_column('kernels', 'idx')
