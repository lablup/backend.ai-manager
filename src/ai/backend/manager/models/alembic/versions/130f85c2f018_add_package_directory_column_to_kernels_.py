"""Add package_directory column to kernels table

Revision ID: 130f85c2f018
Revises: 1e8531583e20
Create Date: 2020-01-28 14:13:59.722152

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '130f85c2f018'
down_revision = '1e8531583e20'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('kernels', sa.Column('package_directory', sa.ARRAY(sa.String()), nullable=True))


def downgrade():
    op.drop_column('kernels', 'package_directory')
