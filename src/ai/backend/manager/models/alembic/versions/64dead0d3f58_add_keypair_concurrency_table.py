"""add_keypair_concurrency_table

Revision ID: 64dead0d3f58
Revises: 60a1effa77d2
Create Date: 2022-03-04 20:06:50.684295

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '64dead0d3f58'
down_revision = '60a1effa77d2'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('keypairs', 'concurrency_used')
    op.create_table(
        'keypairs_concurrency',
        sa.Column('access_key', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('concurrency_used', sa.Integer),
    )


def downgrade():
    op.add_column('keypairs', sa.Column('concurrency_used', sa.Integer, nullable=True))
    op.drop_table('keypairs_concurrency')
