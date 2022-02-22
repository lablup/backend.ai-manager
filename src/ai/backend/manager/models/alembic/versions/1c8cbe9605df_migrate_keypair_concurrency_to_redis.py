"""migrate_keypair_concurrency_to_redis

Revision ID: 1c8cbe9605df
Revises: 60a1effa77d2
Create Date: 2022-02-22 11:54:22.056005

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1c8cbe9605df'
down_revision = '60a1effa77d2'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('keypairs', 'concurrency_used')


def downgrade():
    op.add_column('keypairs', sa.Column('concurrency_used', sa.Integer, default=0))
