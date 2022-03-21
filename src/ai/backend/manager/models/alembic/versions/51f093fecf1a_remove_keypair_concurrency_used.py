"""remove_keypair_concurrency_used

Revision ID: 51f093fecf1a
Revises: 1c8cbe9605df
Create Date: 2022-03-21 15:40:49.857290

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '51f093fecf1a'
down_revision = '1c8cbe9605df'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('keypairs', 'concurrency_used')


def downgrade():
    op.add_column('keypairs', sa.Column('concurrency_used', sa.Integer, nullable=True))
