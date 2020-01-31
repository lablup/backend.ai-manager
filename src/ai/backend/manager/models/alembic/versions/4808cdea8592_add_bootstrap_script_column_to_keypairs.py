"""add_bootstrap_script_column_to_keypairs

Revision ID: 4808cdea8592
Revises: 1e8531583e20
Create Date: 2020-01-31 14:31:59.652897

"""
from alembic import op
import sqlalchemy as sa
from ai.backend.manager.models import keypairs


# revision identifiers, used by Alembic.
revision = '4808cdea8592'
down_revision = '1e8531583e20'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('keypairs', sa.Column('bootstrap_script',
                                        sa.LargeBinary(length=64 * 1024),
                                        nullable=False,
                                        server_default='\\x90'))


def downgrade():
    op.drop_column('keypairs', 'bootstrap_script')
