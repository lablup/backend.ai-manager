"""Add session_uuid column in kernels table

Revision ID: 1f328acc29d8
Revises: d5cc54fd36b5
Create Date: 2020-01-07 16:12:21.543329

"""
from alembic import op
import sqlalchemy as sa

from ai.backend.manager.models.base import (
    GUID
)
from ai.backend.manager.models import (
    kernels
)

# revision identifiers, used by Alembic.
revision = '1f328acc29d8'
down_revision = 'd5cc54fd36b5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'kernels',
        sa.Column('sess_uuid', GUID, nullable=True),
    )
    connection = op.get_bind()
    # Fill in sess_uuid for all legacy kernels
    query = sa.select([kernels.c.id]).select_from(kernels)
    all_kernels = connection.execute(query).fetchall()
    for kernel in all_kernels:
        query = f"UPDATE kernels SET sess_uuid = uuid_generate_v4() WHERE id = '{kernel['id']}'"
        connection.execute(query)
    op.alter_column('kernels', 'sess_uuid', nullable=False)

def downgrade():
    op.drop_column('kernels', 'sess_uuid')