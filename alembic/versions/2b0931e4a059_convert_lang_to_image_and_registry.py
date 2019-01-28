"""convert-lang-to-image-and-registry

Revision ID: 2b0931e4a059
Revises: f0f4ee907155
Create Date: 2019-01-28 23:53:44.342786

"""
from alembic import op
import sqlalchemy as sa
import ai.backend.manager.models.base  # noqa


# revision identifiers, used by Alembic.
revision = '2b0931e4a059'
down_revision = 'f0f4ee907155'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('kernels', column_name='lang', new_column_name='image')
    op.add_column('kernels', sa.Column('registry', sa.String(length=512),
                  nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('kernels', column_name='image', new_column_name='lang')
    op.drop_column('kernels', 'registry')
    # ### end Alembic commands ###
