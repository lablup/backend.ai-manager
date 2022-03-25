"""change char col to str

Revision ID: 11146ba02235
Revises: 0f7a4b643940
Create Date: 2022-03-25 12:32:05.637628

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam

# revision identifiers, used by Alembic.
revision = '11146ba02235'
down_revision = '0f7a4b643940'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    agents = sa.table('agents',
        sa.Column('id', sa.String), sa.Column('architecture', sa.String),
    )
    result = conn.execute(sa.select([agents.c.id, agents.c.architecture])).all()
    updates = []
    for row in result:
        if row.architecture != row.architecture.rstrip():
            print(f'len(row.architecture) = {len(row.architecture)}')
            updates.append({'row_id': row.id, 'architecture': row.architecture.rstrip()})
    op.alter_column('agents', column_name='architecture', type_=sa.String(length=32))
    query = (
        sa.update(agents)
        .values(architecture = bindparam('architecture'))
        .where(agents.c.id == bindparam('row_id'))
    )
    conn.execute(query, updates)

def downgrade():
    op.alter_column('agents', column_name='architecture', type_=sa.CHAR(length=32))
