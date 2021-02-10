"""add inactive status to agent status

Revision ID: 97a85e31293b
Revises: 518ecf41f567
Create Date: 2021-02-09 19:00:12.344858

"""
import textwrap

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '97a85e31293b'
down_revision = '518ecf41f567'
branch_labels = None
depends_on = None


agentstatus_new_values = [
    'ALIVE',
    'LOST',
    'RESTARTING',
    'TERMINATED',
    'INACTIVE'      # added
]

agentstatus_new = postgresql.ENUM(*agentstatus_new_values, name='agentstatus')

agentstatus_old_values = [
    'ALIVE',
    'LOST',
    'RESTARTING',
    'TERMINATED',
]
agentstatus_old = postgresql.ENUM(*agentstatus_old_values, name='agentstatus')


def upgrade():
    conn = op.get_bind()
    conn.execute('ALTER TYPE agentstatus RENAME TO agentstatus_old;')
    agentstatus_new.create(conn)
    conn.execute(textwrap.dedent('''\
    ALTER TABLE agents
        ALTER COLUMN "status" DROP DEFAULT,
        ALTER COLUMN "status" TYPE agentstatus USING "status"::text::agentstatus,
        ALTER COLUMN "status" SET DEFAULT 'ALIVE'::agentstatus;
    DROP TYPE agentstatus_old;
    '''))


def downgrade():
    conn = op.get_bind()
    conn.execute('ALTER TYPE agentstatus RENAME TO agentstatus_new;')
    agentstatus_old.create(conn)
    conn.execute(textwrap.dedent('''\
    ALTER TABLE agents
        ALTER COLUMN "status" TYPE agentstatus USING (
            CASE "status"::text
                WHEN 'INACTIVE' THEN 'ALIVE'
                ELSE "status"::text
            END
        )::agentstatus
    DROP TYPE agentstatus_new;
    '''))
