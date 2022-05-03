"""add audit_logs table

Revision ID: 0e9df304197e
Revises: a7ca9f175d5f
Create Date: 2022-05-02 16:31:53.430719

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0e9df304197e'
down_revision = 'a7ca9f175d5f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('audit_logs',
                    sa.Column('user_id', sa.String(length=256), nullable=True),
                    sa.Column('access_key', sa.String(length=20), nullable=True),
                    sa.Column('email', sa.String(length=64), nullable=True),
                    sa.Column('action', postgresql.ENUM('CREATE', 'CHANGE',
                                                        'DELETE', name='auditlogs_action'), nullable=True),
                    sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
                    sa.Column('target', sa.String(length=64), nullable=True),
                    sa.Column('created_at', sa.DateTime(timezone=True),
                              server_default=sa.text('now()'), nullable=True)
                    )
    op.create_index(op.f('ix_audit_logs_access_key'), 'audit_logs', ['access_key'], unique=False)
    op.create_index(op.f('ix_audit_logs_action'), 'audit_logs', ['action'], unique=False)
    op.create_index(op.f('ix_audit_logs_created_at'), 'audit_logs', ['created_at'], unique=False)
    op.create_index(op.f('ix_audit_logs_email'), 'audit_logs', ['email'], unique=False)
    op.create_index(op.f('ix_audit_logs_target'), 'audit_logs', ['target'], unique=False)
    op.create_index(op.f('ix_audit_logs_user_id'), 'audit_logs', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_audit_logs_user_id'), table_name='audit_logs')
    op.drop_index(op.f('ix_audit_logs_target'), table_name='audit_logs')
    op.drop_index(op.f('ix_audit_logs_email'), table_name='audit_logs')
    op.drop_index(op.f('ix_audit_logs_created_at'), table_name='audit_logs')
    op.drop_index(op.f('ix_audit_logs_action'), table_name='audit_logs')
    op.drop_index(op.f('ix_audit_logs_access_key'), table_name='audit_logs')
    op.drop_table('audit_logs')
    # ### end Alembic commands ###
