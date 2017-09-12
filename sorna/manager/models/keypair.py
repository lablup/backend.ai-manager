import sqlalchemy as sa
from .base import metadata

__all__ = ('keypairs', )


keypairs = sa.Table(
    'keypairs', metadata,
    sa.Column('user_id', sa.Integer(), index=True),  # foreign key
    sa.Column('access_key', sa.String(length=20), primary_key=True),
    sa.Column('secret_key', sa.String(length=40)),
    sa.Column('is_active', sa.Boolean, index=True),
    sa.Column('resource_policy', sa.String, nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('concurrency_limit', sa.Integer),
    sa.Column('concurrency_used', sa.Integer),
    sa.Column('rate_limit', sa.Integer),
    sa.Column('num_queries', sa.Integer, server_default='0'),
)
