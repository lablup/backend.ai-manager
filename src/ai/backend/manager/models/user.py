import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from passlib.hash import bcrypt
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, VARCHAR

from .base import metadata, IDColumn


__all__ = (
    'users',
    'User',
)


def _hash_password(password):
    return bcrypt.hash(password, rounds=12)


def _verify_password(guess, hashed):
    return bcrypt.verify(guess, hashed)


class PasswordColumn(TypeDecorator):
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        return _hash_password(value)


users = sa.Table(
    'users', metadata,
    IDColumn('id'),
    sa.Column('username', sa.String(length=64)),
    sa.Column('email', sa.String(length=64), index=True, nullable=False),
    PasswordColumn('password'),
    sa.Column('need_password_change', sa.Boolean),
    sa.Column('first_name', sa.String(length=32)),
    sa.Column('last_name', sa.String(length=32)),
    sa.Column('description', sa.String(length=500)),
    sa.Column('is_active', sa.Boolean),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
)


class User(graphene.ObjectType):
    id = graphene.UUID()
    username = graphene.String()
    email = graphene.String()
    password = graphene.String()
    need_password_change = graphene.Boolean()
    first_name = graphene.String()
    last_name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        return cls(
            id=row['id'],
            username=row['username'],
            email=row['email'],
            need_password_change=row['need_password_change'],
            first=row['first'],
            last=row['last'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
        )

    @staticmethod
    async def load_all(context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([users]).select_from(users)
            if is_active is not None:
                query = query.where(users.c.is_active == is_active)
            objs = []
            async for row in conn.execute(query):
                o = User.from_row(row)
                objs.append(o)
        return objs
