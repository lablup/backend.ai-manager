import enum
import uuid

import sqlalchemy as sa
from sqlalchemy.types import (
    Enum as SAEnum,
    TypeDecorator,
    CHAR
)
from sqlalchemy.dialects.postgresql import UUID

# The common shared metadata instance
convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = sa.MetaData(naming_convention=convention)


class EnumType(TypeDecorator):
    '''
    A stripped-down version of Spoqa's sqlalchemy-enum34.
    '''

    impl = SAEnum

    def __init__(self, enum_cls, **opts):
        assert issubclass(enum_cls, enum.Enum)
        self._opts = opts
        self._enum_cls = enum_cls
        enums = (m.name for m in enum_cls)
        super().__init__(*enums, **opts)

    def process_bind_param(self, value, dialect):
        return value.name if value else None

    def process_result_value(self, value: str, dialect):
        return self._enum_cls[value] if value else None

    def copy(self):
        return EnumType(self._enum_cls, **self._opts)


class CurrencyTypes(enum.Enum):
    KRW = 'KRW'
    USD = 'USD'


class GUID(TypeDecorator):
    '''
    Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses CHAR(16) storing as raw bytes.
    '''
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(16))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            if isinstance(value, uuid.UUID):
                return str(value)
            else:
                return str(uuid.UUID(value))
        else:
            if isinstance(value, uuid.UUID):
                return value.bytes
            else:
                return uuid.UUID(value).bytes

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


def IDColumn(name='id'):
    return sa.Column(name, GUID, primary_key=True,
                     server_default=sa.text("uuid_generate_v4()"))
