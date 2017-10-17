import enum
import sys
import uuid

from aiodataloader import DataLoader
from aiotools import apartial
import sqlalchemy as sa
from sqlalchemy.types import (
    SchemaType,
    TypeDecorator,
    CHAR
)
from sqlalchemy.dialects.postgresql import UUID, ENUM

# The common shared metadata instance
convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = sa.MetaData(naming_convention=convention)


# helper functions
def zero_if_none(val):
    return 0 if val is None else val


class EnumType(TypeDecorator, SchemaType):
    '''
    A stripped-down version of Spoqa's sqlalchemy-enum34.
    It also handles postgres-specific enum type creation.
    '''

    impl = ENUM

    def __init__(self, enum_cls, **opts):
        assert issubclass(enum_cls, enum.Enum)
        if 'name' not in opts:
            opts['name'] = enum_cls.__name__.lower()
        self._opts = opts
        self._enum_cls = enum_cls
        enums = (m.name for m in enum_cls)
        super().__init__(*enums, **opts)

    def _set_parent(self, column):
        self.impl._set_parent(column)

    def _set_table(self, table, column):
        self.impl._set_table(table, column)

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


class DataLoaderManager:
    '''
    For every different combination of filtering conditions, we need to make a
    new DataLoader instance because it "batches" the database queries.
    This manager get-or-creates dataloaders with fixed conditions (represetned
    as arguments) like a cache.

    NOTE: Just like DataLoaders, it is recommended to instantiate this manager
    for every incoming API request.
    '''

    def __init__(self, *common_args):
        self.cache = {}
        self.common_args = common_args
        self.mod = sys.modules['ai.backend.manager.models']

    @staticmethod
    def _get_key(otname, args, kwargs):
        '''
        Calculate the hash of the all arguments and keyword arguments.
        '''
        key = (otname, ) + args
        for item in kwargs.items():
            key += item
        return hash(key)

    def get_loader(self, objtype_name, *args, **kwargs):
        k = self._get_key(objtype_name, args, kwargs)
        loader = self.cache.get(k)
        if loader is None:
            objtype_name, has_variant, variant_name = objtype_name.partition('.')
            objtype = getattr(self.mod, objtype_name)
            if has_variant:
                batch_load_fn = getattr(objtype, 'batch_load_' + variant_name)
            else:
                batch_load_fn = objtype.batch_load
            loader = DataLoader(
                apartial(batch_load_fn, *self.common_args, *args, **kwargs),
                max_batch_size=16)
            self.cache[k] = loader
        return loader
