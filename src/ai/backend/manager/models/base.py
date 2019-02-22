import enum
import functools
import logging
import sys
import uuid

from aiodataloader import DataLoader
from aiotools import apartial
import graphene
from graphene.types import Scalar
from graphql.language import ast
from graphene.types.scalars import MIN_INT, MAX_INT
import sqlalchemy as sa
from sqlalchemy.types import (
    SchemaType,
    TypeDecorator,
    CHAR
)
from sqlalchemy.dialects.postgresql import UUID, ENUM

from ai.backend.common.logging import BraceStyleAdapter
from .. import models

SAFE_MIN_INT = -9007199254740991
SAFE_MAX_INT = 9007199254740991

log = BraceStyleAdapter(logging.getLogger(__name__))

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

    The actual postgres enum choices are taken from the Python enum names.
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


class EnumValueType(TypeDecorator, SchemaType):
    '''
    A stripped-down version of Spoqa's sqlalchemy-enum34.
    It also handles postgres-specific enum type creation.

    The actual postgres enum choices are taken from the Python enum values.
    '''

    impl = ENUM

    def __init__(self, enum_cls, **opts):
        assert issubclass(enum_cls, enum.Enum)
        if 'name' not in opts:
            opts['name'] = enum_cls.__name__.lower()
        self._opts = opts
        self._enum_cls = enum_cls
        enums = (m.value for m in enum_cls)
        super().__init__(*enums, **opts)

    def _set_parent(self, column):
        self.impl._set_parent(column)

    def _set_table(self, table, column):
        self.impl._set_table(table, column)

    def process_bind_param(self, value, dialect):
        return value.value if value else None

    def process_result_value(self, value: str, dialect):
        return self._enum_cls(value) if value else None

    def copy(self):
        return EnumValueType(self._enum_cls, **self._opts)


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


class ResourceLimit(graphene.ObjectType):
    key = graphene.String()
    min = graphene.String()
    max = graphene.String()


class KVPair(graphene.ObjectType):
    key = graphene.String()
    value = graphene.String()


class BigInt(Scalar):
    """
    BigInt is an extension of the regular graphene.Int scalar type
    to support integers outside the range of a signed 32-bit integer.
    """

    @staticmethod
    def coerce_bigint(value):
        num = int(value)
        if not (SAFE_MIN_INT <= num <= SAFE_MAX_INT):
            raise ValueError(
                'Cannot serialize integer out of the safe range.')
        if not (MIN_INT <= num <= MAX_INT):
            # treat as float
            return float(int(num))
        return num

    serialize = coerce_bigint
    parse_value = coerce_bigint

    @staticmethod
    def parse_literal(node):
        if isinstance(node, ast.IntValue):
            num = int(node.value)
            if not (SAFE_MIN_INT <= num <= SAFE_MAX_INT):
                raise ValueError(
                    'Cannot parse integer out of the safe range.')
            if not (MIN_INT <= num <= MAX_INT):
                # treat as float
                return float(int(num))
            return num


class Item(graphene.Interface):
    id = graphene.ID()


class PaginatedList(graphene.Interface):
    items = graphene.List(Item, required=True)
    total_count = graphene.Int(required=True)


def populate_fixture(db_connection, fixture_data):
    for table_name, rows in fixture_data.items():
        table = getattr(models, table_name)
        cols = table.columns
        pk_cols = table.primary_key.columns
        for row in rows:
            # compose pk match where clause
            pk_match = functools.reduce(lambda x, y: x & y, [
                (col == row[col.name])
                for col in pk_cols
            ])
            ret = db_connection.execute(
                sa.select(pk_cols).select_from(table).where(pk_match))
            if ret.rowcount == 0:
                # convert enumtype to native values
                for col in cols:
                    if isinstance(col.type, EnumType):
                        row[col.name] = col.type._enum_cls[row[col.name]]
                    elif isinstance(col.type, EnumValueType):
                        row[col.name] = col.type._enum_cls(row[col.name])
                db_connection.execute(table.insert(), [row])
            else:
                pk_tuple = tuple(row[col.name] for col in pk_cols)
                log.info('skipped inserting {} to {} as the row already exists.',
                         f"[{','.join(pk_tuple)}]", table_name)
