import asyncio
import enum
import functools
import logging
from typing import (
    Union,
    Dict, Mapping
)
import sys
import uuid

from aiodataloader import DataLoader
from aiotools import apartial
import graphene
from graphene.types import Scalar
from graphql.language import ast
from graphene.types.scalars import MIN_INT, MAX_INT
import psycopg2 as pg
import sqlalchemy as sa
from sqlalchemy.types import (
    SchemaType,
    TypeDecorator,
    CHAR
)
from sqlalchemy.dialects.postgresql import UUID, ENUM, JSONB

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import BinarySize, ResourceSlot
from .. import models
from ...gateway.exceptions import (
    GenericForbidden, InvalidAPIParameters,
)

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


class ResourceSlotColumn(TypeDecorator):
    '''
    A column type wrapper for ResourceSlot from JSONB.
    '''

    impl = JSONB

    def process_bind_param(self, value: Union[Mapping, ResourceSlot], dialect):
        if isinstance(value, Mapping) and not isinstance(value, ResourceSlot):
            return value
        return value.to_json() if value is not None else None

    def process_result_value(self, value: Dict[str, str], dialect):
        # legacy handling
        mem = value.get('mem')
        if isinstance(mem, str) and not mem.isdigit():
            value['mem'] = BinarySize.from_str(mem)
        return ResourceSlot.from_json(value) if value is not None else None

    def copy(self):
        return ResourceSlotColumn()


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


def ForeignKeyIDColumn(name, fk_field, nullable=True):
    return sa.Column(name, GUID, sa.ForeignKey(fk_field), nullable=nullable)


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


def privileged_query(required_role):

    def wrap(func):

        @functools.wraps(func)
        async def wrapped(executor, info, *args, **kwargs):
            from .user import UserRole
            if info.context['user']['role'] != UserRole.SUPERADMIN:
                raise GenericForbidden('superadmin privilege required')
            return await func(executor, info, *args, **kwargs)

        return wrapped

    return wrap


def scoped_query(*,
                 autofill_user: bool = False,
                 user_key: str = 'access_key'):
    '''
    Prepends checks for domain/group/user access rights depending
    on the client's user and keypair information.

    :param autofill_user: When the *user_key* is not specified,
        automatically fills out the user data with the current
        user who is makeing the API request.
    :param user_key: The key used for storing user identification value
        in the keyword arguments.
    '''

    def wrap(resolve_func):

        @functools.wraps(resolve_func)
        async def wrapped(executor, info, *args, **kwargs):
            from .user import UserRole
            client_role = info.context['user']['role']
            if user_key == 'access_key':
                client_user_id = info.context['access_key']
            elif user_key == 'email':
                client_user_id = info.context['user']['email']
            else:
                client_user_id = info.context['user']['uuid']
            client_domain = info.context['user']['domain_name']
            domain_name = kwargs.get('domain_name', None)
            group_id = kwargs.get('group_id', None)
            user_id = kwargs.get(user_key, None)
            if client_role == UserRole.SUPERADMIN:
                if autofill_user:
                    if user_id is None:
                        user_id = client_user_id
            elif client_role == UserRole.ADMIN:
                if domain_name is not None and domain_name != client_domain:
                    raise GenericForbidden
                domain_name = client_domain
                if group_id is not None:
                    # TODO: check if the group is a member of the domain
                    pass
                if autofill_user:
                    if user_id is None:
                        user_id = client_user_id
            elif client_role == UserRole.USER:
                if domain_name is not None and domain_name != client_domain:
                    raise GenericForbidden
                domain_name = client_domain
                if group_id is not None:
                    # TODO: check if the group is a member of the domain
                    # TODO: check if the client is a member of the group
                    pass
                if user_id is not None and user_id != client_user_id:
                    raise GenericForbidden
                user_id = client_user_id
            else:
                raise InvalidAPIParameters('Unknown client role')
            kwargs['domain_name'] = domain_name
            if group_id is not None:
                kwargs['group_id'] = group_id
            kwargs[user_key] = user_id
            return await resolve_func(executor, info, *args, **kwargs)

        return wrapped

    return wrap


def privileged_mutation(required_role, target_func=None):

    def wrap(func):

        @functools.wraps(func)
        async def wrapped(cls, root, info, *args, **kwargs):
            from .user import UserRole
            from .group import groups  # , association_groups_users
            user = info.context['user']
            permitted = False
            if required_role == UserRole.SUPERADMIN:
                if user['role'] == required_role:
                    permitted = True
            elif required_role == UserRole.ADMIN:
                if user['role'] == UserRole.SUPERADMIN:
                    permitted = True
                elif user['role'] == UserRole.USER:
                    permitted = False
                else:
                    if target_func is None:
                        return cls(False, 'misconfigured privileged mutation: no target_func', None)
                    target_domain, target_group = target_func(*args, **kwargs)
                    if target_domain is None and target_group is None:
                        return cls(False, 'misconfigured privileged mutation: '
                                          'both target_domain and target_group missing', None)
                    permit_chains = []
                    if target_domain is not None:
                        if user['domain_name'] == target_domain:
                            permit_chains.append(True)
                    if target_group is not None:
                        async with info.context['dbpool'].acquire() as conn, conn.begin():
                            # check if the group is part of the requester's domain.
                            query = (
                                groups.select()
                                .where(
                                    (groups.c.id == target_group) &
                                    (groups.c.domain_name == user['domain_name'])
                                )
                            )
                            result = await conn.execute(query)
                            if result.rowcount > 0:
                                permit_chains.append(True)
                            # TODO: check the group permission if implemented
                            # query = (
                            #     association_groups_users.select()
                            #     .where(association_groups_users.c.group_id == target_group)
                            # )
                            # result = await conn.execute(query)
                            # if result.rowcount > 0:
                            #     permit_chains.append(True)
                    permitted = all(permit_chains) if permit_chains else False
            elif required_role == UserRole.USER:
                permitted = True
            # assuming that mutation result objects has 2 or 3 fields:
            # success(bool), message(str) - usually for delete mutations
            # success(bool), message(str), item(object)
            if permitted:
                return await func(cls, root, info, *args, **kwargs)
            return cls(False, f"no permission to execute {info.path[0]}")

        return wrapped

    return wrap


async def simple_db_mutate(result_cls, context, mutation_query):
    async with context['dbpool'].acquire() as conn, conn.begin():
        try:
            result = await conn.execute(mutation_query)
            if result.rowcount > 0:
                return result_cls(True, 'success')
            else:
                return result_cls(False, 'no matching record')
        except (pg.IntegrityError, sa.exc.IntegrityError) as e:
            return result_cls(False, f'integrity error: {e}')
        except (asyncio.CancelledError, asyncio.TimeoutError):
            raise
        except Exception as e:
            return result_cls(False, f'unexpected error: {e}')


async def simple_db_mutate_returning_item(result_cls, context, mutation_query, *,
                                          item_query, item_cls):
    async with context['dbpool'].acquire() as conn, conn.begin():
        try:
            result = await conn.execute(mutation_query)
            if result.rowcount > 0:
                result = await conn.execute(item_query)
                item = await result.first()
                return result_cls(True, 'success', item_cls.from_row(item))
            else:
                return result_cls(False, 'no matching record', None)
        except (pg.IntegrityError, sa.exc.IntegrityError) as e:
            return result_cls(False, f'integrity error: {e}', None)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            raise
        except Exception as e:
            return result_cls(False, f'unexpected error: {e}', None)


def set_if_set(src, target, name, *, clean_func=None):
    v = getattr(src, name)
    # NOTE: unset optional fields are passed as null.
    if v is not None:
        if callable(clean_func):
            target[name] = clean_func(v)
        else:
            target[name] = v


def populate_fixture(db_connection, fixture_data):

    def insert(table, row):
        # convert enumtype to native values
        for col in table.columns:
            if isinstance(col.type, EnumType):
                row[col.name] = col.type._enum_cls[row[col.name]]
            elif isinstance(col.type, EnumValueType):
                row[col.name] = col.type._enum_cls(row[col.name])
        db_connection.execute(table.insert(), [row])

    for table_name, rows in fixture_data.items():
        table = getattr(models, table_name)
        pk_cols = table.primary_key.columns
        for row in rows:
            if len(pk_cols) == 0:
                # some tables may not have primary keys.
                # (e.g., m2m relationship)
                insert(table, row)
                continue
            # compose pk match where clause
            pk_match = functools.reduce(lambda x, y: x & y, [
                (col == row[col.name])
                for col in pk_cols
            ])
            ret = db_connection.execute(
                sa.select(pk_cols).select_from(table).where(pk_match))
            if ret.rowcount == 0:
                insert(table, row)
            else:
                pk_tuple = tuple(row[col.name] for col in pk_cols)
                log.info('skipped inserting {} to {} as the row already exists.',
                         f"[{','.join(pk_tuple)}]", table_name)
