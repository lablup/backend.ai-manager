from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager as actxmgr
import logging
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Final,
    Mapping,
    Tuple,
    TypeVar,
)

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.engine import create_engine as _create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection as SAConnection,
    AsyncEngine as SAEngine,
)

from ai.backend.common.logging import BraceStyleAdapter

from ..defs import AdvisoryLock

log = BraceStyleAdapter(logging.getLogger(__name__))


class ExtendedAsyncSAEngine(SAEngine):
    """
    A subclass to add a few more convenience methods to the SQLAlchemy's async engine.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._readonly_txn_count = 0
        self._generic_txn_count = 0
        self._txn_concurrency_threshold = kwargs.pop("txn_concurrency_threshold", 8)

    @actxmgr
    async def begin(self) -> AsyncIterator[SAConnection]:
        async with super().begin() as conn:
            self._generic_txn_count += 1
            if self._generic_txn_count >= self._txn_concurrency_threshold:
                log.warning(
                    "The number of concurrent read-only transaction ({}) exceeded the threshold {}.",
                    self._generic_txn_count, self._txn_concurrency_threshold,
                    stack_info=True,
                )
            try:
                yield conn
            finally:
                self._generic_txn_count -= 1

    @actxmgr
    async def begin_readonly(self, deferrable: bool = False) -> AsyncIterator[SAConnection]:
        async with self.connect() as conn:
            self._readonly_txn_count += 1
            if self._readonly_txn_count >= self._txn_concurrency_threshold:
                log.warning(
                    "The number of concurrent generic transaction ({}) exceeded the threshold {}.",
                    self._readonly_txn_count, self._txn_concurrency_threshold,
                    stack_info=True,
                )
            conn_with_exec_opts = await conn.execution_options(
                postgresql_readonly=True,
                postgresql_deferrable=deferrable,
            )
            async with conn_with_exec_opts.begin():
                try:
                    yield conn_with_exec_opts
                finally:
                    self._readonly_txn_count -= 1

    @actxmgr
    async def advisory_lock(self, lock_id: AdvisoryLock) -> AsyncIterator[None]:
        async with self.connect() as lock_conn:
            # It is usually a BAD practice to directly interpolate strings into SQL statements,
            # but in this case:
            #  - The lock ID is only given from trusted codes.
            #  - asyncpg does not support parameter interpolation with raw SQL statements.
            await lock_conn.exec_driver_sql(f"SELECT pg_advisory_lock({lock_id:d})")
            try:
                yield
            finally:
                await lock_conn.exec_driver_sql(f"SELECT pg_advisory_unlock({lock_id:d})")


def create_async_engine(*args, **kwargs) -> ExtendedAsyncSAEngine:
    kwargs["future"] = True
    sync_engine = _create_engine(*args, **kwargs)
    return ExtendedAsyncSAEngine(sync_engine)


@actxmgr
async def reenter_txn(
    pool: ExtendedAsyncSAEngine,
    conn: SAConnection,
    execution_opts: Mapping[str, Any] | None = None,
) -> AsyncIterator[SAConnection]:
    if conn is None:
        async with pool.connect() as conn:
            if execution_opts:
                await conn.execution_options(**execution_opts)
            async with conn.begin():
                yield conn
    else:
        async with conn.begin_nested():
            yield conn


TQueryResult = TypeVar('TQueryResult')


async def execute_with_retry(txn_func: Callable[[], Awaitable[TQueryResult]]) -> TQueryResult:
    max_retries: Final = 10
    num_retries = 0
    while True:
        if num_retries == max_retries:
            raise RuntimeError(f"DB serialization failed after {max_retries} retries")
        try:
            return await txn_func()
        except DBAPIError as e:
            num_retries += 1
            if getattr(e.orig, 'pgcode', None) == '40001':
                await asyncio.sleep((num_retries - 1) * 0.02)
                continue
            raise


def sql_json_merge(
    col,
    key: Tuple[str, ...],
    obj: Mapping[str, Any],
    *,
    _depth: int = 0,
):
    """
    Generate an SQLAlchemy column update expression that merges the given object with
    the existing object at a specific (nested) key of the given JSONB column,
    with automatic creation of empty objects in parents and the target level.

    Note that the existing value must be also an object, not a primitive value.
    """
    expr = sa.func.coalesce(
        col if _depth == 0 else col[key[:_depth]],
        sa.text("'{}'::jsonb"),
    ).concat(
        sa.func.jsonb_build_object(
            key[_depth],
            (
                sa.func.coalesce(col[key], sa.text("'{}'::jsonb"))
                .concat(sa.func.cast(obj, psql.JSONB))
                if _depth == len(key) - 1
                else sql_json_merge(col, key, obj=obj, _depth=_depth + 1)
            )
        )
    )
    return expr


def sql_json_increment(
    col,
    key: Tuple[str, ...],
    *,
    parent_updates: Mapping[str, Any] = None,
    _depth: int = 0,
):
    """
    Generate an SQLAlchemy column update expression that increments the value at a specific
    (nested) key of the given JSONB column,
    with automatic creation of empty objects in parents and population of the
    optional parent_updates object to the target key's parent.

    Note that the existing value of the parent key must be also an object, not a primitive value.
    """
    expr = sa.func.coalesce(
        col if _depth == 0 else col[key[:_depth]],
        sa.text("'{}'::jsonb"),
    ).concat(
        sa.func.jsonb_build_object(
            key[_depth],
            (
                sa.func.coalesce(col[key].as_integer(), 0) + 1
                if _depth == len(key) - 1
                else sql_json_increment(col, key, parent_updates=parent_updates, _depth=_depth + 1)
            )
        )
    )
    if _depth == len(key) - 1 and parent_updates is not None:
        expr = expr.concat(sa.func.cast(parent_updates, psql.JSONB))
    return expr
