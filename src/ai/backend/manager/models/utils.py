from contextlib import asynccontextmanager as actxmgr
from typing import Any, AsyncIterator, Mapping, Tuple

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from aiopg.sa.connection import SAConnection
from aiopg.sa.engine import Engine as SAEngine


@actxmgr
async def reenter_txn(pool: SAEngine, conn: SAConnection) -> AsyncIterator[SAConnection]:
    if conn is None:
        async with pool.acquire() as conn, conn.begin():
            yield conn
    else:
        async with conn.begin_nested():
            yield conn


def sql_json_merge(
    col,
    key: Tuple[str, ...],
    obj: Mapping[str, Any],
    _depth: int = 0,
):
    """
    Generate an SQLAlchemy expression that merges the given object with
    the existing object of a specific (nested) key of the given JSONB column,
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
    last_level_obj: Mapping[str, Any] = None,
    _depth: int = 0,
):
    """
    Generate an SQLAlchemy expression that increments a specific (nested) key of the given JSONB column,
    with automatic creation of empty objects in parents and population of the optional last-level
    object at the same level with the target key.

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
                else sql_json_increment(col, key, last_level_obj=last_level_obj, _depth=_depth + 1)
            )
        )
    )
    if _depth == len(key) - 1 and last_level_obj is not None:
        expr = expr.concat(sa.func.cast(last_level_obj, psql.JSONB))
    return expr
