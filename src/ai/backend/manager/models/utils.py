from contextlib import asynccontextmanager as actxmgr

from aiopg.sa.connection import SAConnection
from aiopg.sa.engine import Engine as SAEngine


@actxmgr
async def reenter_txn(pool: SAEngine, conn: SAConnection):
    if conn is None:
        async with pool.acquire() as conn, conn.begin():
            yield conn
    else:
        async with conn.begin_nested():
            yield conn
