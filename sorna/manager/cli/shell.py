import logging

from aioconsole.events import run_console
import asyncpgsa

from . import register_command

log = logging.getLogger(__name__)
_args = None


@register_command
def shell(args):
    '''Launch an interactive Python prompt running under an async event loop.'''
    global _args
    _args = args
    run_console()


async def create_dbpool():
    p = await asyncpgsa.create_pool(
        host=str(_args.db_addr[0]),
        port=_args.db_addr[1],
        database=_args.db_name,
        user=_args.db_user,
        password=_args.db_password,
        min_size=1, max_size=4,
    )
    return p
