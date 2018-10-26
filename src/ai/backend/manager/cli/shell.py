import logging

from aioconsole.events import run_console
from aiopg.sa import create_engine

from ai.backend.common.logging import BraceStyleAdapter

from . import register_command

log = BraceStyleAdapter(logging.getLogger(__name__))
_args = None


@register_command
def shell(args):
    '''Launch an interactive Python prompt running under an async event loop.'''
    global _args
    _args = args
    run_console()


async def create_dbpool():
    p = await create_engine(
        host=_args.db_addr[0], port=_args.db_addr[1],
        user=_args.db_user, password=_args.db_password,
        dbname=_args.db_name, minsize=1, maxsize=4
    )
    return p
