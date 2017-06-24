import logging

import sqlalchemy as sa

from . import register_command
from .. import models
from ..models import fixtures

log = logging.getLogger(__name__)


@register_command
def fixture(args):
    '''Manages database fixtures for testing.'''
    print('Please use -h/--help to see the usage.')


@fixture.register_command
def populate(args):
    '''Populate fixtures.'''
    log.info(f"populating fixture '{args.fixture_name}'")
    try:
        fixture = getattr(fixtures, args.fixture_name)
    except AttributeError:
        log.error('No such fixture.')
        return

    engine = sa.create_engine(f"postgres://{args.db_user}:{args.db_password}"
                              f"@{args.db_addr}/{args.db_name}")
    conn = engine.connect()
    for rowset in fixture:
        table = getattr(models, rowset[0])
        conn.execute(table.insert(), rowset[1])
    conn.close()


populate.add_argument('fixture_name', type=str, help='The name of fixture.')


@fixture.register_command
def list(args):
    '''List all available fixtures.'''
    for fixture_name in fixtures.__all__:
        f = getattr(fixtures, fixture_name)
        stat = map(lambda rowset: f"{rowset[0]}:{len(rowset[1])}", f)
        print(f"{fixture_name} ({', '.join(stat)})")
