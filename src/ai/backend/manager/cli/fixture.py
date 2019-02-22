import logging
import json
from pathlib import Path

import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter

from . import register_command
from ..models.base import populate_fixture

log = BraceStyleAdapter(logging.getLogger(__name__))


@register_command
def fixture(args):
    '''Manages database fixtures for testing.'''
    print('Please use -h/--help to see the usage.')


@fixture.register_command
def populate(args):
    '''Populate fixtures.'''
    log.info("populating fixture '{0}'", args.fixture_path)
    try:
        fixture = json.loads(args.fixture_path.read_text(encoding='utf8'))
    except AttributeError:
        log.error('No such fixture.')
        return

    engine = sa.create_engine(f"postgres://{args.db_user}:{args.db_password}"
                              f"@{args.db_addr}/{args.db_name}")
    conn = engine.connect()
    populate_fixture(conn, fixture)
    conn.close()


populate.add('fixture_path', type=Path,
             help='The path to a fixture dataset written as JSON.')


@fixture.register_command
def list(args):
    '''List all available fixtures.'''
    print('WARNING: This command is deprecated.')
