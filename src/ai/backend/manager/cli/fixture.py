import functools
import logging
import json
from pathlib import Path

import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter

from . import register_command
from .. import models
from ..models.base import EnumType, EnumValueType

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
    for table_name, rows in fixture.items():
        table = getattr(models, table_name)
        cols = table.columns
        pk_cols = table.primary_key.columns
        for row in rows:
            # compose pk match where clause
            pk_match = functools.reduce(lambda x, y: x & y, [
                (col == row[col.name])
                for col in pk_cols
            ])
            ret = conn.execute(sa.select(pk_cols).select_from(table).where(pk_match))
            if ret.rowcount == 0:
                # convert enumtype to native values
                for col in cols:
                    if isinstance(col.type, EnumType):
                        row[col.name] = col.type._enum_cls[row[col.name]]
                    elif isinstance(col.type, EnumValueType):
                        row[col.name] = col.type._enum_cls(row[col.name])
                conn.execute(table.insert(), [row])
            else:
                pk_tuple = tuple(row[col.name] for col in pk_cols)
                log.info('skipped inserting {} to {} as the row already exists.',
                         f"[{','.join(pk_tuple)}]", table_name)
    conn.close()


populate.add('fixture_path', type=Path,
             help='The path to a fixture dataset written as JSON.')


@fixture.register_command
def list(args):
    '''List all available fixtures.'''
    print('WARNING: This command is deprecated.')
