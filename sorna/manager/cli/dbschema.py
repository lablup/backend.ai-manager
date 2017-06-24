import logging

import sqlalchemy as sa

from . import register_command

log = logging.getLogger(__name__)


@register_command
def schema(args):
    '''Manages database schemas.'''
    print('Please use -h/--help to see the usage.')


@schema.register_command
def show(args):
    '''Show the current schema information.'''
    log.warning('Not implemented yet.')


@schema.register_command
def fix(args):
    '''Fix the current schema HEAD version.'''
    log.warning('Not implemented yet.')


fix.add_argument('schema_version', help='The schema version hash.')
