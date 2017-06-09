import logging

import sqlalchemy as sa

from . import BaseCommand
from .. import models
from ..models import fixtures

log = logging.getLogger(__name__)


class FixtureCommand(BaseCommand):

    '''Manages database fixtures for testing.'''

    @classmethod
    def init_argparser(cls, parser):
        subparsers = parser.add_subparsers(title='sub-commands', dest='subcmd')

        subparser = subparsers.add_parser('populate', help='Populate fixtures')
        subparser.add_argument('name', type=str, help='The name of fixture.')

        subparsers.add_parser('list', help='List all available fixtures')

    def execute(self, args):
        if args.subcmd is None:
            log.error('You should specify which operation to execute. Checkout "--help" to see the usage.')
            return
        elif args.subcmd == 'populate':
            self.populate(args)
        elif args.subcmd == 'list':
            self.list(args)

    def populate(self, args):
        log.info(f"populating fixture '{args.name}'")
        try:
            fixture = getattr(fixtures, args.name)
        except AttributeError:
            log.error('No such fixture.')
            return

        engine = sa.create_engine(f"postgres://{args.db_user}:{args.db_password}@{args.db_addr}/{args.db_name}")
        conn = engine.connect()
        for rowset in fixture:
            table = getattr(models, rowset[0])
            conn.execute(table.insert(), rowset[1])
        conn.close()

    def list(self, args):
        for fixture_name in fixtures.__all__:
            f = getattr(fixtures, fixture_name)
            stat = map(lambda rowset: f"{rowset[0]}:{len(rowset[1])}", f)
            print(f"{fixture_name} ({', '.join(stat)})")
