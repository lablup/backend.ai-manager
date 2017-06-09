import logging

from . import BaseCommand

log = logging.getLogger(__name__)


class FixtureCommand(BaseCommand):

    '''Manages database fixtures for testing.'''

    @classmethod
    def init_argparser(cls, parser):
        parser.add_argument('-t' ,'--test', action='store_true')
        subparsers = parser.add_subparsers(title='sub-commands', dest='subcmd')
        subparsers.add_parser('populate', help='Populate fixtures')

    def execute(self, args):
        log.info(f'loading fixture with {args}')
