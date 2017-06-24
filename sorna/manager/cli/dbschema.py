import logging
import subprocess

import sqlalchemy as sa

from . import register_command
from ..models.base import metadata

log = logging.getLogger(__name__)


@register_command
def dbshell(args):
    '''Run the database shell.'''
    if args.dockerize:
        cmd = [
            'docker', 'run', '--rm', '-i', '-t',
            '--network', args.docker_network,
            'postgres:9.6',
            'psql',
            (f'postgres://{args.db_user}:{args.db_password}'
             f'@{args.docker_dbaddr}/{args.db_name}'),
        ]
    else:
        cmd = [
            'psql',
            (f'postgres://{args.db_user}:{args.db_password}'
             f'@{args.db_addr}/{args.db_name}'),
        ]
    subprocess.call(cmd)


dbshell.add_argument('-d', '--dockerize', action='store_true', default=False,
                     help='Assume dockerized db instance. It creates a '
                          'temporary pgsql shell container. [default: false]')
dbshell.add_argument('--docker-network', default='sorna_default',
                     help='The network name to attach the shell container. '
                          '(used only with --dockerize) '
                          '[default: sorna_default]')
dbshell.add_argument('--docker-dbaddr', default='sorna-db',
                     help='The address of the database host in the container. '
                          '(used only with --dockerize) [default: sorna-db]')


@register_command
def schema(args):
    '''Manages database schemas.'''
    print('Please use -h/--help to see the usage.')


@schema.register_command
def show(args):
    '''Show the current schema information.'''
    log.warning('Not implemented yet.')


@schema.register_command
def oneshot(args):
    '''Set up your database with one-shot schema migration instead of
    iterating over multiple revisions.

    Reference: http://alembic.zzzcomputing.com/en/latest/cookbook.html#building-an-up-to-date-database-from-scratch
    '''
    from alembic.config import Config
    from alembic import command

    log.info('Creating tables...')
    engine = sa.create_engine(f"postgres://{args.db_user}:{args.db_password}"
                              f"@{args.db_addr}/{args.db_name}")
    # it will raise error if tables already exist.
    metadata.create_all(engine, checkfirst=False)

    log.info(f'Stamping alembic version to {args.schema_version}...')
    alembic_cfg = Config(args.config)
    command.stamp(alembic_cfg, args.schema_version)
    log.info("If you don't need old migrations, delete them and set "
             "\"down_revision\" value in the earliest migration to \"None\".")


oneshot.add_argument('schema_version',
                     help='The schema version hash. (example: head)')
oneshot.add_argument('-f', '--config', default='alembic.ini', metavar='PATH',
                     help='The path to Alembic config file. '
                          '[default: alembic.ini]')
