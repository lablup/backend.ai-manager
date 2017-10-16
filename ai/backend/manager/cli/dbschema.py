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
dbshell.add_argument('--docker-network', default='backend_ai_default',
                     help='The network name to attach the shell container. '
                          '(used only with --dockerize) '
                          '[default: backend_ai_default]')
dbshell.add_argument('--docker-dbaddr', default='backendai-db',
                     help='The address of the database host in the container. '
                          '(used only with --dockerize) [default: backendai-db]')


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
    It uses alembic.ini to configure database connection.

    Reference: http://alembic.zzzcomputing.com/en/latest/cookbook.html
               #building-an-up-to-date-database-from-scratch
    '''
    from alembic.config import Config
    from alembic import command

    alembic_cfg = Config(args.config)
    sa_url = alembic_cfg.get_main_option('sqlalchemy.url')

    log.info('Creating tables...')
    engine = sa.create_engine(sa_url)
    engine.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    # it will raise error if tables already exist.
    metadata.create_all(engine, checkfirst=False)

    log.info(f'Stamping alembic version to {args.schema_version}...')
    command.stamp(alembic_cfg, args.schema_version)
    log.info("If you don't need old migrations, delete them and set "
             "\"down_revision\" value in the earliest migration to \"None\".")


oneshot.add_argument('schema_version',
                     help='The schema version hash. (example: head)')
oneshot.add_argument('-f', '--config', default='alembic.ini', metavar='PATH',
                     help='The path to Alembic config file. '
                          '[default: alembic.ini]')
