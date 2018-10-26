import logging
import subprocess

from alembic.config import Config
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter

from . import register_command
from ..models.base import metadata

log = BraceStyleAdapter(logging.getLogger(__name__))


@register_command
def dbshell(args):
    '''Run the database shell.'''
    if args.dockerize:
        cmd = [
            'docker', 'run', '--rm', '-i', '-t',
            '--network', args.docker_network,
            'postgres:9.6-alpine',
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


dbshell.add('-d', '--dockerize', action='store_true', default=False,
            help='Assume dockerized db instance. It creates a '
                 'temporary pgsql shell container. [default: false]')
dbshell.add('--docker-network', default='backend_ai_default',
            help='The network name to attach the shell container. '
                 '(used only with --dockerize) '
                 '[default: backend_ai_default]')
dbshell.add('--docker-dbaddr', default='backendai-db',
            help='The address of the database host in the container. '
                 '(used only with --dockerize) [default: backendai-db]')


@register_command
def schema(args):
    '''Manages database schemas.'''
    print('Please use -h/--help to see the usage.')


@schema.register_command
def show(args):
    '''Show the current schema information.'''
    alembic_cfg = Config(args.config)
    sa_url = alembic_cfg.get_main_option('sqlalchemy.url')
    engine = sa.create_engine(sa_url)
    with engine.begin() as connection:
        context = MigrationContext.configure(connection)
        current_rev = context.get_current_revision()

    script = ScriptDirectory.from_config(alembic_cfg)
    heads = script.get_heads()
    head_rev = heads[0] if len(heads) > 0 else None
    print(f'Current database revision: {current_rev}')
    print(f'The head revision of available migrations: {head_rev}')


show.add('-f', '--config', default='alembic.ini', metavar='PATH',
         help='The path to Alembic config file. '
              '[default: alembic.ini]')


@schema.register_command
def oneshot(args):
    '''Set up your database with one-shot schema migration instead of
    iterating over multiple revisions.
    It uses alembic.ini to configure database connection.

    Reference: http://alembic.zzzcomputing.com/en/latest/cookbook.html
               #building-an-up-to-date-database-from-scratch
    '''
    alembic_cfg = Config(args.config)
    sa_url = alembic_cfg.get_main_option('sqlalchemy.url')

    engine = sa.create_engine(sa_url)
    engine.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    with engine.begin() as connection:
        context = MigrationContext.configure(connection)
        current_rev = context.get_current_revision()

    if current_rev is None:
        # For a fresh clean database, create all from scratch.
        # (it will raise error if tables already exist.)
        log.info('Detected a fresh new database.')
        log.info('Creating tables...')
        with engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            metadata.create_all(engine, checkfirst=False)
            log.info('Stamping alembic version to {0}...', args.schema_version)
            command.stamp(alembic_cfg, args.schema_version)
    else:
        # If alembic version info is already available, perform incremental upgrade.
        log.info('Detected an existing database.')
        log.info('Performing schema upgrade to {0}...', args.schema_version)
        with engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "head")

    log.info("If you don't need old migrations, delete them and set "
             "\"down_revision\" value in the earliest migration to \"None\".")


oneshot.add('schema_version',
            help='The schema version hash. (example: head)')
oneshot.add('-f', '--config', default='alembic.ini', metavar='PATH',
            help='The path to Alembic config file. '
                 '[default: alembic.ini]')
