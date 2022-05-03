from __future__ import annotations

from datetime import datetime
import logging
from setproctitle import setproctitle
import subprocess
import sys
from pathlib import Path

import click
import psycopg2

from ai.backend.common.cli import LazyGroup
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import TimeDuration

from ..config import load as load_config
from ..models.keypair import generate_keypair as _gen_keypair
from .context import CLIContext, init_logger

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.cli'))


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./manager.conf and /etc/backend.ai/manager.conf)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(ctx, config_path, debug):
    local_config = load_config(config_path)
    setproctitle(f"backend.ai: manager.cli {local_config['etcd']['namespace']}")
    ctx.obj = CLIContext(
        logger=init_logger(local_config),
        local_config=local_config,
    )


@main.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.option('--psql-container', 'container_name', type=str, default=None,
              metavar='ID_OR_NAME',
              help='Open a postgres client shell using the psql executable '
                   'shipped with the given postgres container. '
                   'If not set or set as an empty string "", it will auto-detect '
                   'the psql container from the halfstack. '
                   'If set "-", it will use the host-provided psql executable. '
                   'You may append additional arguments passed to the psql cli command. '
                   '[default: auto-detect from halfstack]')
@click.option('--psql-help', is_flag=True,
              help='Show the help text of the psql command instead of '
                   'this dbshell command.')
@click.argument('psql_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def dbshell(cli_ctx: CLIContext, container_name, psql_help, psql_args):
    """
    Run the database shell.

    All arguments except `--psql-container` and `--psql-help` are transparently
    forwarded to the psql command.  For instance, you can use `-c` to execute a
    psql/SQL statement on the command line.  Note that you do not have to specify
    connection-related options because the dbshell command fills out them from the
    manager configuration.
    """
    local_config = cli_ctx.local_config
    if psql_help:
        psql_args = ['--help']
    if not container_name:
        # Try to get the database container name of the halfstack
        candidate_container_names = subprocess.check_output(
            ['docker', 'ps', '--format', '{{.Names}}', '--filter', 'name=half-db'],
        )
        if not candidate_container_names:
            click.echo("Could not find the halfstack postgres container. "
                       "Please set the container name explicitly.",
                       err=True)
            sys.exit(1)
        container_name = candidate_container_names.decode().splitlines()[0].strip()
    elif container_name == '-':
        # Use the host-provided psql command
        cmd = [
            'psql',
            (f"postgres://{local_config['db']['user']}:{local_config['db']['password']}"
             f"@{local_config['db']['addr']}/{local_config['db']['name']}"),
            *psql_args,
        ]
        subprocess.call(cmd)
        return
    # Use the container to start the psql client command
    print(f"using the db container {container_name} ...")
    cmd = [
        'docker', 'exec', '-i', '-t',
        container_name,
        'psql',
        '-U', local_config['db']['user'],
        '-d', local_config['db']['name'],
        *psql_args,
    ]
    subprocess.call(cmd)


@main.command()
@click.pass_obj
def generate_keypair(cli_ctx: CLIContext):
    """
    Generate a random keypair and print it out to stdout.
    """
    log.info('generating keypair...')
    ak, sk = _gen_keypair()
    print(f'Access Key: {ak} ({len(ak)} bytes)')
    print(f'Secret Key: {sk} ({len(sk)} bytes)')


@main.command()
@click.option('-r', '--retention', type=str, default='1yr',
              help='The retention limit. e.g., 20d, 1mo, 6mo, 1yr')
@click.option('-v', '--vacuum-full', type=bool, default=False,
              help='Reclaim storage occupied by dead tuples.'
                    'If not set or set False, it will run VACUUM without FULL.'
                    'If set True, it will run VACUUM FULL.'
                    'When VACUUM FULL is being processed, the database is locked.'
                    '[default: False]')
@click.option('-t', '--table', type=click.Choice(['kernels', 'audit_logs'], case_sensitive=False),
                default='kernels',
                help='Which table to run the operation on. Options available are kernels and audit_logs. '
                'If not set or set to kernels, it will run the operation on kernels table. '
                'If set to audit_logs, it will run on audit_logs table. '
                '[default: kernels]')
@click.option('-o', '--object', type=click.Choice(['user', 'keypair'], case_sensitive=False),
                default=None,
                help='For audit_logs you can chose which audit target object you would like the operation to run on. '
                'You can chose between user and keypairs or both. '
                'If not set, it will run on both users and keypairs targets. ')
@click.pass_obj
def clear_history(cli_ctx: CLIContext, retention, vacuum_full, table, object) -> None:
    """
    Delete old records from the kernels or audit_logs tables and
    invoke the PostgreSQL's vaccuum operation to clear up the actual disk space.
    """
    local_config = cli_ctx.local_config
    with cli_ctx.logger:
        today = datetime.now()
        duration = TimeDuration()
        expiration = today - duration.check_and_return(retention)
        expiration_date = expiration.strftime('%Y-%m-%d %H:%M:%S')
        table = 'kernels' if table != 'audit_logs' else 'audit_logs'
        treshold_date = 'terminated_at' if table != 'audit_logs' else 'created_at'

        conn = psycopg2.connect(
            host=local_config['db']['addr'][0],
            port=local_config['db']['addr'][1],
            dbname=local_config['db']['name'],
            user=local_config['db']['user'],
            password=local_config['db']['password'],
        )
        with conn.cursor() as curs:
            if vacuum_full:
                vacuum_sql = "VACUUM FULL"
            else:
                vacuum_sql = "VACUUM"
            if table == 'audit_logs' and object is not None:
                curs.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {treshold_date} < '{expiration_date}'
                 AND target = '{str(object)}';
                """)
            else:
                curs.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {treshold_date} < '{expiration_date}';
                """)
            deleted_count = curs.fetchone()[0]

            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            log.info('Deleting old records...')
            if table == 'audit_logs' and object is not None:
                curs.execute(f"""
                DELETE FROM {table} WHERE {treshold_date} < '{expiration_date}'
                 AND target = '{str(object)}';
                """)
                log.info(
                    f'Perfoming {vacuum_sql} operation on {table} where target object is {str(object)}...')
            else:
                curs.execute(f"""
                DELETE FROM {table} WHERE {treshold_date} < '{expiration_date}';
                """)
                log.info(f'Perfoming {vacuum_sql} operation on {table}...')
            curs.execute(vacuum_sql)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

            curs.execute(f"""
            SELECT COUNT(*) FROM {table};
            """)
            table_size = curs.fetchone()[0]
            log.info(f'{table} table size: {table_size}')

        log.info('Cleaned up {:,} database records older than {:}.', deleted_count, expiration_date)

    """
    Delete old records from the kernels table and
    invoke the PostgreSQL's vaccuum operation to clear up the actual disk space.
    """
    local_config = cli_ctx.local_config
    with cli_ctx.logger:
        today = datetime.now()
        duration = TimeDuration()
        expiration = today - duration.check_and_return(retention)
        expiration_date = expiration.strftime('%Y-%m-%d %H:%M:%S')

        conn = psycopg2.connect(
            host=local_config['db']['addr'][0],
            port=local_config['db']['addr'][1],
            dbname=local_config['db']['name'],
            user=local_config['db']['user'],
            password=local_config['db']['password'],
        )
        with conn.cursor() as curs:
            if vacuum_full:
                vacuum_sql = "VACUUM FULL"
            else:
                vacuum_sql = "VACUUM"

            curs.execute(f"""
            SELECT COUNT(*) FROM kernels WHERE terminated_at < '{expiration_date}';
            """)
            deleted_count = curs.fetchone()[0]

            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            log.info('Deleting old records...')
            curs.execute(f"""
            DELETE FROM kernels WHERE terminated_at < '{expiration_date}';
            """)
            log.info(f'Perfoming {vacuum_sql} operation...')
            curs.execute(vacuum_sql)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

            curs.execute("""
            SELECT COUNT(*) FROM kernels;
            """)
            table_size = curs.fetchone()[0]
            log.info(f'kernels table size: {table_size}')

        log.info('Cleaned up {:,} database records older than {:}.', deleted_count, expiration_date)


@main.group(cls=LazyGroup, import_name='ai.backend.manager.cli.dbschema:cli')
def schema():
    '''Command set for managing the database schema.'''


@main.group(cls=LazyGroup, import_name='ai.backend.manager.cli.etcd:cli')
def etcd():
    '''Command set for putting/getting data to/from etcd.'''


@main.group(cls=LazyGroup, import_name='ai.backend.manager.cli.fixture:cli')
def fixture():
    '''Command set for managing fixtures.'''


@main.group(cls=LazyGroup, import_name='ai.backend.manager.cli.gql:cli')
def gql():
    '''Command set for GraphQL schema.'''


@main.group(cls=LazyGroup, import_name='ai.backend.manager.cli.image:cli')
def image():
    '''Command set for managing images.'''


if __name__ == '__main__':
    main()
