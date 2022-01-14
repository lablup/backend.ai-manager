from __future__ import annotations

import asyncio
import configparser
import ipaddress
import json.decoder
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aioredis
import click
import etcd3
import psycopg2
import tomlkit as tomlkit
from ai.backend.common.cli import LazyGroup
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import TimeDuration
from setproctitle import setproctitle

from .context import CLIContext, init_logger
from ..config import load as load_config
from ..models.keypair import generate_keypair as _gen_keypair

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
@click.pass_obj
def clear_history(cli_ctx: CLIContext, retention, vacuum_full) -> None:
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


@main.command()
def configure() -> None:
    """
    Take necessary inputs from user and generate toml file.
    """
    # toml section
    with open('config/sample.toml', 'r') as f:
        config = tomlkit.loads(f.read())
    # Interactive user input
    # etcd section
    while True:
        while True:
            etcd_address = input('Input etcd host: ')
            if validate_ip(etcd_address):
                break
            print('Please input correct etcd IP address.')

        while True:
            try:
                etcd_port = int(input('Input etcd port: '))
                if 1 <= etcd_port <= 65535:
                    break
            except ValueError:
                print('Please input correct etcd port.')

        etcd_user = input('Input etcd user name: ')
        etcd_password = input('Input etcd password: ')

        if check_etcd_health(etcd_address, etcd_port):
            break
        print('Cannot connect to etcd. Please input etcd information again.')

    # db section
    while True:
        while True:
            database_address = input('Input database host: ')
            if validate_ip(database_address):
                break
            print('Please input correct database IP address.')

        while True:
            try:
                database_port = int(input('Input database port: '))
                if 1 <= database_port <= 65535:
                    break
            except ValueError:
                print('Please input correct database port.')

        database_name = input('Input database name: ')
        database_user = input('Input database user: ')
        database_password = input('Input database password: ')

        if check_database_health(
            database_address,
            database_port,
            database_name,
            database_user,
            database_password,
        ):
            break

    # manager section
    while True:
        cpu_count: Optional[int] = os.cpu_count()
        if cpu_count:
            try:
                no_of_processors = int(input(f'Input cpu count how many manager uses(1~{cpu_count}): '))
                if 1 <= no_of_processors <= cpu_count:
                    no_of_processors_integer = int(no_of_processors)
                    break
            except ValueError:
                print('Please input correct cpu count.')
        else:
            print('Can\'t detect number of cpu.\n')
            return
    secret_token = input('Input secret token, if you don\'t want, just leave empty: ').strip()
    daemon_user = input(
        'Input user name used for the manager daemon, if you don\'t want, just leave empty: ').strip()
    daemon_group = input(
        'Input group name used for the manager daemon, if you don\'t want, just leave empty: ').strip()

    while True:
        manager_address = input('Input manager host: ')
        if validate_ip(manager_address):
            break
        print('Please input correct manager IP address.')

    while True:
        try:
            manager_port = int(input('Input manager port: '))
            if 1 <= manager_port <= 65535:
                break
        except ValueError:
            print('Please input correct manager port.')

    while True:
        ssl_enabled_input = input('Input value used for enable ssl(True/False): ')
        if ssl_enabled_input.lower() == 'true':
            ssl_enabled = True
            break
        elif ssl_enabled_input.lower() == 'false':
            ssl_enabled = False
            break

    ssl_cert = None
    ssl_private_key = None
    if ssl_enabled:
        while True:
            ssl_cert = input('Input ssl cert path: ')
            if os.path.exists(ssl_cert):
                break
            print('Please input correct ssl certificate path.')
        while True:
            ssl_private_key = input('Input ssl private key path: ')
            if os.path.exists(ssl_private_key):
                break
            print('Please input correct ssl private key path.')

    while True:
        try:
            heartbeat_timeout = float(input('Input heartbeat timeout: '))
            break
        except ValueError:
            print('Please input correct pid file path.')
    node_name = input('Input manager node name, if you don\'t want, just leave empty: ')

    while True:
        pid_path = input('Input pid file path: ')
        if os.path.exists(pid_path):
            break
        print('Please input correct pid file path.')

    while True:
        hide_agent_input = input('Input value used for hide agent and container ID(True/False): ')
        if hide_agent_input.lower() == 'true':
            hide_agent = True
            break
        elif hide_agent_input.lower() == 'false':
            hide_agent = False
            break

    while True:
        event_loop = input('Input a kind of event loop(asyncio/uvloop): ')
        if event_loop == 'asyncio' or event_loop == 'uvloop':
            break
        print('Please input a kind of event loop between asyncio and uvloop')

    config['etcd']['addr'] = {"host": etcd_address, "port": etcd_port}
    config['etcd']['user'] = etcd_user
    config['etcd']['password'] = etcd_password

    config['db']['addr'] = {"host": database_address, "port": database_port}
    config['db']['name'] = database_name
    config['db']['user'] = database_user
    config['db']['password'] = database_password

    config['manager']['num-proc'] = no_of_processors_integer
    if secret_token:
        config['manager']['secret'] = secret_token
    else:
        config['manager'].pop('secret')
    if daemon_user:
        config['manager']['user'] = daemon_user
    else:
        config['manager'].pop('user')
    if daemon_group:
        config['manager']['group'] = daemon_group
    else:
        config['manager'].pop('group')
    config['manager']['service-addr'] = {"host": manager_address, "port": manager_port}
    config['manager']['ssl-enabled'] = ssl_enabled
    if ssl_enabled:
        config['manager']['ssl-cert'] = ssl_cert
        config['manager']['ssl-privkey'] = ssl_private_key
    config['manager']['heartbeat-timeout'] = heartbeat_timeout
    if node_name:
        config['manager']['id'] = node_name
    if pid_path:
        config['manager']['pid-file'] = pid_path
    config['manager']['hide-agents'] = hide_agent
    config['manager']['event-loop'] = event_loop
    with open('manager.toml', 'w') as f:
        print('\nDump to manager.toml\n')
        tomlkit.dump(config, f)

    # Dump alembic.ini
    config = configparser.ConfigParser()
    config.read('config/halfstack.alembic.ini')
    # modify database scheme
    config['alembic']['sqlalchemy.url'] = f'postgresql://{database_user}:{database_password}@' \
                                          f'{database_address}:{database_port}/{database_name} '
    with open('alembic.ini', 'w') as f:
        print('\nDump to alembic.ini\n')
        config.write(f)

    # Dump etcd config json
    with open('config/sample.etcd.config.json') as f:
        config = json.load(f)

    while True:
        while True:
            redis_address = input('Input redis host: ')
            if validate_ip(redis_address):
                break
            print('Please input correct redis IP address.')

        while True:
            try:
                redis_port = int(input('Input redis port: '))
                if 1 <= redis_port <= 65535:
                    break
            except ValueError:
                print('Please input correct redis port.')
        redis_password = input('Input redis password. If you don\'t want, just leave empty: ')

        if redis_password:
            redis_client = aioredis.Redis(
                host=redis_address,
                port=redis_port,
                password=redis_password)
        else:
            redis_client = aioredis.Redis(host=redis_address, port=redis_port)

        try:
            loop = asyncio.get_event_loop()
            coroutine = redis_client.get("")
            loop.run_until_complete(coroutine)
            break
        except (aioredis.exceptions.ConnectionError, aioredis.exceptions.BusyLoadingError):
            print('Cannot connect to etcd. Please input etcd information again.')

    while True:
        timezone = input('Input system timezone: ')
        try:
            _ = ZoneInfo(timezone)
            break
        except (ValueError, ZoneInfoNotFoundError):
            print('Please input correct timezone.')

    config['system']['timezone'] = timezone
    config['redis']['addr'] = f'{redis_address}:{redis_port}'
    config['redis']['password'] = redis_password

    with open('dev.etcd.config.json', 'w') as f:
        print('\nDump to dev.etcd.config.json\n')
        json.dump(config, f, indent=4)

    print("Complete configure backend.ai manager. "
          "If you want to control more value, edit following files.\n")
    print("manager.toml : etcd, database, manager configuration, logging options and so on.")
    print("alembic.ini : option about alembic")
    print("dev.etcd.config.json : etcd options like timezone, host, port and so on.")


def validate_ip(ip_address: str) -> bool:
    try:
        _ = ipaddress.ip_address(ip_address)
        return True
    except ValueError:
        return False


def check_etcd_health(host: str, port: int):
    try:
        _ = etcd3.Etcd3Client(host=host, port=port)
    except (etcd3.exceptions.ConnectionFailedError, etcd3.exceptions.ConnectionTimeoutError):
        return False


def check_database_health(host: str, port: int, database_name: str, user: str, password: str):
    try:
        database_client = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database_name,
        )
        database_client.close()
        return True
    except Exception as e:
        print(e)
        return False


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


if __name__ == '__main__':
    main()
