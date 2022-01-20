from __future__ import annotations

import asyncio
import configparser
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
import tomlkit
from ai.backend.common.cli import LazyGroup
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import TimeDuration
from ai.backend.cli.interaction import ask_host, ask_number
from ai.backend.cli.interaction import ask_string, ask_string_in_array, ask_file_path
from setproctitle import setproctitle
from tomlkit.items import Table, InlineTable

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
    with open("config/sample.toml", "r") as f:
        config_toml: dict = dict(tomlkit.loads(f.read()))
    # Interactive user input
    # etcd section
    try:
        if config_toml.get("etcd") is None:
            raise KeyError
        elif type(config_toml.get("etcd")) != Table:
            raise TypeError
        etcd_config: dict = dict(config_toml["etcd"])
        while True:
            try:
                if etcd_config.get("addr") is None:
                    raise KeyError
                elif type(etcd_config.get("addr")) != InlineTable:
                    raise TypeError
                etcd_address: dict = dict(etcd_config["addr"])
                etcd_host = ask_host("Etcd host: ", etcd_address["host"])
                if type(etcd_address.get("port")) != str:
                    etcd_port = ask_number("Etcd port: ", int(etcd_address["port"]), 1, 65535)
                else:
                    raise TypeError
                if check_etcd_health(etcd_host, etcd_port):
                    break
                print("Cannot connect to etcd. Please input etcd information again.")
            except ValueError:
                print("Invalid etcd address sample.")

        etcd_user = ask_string("Etcd user name", use_default=False)
        etcd_password = ask_string("Etcd password", use_default=False)
        config_toml['etcd']['addr'] = {"host": etcd_host, "port": etcd_port}
        config_toml['etcd']['user'] = etcd_user
        config_toml['etcd']['password'] = etcd_password
    except ValueError:
        raise ValueError

    # db section
    try:
        if config_toml.get("db") is None:
            raise KeyError
        elif type(config_toml.get("db")) != Table:
            raise TypeError
        database_config: dict = dict(config_toml["db"])
        while True:
            try:
                if database_config.get("addr") is None:
                    raise KeyError
                elif type(database_config.get("addr")) != InlineTable:
                    raise TypeError
                database_address: dict = dict(database_config["addr"])
                database_host = ask_host("Database host: ", str(database_address.get("host")))
                if type(database_address.get("port")) != str:
                    database_port = ask_number("Database port: ",
                                               int(database_address["port"]), 1, 65535)
                else:
                    raise TypeError
                database_name = ask_string("Database name", str(database_config.get("name")))
                database_user = ask_string("Database user", str(database_config.get("user")))
                database_password = ask_string("Database password", use_default=False)
                if check_database_health(
                    database_host,
                    database_port,
                    database_name,
                    database_user,
                    database_password,
                ):
                    config_toml['db']['addr'] = {"host": database_address, "port": database_port}
                    config_toml['db']['name'] = database_name
                    config_toml['db']['user'] = database_user
                    config_toml['db']['password'] = database_password
                    break
            except ValueError:
                raise ValueError
    except ValueError:
        raise ValueError

    # manager section
    try:
        if config_toml.get("manager") is None:
            raise KeyError
        elif type(config_toml.get("manager")) != Table:
            raise TypeError
        manager_config: dict = dict(config_toml["manager"])
        cpu_count: Optional[int] = os.cpu_count()
        if cpu_count:
            no_of_processors: int = ask_number("How many processors that manager uses: ", 1, 1,
                                               cpu_count)
            config_toml["manager"]["num-proc"] = no_of_processors

        secret_token: str = ask_string("Secret token", use_default=False)
        if secret_token:
            config_toml["manager"]["secret"] = secret_token
        else:
            config_toml["manager"].pop("secret")

        daemon_user: str = ask_string("User name used for the manager daemon", use_default=False)
        daemon_group: str = ask_string("Group name used for the manager daemon", use_default=False)
        if daemon_user:
            config_toml["manager"]["user"] = daemon_user
        else:
            config_toml["manager"].pop("user")
        if daemon_group:
            config_toml["manager"]["group"] = daemon_group
        else:
            config_toml["manager"].pop("group")

        try:
            if manager_config.get("service-addr") is None:
                raise KeyError
            elif type(manager_config.get("service-addr")) != InlineTable:
                raise TypeError
            manager_address: dict = dict(manager_config["service-addr"])
            manager_host = ask_host("Manager host: ", manager_address["host"])
            if type(manager_address.get("port")) != str:
                manager_port = ask_number("Manager port: ", int(manager_address["port"]), 1, 65535)
            else:
                raise TypeError
            config_toml["manager"]["service-addr"] = {"host": manager_host, "port": manager_port}
        except ValueError:
            raise ValueError

        ssl_enabled = ask_string_in_array("Enable SSL", choices=["true", "false"])
        config_toml["manager"]["ssl-enabled"] = ssl_enabled == "true"

        if ssl_enabled == "true":
            ssl_cert = ask_file_path("SSL cert path")
            ssl_private_key = ask_file_path("SSL private key path")
            config_toml["manager"]["ssl-cert"] = ssl_cert
            config_toml["manager"]["ssl-privkey"] = ssl_private_key

        while True:
            try:
                heartbeat_timeout = float(input("Heartbeat timeout: "))
                config_toml["manager"]["heartbeat-timeout"] = heartbeat_timeout
                break
            except ValueError:
                print("Please input correct heartbeat timeout value as float.")

        node_name = ask_string("Manager node name", use_default=False)
        if node_name:
            config_toml["manager"]["id"] = node_name

        pid_path = ask_file_path("PID file path")
        if pid_path:
            config_toml["manager"]["pid-file"] = pid_path

        hide_agent = ask_string_in_array("Hide agent and container ID", choices=["true", "false"])
        config_toml["manager"]["hide-agents"] = hide_agent == "true"

        event_loop = ask_string_in_array("Event loop", choices=["asyncio", "uvloop"])
        config_toml["manager"]["event-loop"] = event_loop
    except ValueError:
        raise ValueError

    with open("manager.toml", "w") as f:
        print("\nDump to manager.toml\n")
        tomlkit.dump(config_toml, f)

    # Dump alembic.ini
    config_parser = configparser.ConfigParser()
    config_parser.read("config/halfstack.alembic.template.ini")

    script_location = ask_string("Script location: ", )
    config_parser["alembic"]["script_location"] = script_location

    file_template = ask_string("File template: ", use_default=False)
    if file_template:
        config_parser["alembic"]["file_template"] = file_template
    else:
        config_parser["alembic"].pop("file_template")

    timezone = ask_string("Timezone: ", use_default=False)
    if file_template:
        config_parser["alembic"]["timezone"] = timezone
    else:
        config_parser["alembic"].pop("timezone")

    truncate_slug_length: int = ask_number("Max length of slug field(If you don\'t want to use, "
                                           "just leave default value): ", 0, 0, 40)
    if truncate_slug_length > 0:
        config_parser["alembic"]["truncate_slug_length"] = str(truncate_slug_length)
    else:
        config_parser["alembic"].pop("truncate_slug_length")

    revision_environment = ask_string_in_array("Revision Environment", choices=["true", "false", ""])
    if revision_environment:
        config_parser["alembic"]["revision_environment"] = revision_environment == "true"
    else:
        config_parser["alembic"].pop("revision_environment")

    sourceless = ask_string_in_array("Sourceless(set to 'true' to allow .pyc and .pyo files "
                                     "without a source .py)", choices=["true", "false", ""])
    if sourceless:
        config_parser["alembic"]["sourceless"] = sourceless == "true"
    else:
        config_parser["alembic"].pop("sourceless")

    version_locations = ask_string("Version locations: ", use_default=False)
    if version_locations:
        config_parser["alembic"]["version_locations"] = version_locations
    else:
        config_parser["alembic"].pop("version_locations")

    # modify database scheme
    if all([x is not None for x in
            [database_user, database_password, database_name, database_host, database_port]]):
        config_parser["alembic"]["sqlalchemy.url"] = \
            f"postgresql://{database_user}:{database_password}" \
            f"@{database_host}:{database_port}/{database_name}"

    logger_keys = ask_string("Logger keys: ", default=config_parser["loggers"]["keys"])
    config_parser["loggers"]["keys"] = logger_keys
    handlers_keys = ask_string("Handlers keys: ", default=config_parser["handlers"]["keys"])
    config_parser["handlers"]["keys"] = handlers_keys
    formatters_keys = ask_string("Formatters keys: ", default=config_parser["formatters"]["keys"])
    config_parser["formatters"]["keys"] = formatters_keys

    logger_root_level = ask_string("Logger root level", default=config_parser["logger_root"]["level"])
    logger_root_handlers = ask_string("Logger root handlers",
                                      default=config_parser["logger_root"]["handlers"])
    logger_root_qualname = ask_string("Logger root qualname",
                                      default=config_parser["logger_root"]["qualname"])
    config_parser["logger_root"]["level"] = logger_root_level
    config_parser["logger_root"]["handlers"] = logger_root_handlers
    config_parser["logger_root"]["qualname"] = logger_root_qualname

    logger_sqlalchemy_level = ask_string("Logger sqlalchemy level",
                                         default=config_parser["logger_sqlalchemy"]["level"])
    logger_sqlalchemy_handlers = ask_string("Logger sqlalchemy handlers",
                                            default=config_parser["logger_sqlalchemy"]["handlers"])
    logger_sqlalchemy_qualname = ask_string("Logger sqlalchemy qualname",
                                            default=config_parser["logger_sqlalchemy"]["qualname"])
    config_parser["logger_sqlalchemy"]["level"] = logger_sqlalchemy_level
    config_parser["logger_sqlalchemy"]["handlers"] = logger_sqlalchemy_handlers
    config_parser["logger_sqlalchemy"]["qualname"] = logger_sqlalchemy_qualname

    logger_alembic_level = ask_string("Logger alembic level",
                                      default=config_parser["logger_alembic"]["level"])
    logger_alembic_handlers = ask_string("Logger alembic handlers",
                                         default=config_parser["logger_alembic"]["handlers"])
    logger_alembic_qualname = ask_string("Logger alembic qualname",
                                         default=config_parser["logger_alembic"]["qualname"])
    config_parser["logger_alembic"]["level"] = logger_alembic_level
    config_parser["logger_alembic"]["handlers"] = logger_alembic_handlers
    config_parser["logger_alembic"]["qualname"] = logger_alembic_qualname

    handler_console_class = ask_string("Handler console class",
                                       default=config_parser["handler_console"]["class"])
    handler_console_args = ask_string("Handler console args",
                                      default=config_parser["handler_console"]["args"])
    handler_console_level = ask_string("Handler console level",
                                       default=config_parser["handler_console"]["level"])
    handler_console_formatter = ask_string("Handler console formatter",
                                           default=config_parser["handler_console"]["formatter"])
    config_parser["handler_console"]["class"] = handler_console_class
    config_parser["handler_console"]["args"] = handler_console_args
    config_parser["handler_console"]["level"] = handler_console_level
    config_parser["handler_console"]["formatter"] = handler_console_formatter

    formatter_generic_format = ask_string("Formatter generic format",
                                          default=config_parser["formatter_generic"]["format"])
    formatter_generic_datefmt = ask_string("Formatter generic datefmt",
                                           default=config_parser["formatter_generic"]["datefmt"])
    config_parser["formatter_generic"]["format"] = formatter_generic_format
    config_parser["formatter_generic"]["datefmt"] = formatter_generic_datefmt

    with open("alembic.ini", 'w') as f:
        print("\nDump to alembic.ini\n")
        config_parser.write(f)

    # Dump etcd config json
    with open("config/sample.etcd.config.json") as f:
        config_json: dict = json.load(f)

    try:
        if config_json.get("redis") is None:
            raise KeyError
        elif type(config_json.get("redis")) != dict:
            raise TypeError
        redis_config: dict = dict(config_json["redis"])
        while True:
            redis_host_str, redis_port_str = str(redis_config.get("addr")).split(":")
            redis_host = ask_host("Redis host: ", str(redis_host_str))
            if type(redis_port_str) != str:
                raise TypeError
            redis_port = ask_number("Redis port: ", int(redis_port_str), 1, 65535)
            redis_password = ask_string("Redis password", use_default=False)
            if redis_password:
                redis_client = aioredis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password)
            else:
                redis_client = aioredis.Redis(host=redis_host, port=redis_port)

            try:
                loop = asyncio.get_event_loop()
                coroutine = redis_client.get("")
                loop.run_until_complete(coroutine)
                coroutine = redis_client.close()
                loop.run_until_complete(coroutine)
                config_json["redis"]["addr"] = f"{redis_host}:{redis_port}"
                config_json["redis"]["password"] = redis_password
                break
            except (aioredis.exceptions.ConnectionError, aioredis.exceptions.BusyLoadingError):
                print("Cannot connect to etcd. Please input etcd information again.")

        while True:
            timezone = input("System timezone: ")
            try:
                _ = ZoneInfo(timezone)
                config_json["system"]["timezone"] = timezone
                break
            except (ValueError, ZoneInfoNotFoundError):
                print('Please input correct timezone.')
    except ValueError:
        raise ValueError

    with open("dev.etcd.config.json", "w") as f:
        print("\nDump to dev.etcd.config.json\n")
        json.dump(config_json, f, indent=4)

    print("Complete configure backend.ai manager. "
          "If you want to control more value, edit following files.\n")
    print("manager.toml : etcd, database, manager configuration, logging options and so on.")
    print("alembic.ini : option about alembic")
    print("dev.etcd.config.json : etcd options like timezone, host, port and so on.")


def check_etcd_health(host: str, port: int):
    try:
        etcd_client = etcd3.Etcd3Client(host=host, port=port)
        etcd_client.close()
    except (etcd3.exceptions.ConnectionFailedError, etcd3.exceptions.ConnectionTimeoutError):
        return False
    return True


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
