import atexit
import logging
import os
from setproctitle import setproctitle
import subprocess
import sys
from typing import Any, Mapping
from pathlib import Path

import attr
import click

from ai.backend.common.cli import LazyGroup
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.gateway.config import load as load_config

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.cli'))


@attr.s(auto_attribs=True, frozen=True)
class CLIContext:
    logger: Logger
    config: Mapping[str, Any]


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./manager.conf and /etc/backend.ai/manager.conf)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(ctx, config_path, debug):
    cfg = load_config(config_path)
    setproctitle(f"backend.ai: manager.cli {cfg['etcd']['namespace']}")
    if 'file' in cfg['logging']['drivers']:
        cfg['logging']['drivers'].remove('file')
    # log_endpoint = f'tcp://127.0.0.1:{find_free_port()}'
    log_sockpath = Path(f'/tmp/backend.ai/ipc/manager-cli-{os.getpid()}.sock')
    log_sockpath.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{log_sockpath}'
    logger = Logger(cfg['logging'], is_master=True, log_endpoint=log_endpoint)
    ctx.obj = CLIContext(
        logger=logger,
        config=cfg,
    )

    def _clean_logger():
        try:
            os.unlink(log_sockpath)
        except FileNotFoundError:
            pass

    atexit.register(_clean_logger)


@main.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.option('-c', '--container-name', type=str, default=None,
              help='Open a postgres client shell using the psql executable '
                   'shipped with the given postgres container. '
                   'If set "-", it will use the host-provided psql executable. '
                   'You may append additional arguments for the psql cli command. '
                   '[default: auto-detect from halfstack]')
@click.option('--psql-help', is_flag=True,
              help='Show the help text of the psql command.')
@click.argument('psql_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def dbshell(cli_ctx, container_name, psql_help, psql_args):
    '''Run the database shell.'''
    config = cli_ctx.config
    if psql_help:
        psql_args = ['--help']
    if container_name is None:
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
            (f"postgres://{config['db']['user']}:{config['db']['password']}"
             f"@{config['db']['addr']}/{config['db']['name']}"),
            *psql_args,
        ]
        subprocess.call(cmd)
        return
    # Use the container to start the psql client command
    cmd = [
        'docker', 'exec', '-i', '-t',
        '-e', f"PGPASSWORD={config['db']['password']}",
        container_name,
        'psql',
        '-U', config['db']['user'],
        '-d', config['db']['name'],
        *psql_args,
    ]
    subprocess.call(cmd)


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
