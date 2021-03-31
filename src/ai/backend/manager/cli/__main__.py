from __future__ import annotations

import logging
from setproctitle import setproctitle
import subprocess
import sys
from pathlib import Path

import click

from ai.backend.common.cli import LazyGroup
from ai.backend.common.logging import BraceStyleAdapter

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
