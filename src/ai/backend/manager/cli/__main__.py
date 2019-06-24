import logging
from setproctitle import setproctitle
import subprocess
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
    logger = Logger(cfg['logging'])
    ctx.obj = CLIContext(
        logger=logger,
        config=cfg,
    )


@main.command()
@click.option('-d', '--dockerize', is_flag=True,
              help='Assume dockerized db instance. It creates a '
                   'temporary pgsql shell container. [default: false]')
@click.option('--docker-network', default='backend_ai_default',
              help='The network name to attach the shell container. '
                   '(used only with --dockerize) '
                   '[default: backend_ai_default]')
@click.option('--docker-dbaddr', default='backendai-db',
              help='The address of the database host in the container. '
                   '(used only with --dockerize) [default: backendai-db]')
@click.pass_obj
def dbshell(cli_ctx, dockerize, docker_network, docker_dbaddr):
    '''Run the database shell.'''
    config = cli_ctx.config
    if dockerize:
        cmd = [
            'docker', 'run', '--rm', '-i', '-t',
            '--network', docker_network,
            'postgres:9.6-alpine',
            'psql',
            (f"postgres://{config['db']['user']}:{config['db']['password']}"
             f"@{docker_dbaddr}/{config['db']['name']}"),
        ]
    else:
        cmd = [
            'psql',
            (f"postgres://{config['db']['user']}:{config['db']['password']}"
             f"@{config['db']['addr']}/{config['db']['name']}"),
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


if __name__ == '__main__':
    main()
