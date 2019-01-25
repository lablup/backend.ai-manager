import logging
from setproctitle import setproctitle

from ai.backend.gateway.config import load_config
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.manager import cli

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.cli'))


def init_app_args(parser):
    cli.fallback_global_argparser = parser

    import ai.backend.manager.cli.fixture   # noqa
    import ai.backend.manager.cli.dbschema  # noqa
    import ai.backend.manager.cli.shell     # noqa
    import ai.backend.manager.cli.etcd      # noqa


def main():
    config = load_config(extra_args_funcs=(init_app_args, Logger.update_log_args))
    setproctitle(f'backend.ai: manager.cli {config.namespace}')
    logger = Logger(config)
    logger.add_pkg('aiopg')
    logger.add_pkg('ai.backend')
    with logger:
        log_config = logging.getLogger('ai.backend.gateway.config')
        log_config.debug('debug mode enabled.')
        config.function(config)


if __name__ == '__main__':
    main()
