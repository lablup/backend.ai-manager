from ai.backend.gateway.config import load_config
from ai.backend.gateway.logging import log_args, log_init
from ai.backend.manager import cli

resolved_command_classes = {}


def init_app_args(parser):
    cli.global_argparser = parser

    import ai.backend.manager.cli.fixture   # noqa
    import ai.backend.manager.cli.dbschema  # noqa
    import ai.backend.manager.cli.shell     # noqa
    import ai.backend.manager.cli.etcd      # noqa


config = load_config(extra_args_funcs=(init_app_args, log_args))
log_init(config)
config.function(config)
