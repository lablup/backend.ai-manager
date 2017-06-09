import argparse
import importlib
import sys

from sorna.gateway.config import load_config, init_logger


resolved_command_classes = {}

def init_app_args(parser):
    commands = {
        'fixture': 'sorna.manager.cli.fixture.FixtureCommand',
    }
    subparsers = parser.add_subparsers(title='commands', dest='command')
    for name, import_path in commands.items():
        modname, clsname = import_path.rsplit('.', 1)
        mod = importlib.import_module(modname)
        cls = getattr(mod, clsname)
        resolved_command_classes[name] = cls
        subparser = subparsers.add_parser(name, help=cls.__doc__)
        cls.init_argparser(subparser)


config = load_config(extra_args_func=init_app_args)
init_logger(config)

app = resolved_command_classes[config.command]()
app.execute(config)
