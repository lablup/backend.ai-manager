import argparse
import importlib
import logging
import sys

from sorna.gateway.config import load_config, init_logger
from sorna.manager import cli


resolved_command_classes = {}

def init_app_args(parser):
    cli.global_argparser = parser

    import sorna.manager.cli.fixture
    import sorna.manager.cli.dbschema


config = load_config(extra_args_func=init_app_args)
init_logger(config)
config.function(config)
