import argparse
import functools
from typing import Callable, Optional, Union

import configargparse

ArgParserType = Union[argparse.ArgumentParser, configargparse.ArgumentParser]

global_argparser = configargparse.ArgumentParser()
_subparsers = dict()


def register_command(handler: Callable[[argparse.Namespace], None],
                     main_parser: Optional[ArgParserType]=None) \
                     -> Callable[[argparse.Namespace], None]:
    if main_parser is None:
        main_parser = global_argparser
    if id(main_parser) not in _subparsers:
        subparsers = main_parser.add_subparsers(title='commands',
                                                dest='command')
        _subparsers[id(main_parser)] = subparsers
    else:
        subparsers = _subparsers[id(main_parser)]

    @functools.wraps(handler)
    def wrapped(args):
        handler(args)

    doc_summary = handler.__doc__.split('\n\n')[0]
    inner_parser = subparsers.add_parser(handler.__name__.replace('_', '-'),
                                         description=handler.__doc__,
                                         help=doc_summary)
    inner_parser.set_defaults(function=wrapped)
    wrapped.register_command = functools.partial(register_command,
                                                 main_parser=inner_parser)
    wrapped.add_argument = inner_parser.add_argument
    return wrapped
