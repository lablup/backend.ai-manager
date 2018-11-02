import argparse
import functools
from typing import Callable, Optional, Union

import configargparse

ArgParserType = Union[argparse.ArgumentParser, configargparse.ArgumentParser]

fallback_global_argparser = configargparse.ArgumentParser()
_subparsers = dict()


def register_command(handler: Callable[[argparse.Namespace], None],
                     outer_parser: Optional[ArgParserType] = None) \
                     -> Callable[[argparse.Namespace], None]:
    if outer_parser is None:
        outer_parser = fallback_global_argparser
    if id(outer_parser) not in _subparsers:
        subparsers = outer_parser.add_subparsers(title='commands',
                                                 dest='command')
        _subparsers[id(outer_parser)] = subparsers
    else:
        subparsers = _subparsers[id(outer_parser)]

    @functools.wraps(handler)
    def wrapped(args):
        handler(args)

    doc_summary = handler.__doc__.split('\n\n')[0]
    inner_parser = subparsers.add_parser(handler.__name__.replace('_', '-'),
                                         description=handler.__doc__,
                                         help=doc_summary)
    inner_parser.set_defaults(function=wrapped)
    wrapped.register_command = functools.partial(register_command,
                                                 outer_parser=inner_parser)
    wrapped.add_argument = inner_parser.add_argument
    wrapped.add = inner_parser.add
    return wrapped
