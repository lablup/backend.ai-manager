import abc
import argparse
from typing import Union

import configargparse

ArgumentParser = Union[argparse.ArgumentParser, configargparse.ArgumentParser]


class BaseCommand(metaclass=abc.ABCMeta):

    @abc.abstractclassmethod
    def init_argparser(cls, parser: ArgumentParser) -> None:
        pass

    @abc.abstractmethod
    def execute(self, args: argparse.Namespace) -> None:
        pass
