import abc
import argparse


class BaseCommand(metaclass=abc.ABCMeta):

    @abc.abstractclassmethod
    def init_argparser(cls, parser: argparse.ArgumentParser) -> None:
        pass

    @abc.abstractmethod
    def execute(self, args: argparse.Namespace) -> None:
        pass
