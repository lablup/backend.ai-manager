import asyncio
import contextlib
from ipaddress import ip_address
import logging
from pathlib import Path

from . import register_command
from ...common.argparse import host_port_pair, HostPortPair
from ...gateway.etcd import ConfigServer

log = logging.getLogger(__name__)


@register_command
def etcd(args):
    '''Provides commands to manage etcd-based Backend.AI cluster configs
    and a simple etcd client functionality'''
    pass


etcd.add_argument('--etcd-addr', env_var='BACKEND_ETCD_ADDR',
                  type=host_port_pair, metavar='HOST:PORT',
                  default=HostPortPair(ip_address('127.0.0.1'), 2379),
                  help='The address of etcd server.')
etcd.add_argument('--namespace', env_var='BACKEND_NAMESPACE',
                  type=str, default='local',
                  help='The namespace of this Backend.AI cluster.')


@contextlib.contextmanager
def etcd_ctx(args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    config_server = ConfigServer(args.etcd_addr, args.namespace)
    with contextlib.closing(loop):
        yield loop, config_server.etcd
    asyncio.set_event_loop(None)


@contextlib.contextmanager
def config_ctx(args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    config_server = ConfigServer(args.etcd_addr, args.namespace)
    with contextlib.closing(loop):
        yield loop, config_server
    asyncio.set_event_loop(None)


@etcd.register_command
def put(args):
    '''Set the value of a key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        loop.run_until_complete(etcd.put(args.key, args.value))


put.add_argument('key', type=str, help='The key.')
put.add_argument('value', type=str, help='The value.')


@etcd.register_command
def get(args):
    '''Get the value of a key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        val = loop.run_until_complete(etcd.get(args.key))
        print(val)


get.add_argument('key', type=str, help='The key.')


@etcd.register_command
def delete(args):
    '''Delete the key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        if args.prefix:
            loop.run_until_complete(etcd.delete_prefix(args.key))
        else:
            loop.run_until_complete(etcd.delete(args.key))


delete.add_argument('key', type=str, help='The key.')
delete.add_argument('--prefix', action='store_true', default=False,
                    help='Delete all keys prefixed with the given key.')


@etcd.register_command
def update_images(args):
    '''Update the latest version of kernels (Docker images)
    that Backend.AI agents will use.'''
    with config_ctx(args) as (loop, config_server):
        if args.file:
            loop.run_until_complete(
                config_server.update_kernel_images_from_file(args.file))
        elif args.scan_docker_hub:
            loop.run_until_complete(
                config_server.update_kernel_images_from_registry(args.registry_addr))
        else:
            log.error('Please specify one of the options. See "--help".')


update_images.add_argument('-f', '--file', type=Path, metavar='PATH',
                           help='A config file to use.')
update_images.add_argument('--scan-registry', default=False, action='store_true',
                           help='Scan the Docker hub to get the latest versinos.')
update_images.add_argument('--docker-registry', env_var='BACKEND_DOCKER_REGISTRY',
                           type=str, metavar='URL', default=None,
                           help='The address of Docker registry server.')


@etcd.register_command
def update_aliases(args):
    '''Update the image aliase list.'''
    with config_ctx(args) as (loop, config_server):
        if not args.file:
            log.error('Please specify the file path using "-f" option.')
            return
        loop.run_until_complete(
            config_server.update_aliases_from_file(args.file))


update_aliases.add_argument('-f', '--file', type=Path, metavar='PATH',
                            help='A config file to use.')


@etcd.register_command
def update_volumes(args):
    '''Update the volume information.'''
    with config_ctx(args) as (loop, config_server):
        if not args.file:
            log.error('Please specify the file path using "-f" option.')
            return
        loop.run_until_complete(
            config_server.update_volumes_from_file(args.file))


update_volumes.add_argument('-f', '--file', type=Path, metavar='PATH',
                            help='A config file to use.')
