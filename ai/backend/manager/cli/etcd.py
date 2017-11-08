import asyncio
from ipaddress import ip_address
import logging
from pathlib import Path

from . import register_command
from ...common.argparse import host_port_pair, HostPortPair
from ...gateway.etcd import ConfigServer

log = logging.getLogger(__name__)


def _add_common_args(func):
    func.add_argument('--etcd-addr', env_var='BACKEND_ETCD_ADDR',
                      type=host_port_pair, metavar='HOST:PORT',
                      default=HostPortPair(ip_address('127.0.0.1'), 2379),
                      help='The address of etcd server.')
    func.add_argument('--namespace', env_var='BACKEND_NAMESPACE',
                      type=str, default='local',
                      help='The namespace of this Backend.AI cluster.')


@register_command
def etcd(args):
    '''Provides commands to manage etcd-based Backend.AI cluster configs.'''
    pass


@etcd.register_command
def update_images(args):
    '''Update the latest version of kernels (Docker images)
    that Backend.AI agents will use.'''
    loop = asyncio.get_event_loop()
    config_server = ConfigServer(args.etcd_addr, args.namespace)
    try:
        if args.file:
            loop.run_until_complete(
                config_server.update_kernel_images_from_file(args.file))
        elif args.scan_docker_hub:
            loop.run_until_complete(
                config_server.update_kernel_images_from_registry(args.registry_addr))
        else:
            log.error('Please specify one of the options. See "--help".')
    finally:
        loop.close()


update_images.add_argument('-f', '--file', type=Path, metavar='PATH',
                           help='A config file to use.')
update_images.add_argument('--scan-registry', default=False, action='store_true',
                           help='Scan the Docker hub to get the latest versinos.')
update_images.add_argument('--docker-registry', env_var='BACKEND_DOCKER_REGISTRY',
                           type=str, metavar='URL', default=None,
                           help='The address of Docker registry server.')
_add_common_args(update_images)


@etcd.register_command
def update_aliases(args):
    '''Update the image aliase list.'''
    loop = asyncio.get_event_loop()
    config_server = ConfigServer(args.etcd_addr, args.namespace)
    try:
        if not args.file:
            log.error('Please specify the file path using "-f" option.')
            return
        loop.run_until_complete(
            config_server.update_aliases_from_file(args.file))
    finally:
        loop.close()


update_aliases.add_argument('-f', '--file', type=Path, metavar='PATH',
                            help='A config file to use.')
_add_common_args(update_aliases)
