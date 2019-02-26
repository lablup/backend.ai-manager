import asyncio
import contextlib
import logging
from pathlib import Path
from pprint import pprint
import sys

import aioredis

from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.logging import BraceStyleAdapter

from . import register_command
from ...gateway.defs import REDIS_IMAGE_DB
from ...gateway.etcd import ConfigServer

log = BraceStyleAdapter(logging.getLogger(__name__))


@register_command
def etcd(args):
    '''Provides commands to manage etcd-based Backend.AI cluster configs
    and a simple etcd client functionality'''
    pass


@contextlib.contextmanager
def etcd_ctx(args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    creds = None
    if args.etcd_user:
        creds = {
            'user': args.etcd_user,
            'password': args.etcd_password,
        }
    etcd = AsyncEtcd(args.etcd_addr, args.namespace, credentials=creds)
    with contextlib.closing(loop):
        yield loop, etcd
    asyncio.set_event_loop(None)


@contextlib.contextmanager
def config_ctx(args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ctx = {}
    ctx['config'] = args
    ctx['redis_image'] = loop.run_until_complete(aioredis.create_redis(
        args.redis_addr.as_sockaddr(),
        password=args.redis_password if args.redis_password else None,
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB))
    config_server = ConfigServer(
        ctx, args.etcd_addr,
        args.etcd_user, args.etcd_password,
        args.namespace)
    with contextlib.closing(loop):
        try:
            yield loop, config_server
        finally:
            ctx['redis_image'].close()
            loop.run_until_complete(ctx['redis_image'].wait_closed())
    asyncio.set_event_loop(None)


@etcd.register_command
def put(args):
    '''Set the value of a key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        loop.run_until_complete(etcd.put(args.key, args.value))


put.add('key', type=str, help='The key.')
put.add('value', type=str, help='The value.')


@etcd.register_command
def get(args):
    '''Get the value of a key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        if args.prefix:
            val = loop.run_until_complete(etcd.get_prefix(args.key))
            for x in val:
                print(x)
        else:
            val = loop.run_until_complete(etcd.get(args.key))
            if val is None:
                sys.exit(1)
            print(val)


get.add('key', type=str, help='The key.')
get.add('--prefix', action='store_true', default=False, help='get key prefixed.')


@etcd.register_command
def delete(args):
    '''Delete the key in the configured etcd namespace.'''
    with etcd_ctx(args) as (loop, etcd):
        if args.prefix:
            loop.run_until_complete(etcd.delete_prefix(args.key))
        else:
            loop.run_until_complete(etcd.delete(args.key))


delete.add('key', type=str, help='The key.')
delete.add('--prefix', action='store_true', default=False,
           help='Delete all keys prefixed with the given key.')


@etcd.register_command
def list_images(args):
    '''List everything about images.'''
    with config_ctx(args) as (loop, config_server):
        try:
            items = loop.run_until_complete(
                config_server.list_images())
            pprint(items)
        except Exception:
            log.exception('An error occurred.')


@etcd.register_command
def inspect_image(args):
    '''List everything about images.'''
    with config_ctx(args) as (loop, config_server):
        try:
            item = loop.run_until_complete(
                config_server.inspect_image(args.reference))
            pprint(item)
        except Exception:
            log.exception('An error occurred.')


inspect_image.add('reference', type=str, help='The reference to an image.')


@etcd.register_command
def set_image_resource_limit(args):
    '''List everything about images.'''
    with config_ctx(args) as (loop, config_server):
        try:
            loop.run_until_complete(
                config_server.set_image_resource_limit(
                    args.reference, args.slot_type, args.value, args.min))
        except Exception:
            log.exception('An error occurred.')


set_image_resource_limit.add('--min', action='store_true', default=False,
                             help='Set the minimum value instead of the maximum.')
set_image_resource_limit.add('reference', type=str,
                             help='The reference to an image.')
set_image_resource_limit.add('slot_type', type=str,
                             help='The resource slot name.')
set_image_resource_limit.add('value', type=str,
                             help='The maximum value allowed.')


@etcd.register_command
def rescan_images(args):
    '''Update the kernel image metadata from all configured docker registries.'''
    with config_ctx(args) as (loop, config_server):
        try:
            loop.run_until_complete(
                config_server.rescan_images(args.registry))
        except Exception:
            log.exception('An error occurred.')


rescan_images.add('-r', '--registry',
                  type=str, metavar='NAME', default=None,
                  help='The name (usually hostname or "lablup") '
                       'of the Docker registry configured.')


@etcd.register_command
def alias(args):
    '''Add an image alias.'''
    with config_ctx(args) as (loop, config_server):
        loop.run_until_complete(
            config_server.alias(args.alias, args.target))


alias.add('alias', type=str, help='The alias of an image to add.')
alias.add('target', type=str, help='The target image.')


@etcd.register_command
def dealias(args):
    '''Remove an image alias.'''
    with config_ctx(args) as (loop, config_server):
        loop.run_until_complete(
            config_server.dealias(args.alias))


dealias.add('alias', type=str, help='The alias of an image to remove.')


@etcd.register_command
def update_aliases(args):
    '''Update the image aliase list.'''
    with config_ctx(args) as (loop, config_server):
        if not args.file:
            log.error('Please specify the file path using "-f" option.')
            return
        loop.run_until_complete(
            config_server.update_aliases_from_file(args.file))


update_aliases.add('-f', '--file', type=Path, metavar='PATH',
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


update_volumes.add('-f', '--file', type=Path, metavar='PATH',
                   help='A config file to use.')
