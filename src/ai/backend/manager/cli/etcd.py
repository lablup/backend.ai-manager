import asyncio
import contextlib
import io
import json
import logging
from pprint import pprint
import sys

import aioredis
import click
from tabulate import tabulate

from ai.backend.common.cli import EnumChoice, MinMaxRange
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.gateway.config import redis_config_iv
from ai.backend.gateway.defs import REDIS_IMAGE_DB
from ai.backend.gateway.etcd import ConfigServer

log = BraceStyleAdapter(logging.getLogger(__name__))


@click.group()
def cli():
    '''Provides commands to manage etcd-based Backend.AI cluster configs
    and a simple etcd client functionality'''
    pass


@contextlib.contextmanager
def etcd_ctx(cli_ctx):
    config = cli_ctx.config
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    creds = None
    if config['etcd']['user']:
        creds = {
            'user': config['etcd']['user'],
            'password': config['etcd']['password'],
        }
    scope_prefix_map = {
        ConfigScopes.GLOBAL: '',
        # TODO: provide a way to specify other scope prefixes
    }
    etcd = AsyncEtcd(config['etcd']['addr'], config['etcd']['namespace'],
                     scope_prefix_map, credentials=creds)
    with contextlib.closing(loop):
        yield loop, etcd
    asyncio.set_event_loop(None)


@contextlib.contextmanager
def config_ctx(cli_ctx):
    config = cli_ctx.config
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ctx = {}
    ctx['config'] = config
    # scope_prefix_map is created inside ConfigServer
    config_server = ConfigServer(
        ctx, config['etcd']['addr'],
        config['etcd']['user'], config['etcd']['password'],
        config['etcd']['namespace'])
    raw_redis_config = loop.run_until_complete(config_server.etcd.get_prefix('config/redis'))
    config['redis'] = redis_config_iv.check(raw_redis_config)
    ctx['redis_image'] = loop.run_until_complete(aioredis.create_redis(
        config['redis']['addr'].as_sockaddr(),
        password=config['redis']['password'] if config['redis']['password'] else None,
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB))
    with contextlib.closing(loop):
        try:
            yield loop, config_server
        finally:
            ctx['redis_image'].close()
            loop.run_until_complete(ctx['redis_image'].wait_closed())
    asyncio.set_event_loop(None)


@cli.command()
@click.argument('key')
@click.argument('value')
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes), default=ConfigScopes.GLOBAL,
              help='The configuration scope to put the value.')
@click.pass_obj
def put(cli_ctx, key, value, scope):
    '''Put a single key-value pair into the etcd.'''
    with cli_ctx.logger, etcd_ctx(cli_ctx) as (loop, etcd):
        loop.run_until_complete(
            etcd.put(key, value, scope=scope))


@cli.command()
@click.argument('key', type=str)
@click.argument('file', type=click.File('rb'))
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes),
              default=ConfigScopes.GLOBAL,
              help='The configuration scope to put the value.')
@click.pass_obj
def put_json(cli_ctx, key, file, scope):
    '''
    Put a JSON object from FILE to the etcd as flattened key-value pairs
    under the given KEY prefix.
    '''
    with etcd_ctx(cli_ctx) as (loop, etcd):
        with contextlib.closing(io.BytesIO()) as buf:
            while True:
                part = file.read(65536)
                if not part:
                    break
                buf.write(part)
            value = json.loads(buf.getvalue())
            value = {f'{key}/{k}': v for k, v in value.items()}
            loop.run_until_complete(
                etcd.put_dict(value, scope=scope))


@cli.command()
@click.argument('key')
@click.option('--prefix', is_flag=True,
              help='Get all key-value pairs prefixed with the given key '
                   'as a JSON form.')
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes),
              default=ConfigScopes.GLOBAL,
              help='The configuration scope to put the value.')
@click.pass_obj
def get(cli_ctx, key, prefix, scope):
    '''
    Get the value of a key in the configured etcd namespace.
    '''
    with cli_ctx.logger, etcd_ctx(cli_ctx) as (loop, etcd):
        if prefix:
            data = loop.run_until_complete(etcd.get_prefix(key, scope=scope))
            print(json.dumps(dict(data), indent=4))
        else:
            val = loop.run_until_complete(etcd.get(key, scope=scope))
            if val is None:
                sys.exit(1)
            print(val)


@cli.command()
@click.argument('key')
@click.option('--prefix', is_flag=True,
              help='Delete all keys prefixed with the given key.')
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes),
              default=ConfigScopes.GLOBAL,
              help='The configuration scope to put the value.')
@click.pass_obj
def delete(cli_ctx, key, prefix, scope):
    '''Delete the key in the configured etcd namespace.'''
    with cli_ctx.logger, etcd_ctx(cli_ctx) as (loop, etcd):
        if prefix:
            loop.run_until_complete(etcd.delete_prefix(key, scope=scope))
        else:
            loop.run_until_complete(etcd.delete(key, scope=scope))


@cli.command()
@click.option('-s', '--short', is_flag=True,
              help='Show only the image references and digests.')
@click.option('-i', '--installed', is_flag=True,
              help='Show only the installed images.')
@click.pass_obj
def list_images(cli_ctx, short, installed):
    '''List everything about images.'''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            displayed_items = []
            items = loop.run_until_complete(
                config_server.list_images())
            for item in items:
                if installed and not item['installed']:
                    continue
                if short:
                    img = ImageRef(f"{item['name']}:{item['tag']}",
                                   item['registry'])
                    displayed_items.append((img.canonical, item['digest']))
                else:
                    pprint(item)
            if short:
                print(tabulate(displayed_items, tablefmt='plain'))
        except Exception:
            log.exception('An error occurred.')


@cli.command()
@click.argument('reference')
@click.pass_obj
def inspect_image(cli_ctx, reference):
    '''List everything about images.'''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            item = loop.run_until_complete(
                config_server.inspect_image(reference))
            pprint(item)
        except Exception:
            log.exception('An error occurred.')


@cli.command()
@click.argument('reference')
@click.pass_obj
def forget_image(cli_ctx, reference):
    '''
    Forget (delete) a specific image.
    NOTE: aliases to the given reference are NOT deleted.
    '''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            loop.run_until_complete(config_server.forget_image(reference))
            log.info('Done.')
        except Exception:
            log.exception('An error occurred.')


@cli.command()
@click.argument('reference')
@click.argument('slot_type')
@click.argument('range_value', type=MinMaxRange)
@click.pass_obj
def set_image_resource_limit(cli_ctx, reference, slot_type, range_value):
    '''Set the MIN:MAX values of a SLOT_TYPE limit for the given image REFERENCE.'''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            loop.run_until_complete(
                config_server.set_image_resource_limit(reference, slot_type, range_value))
        except Exception:
            log.exception('An error occurred.')


@cli.command()
@click.argument('registry')
@click.pass_obj
def rescan_images(cli_ctx, registry):
    '''
    Update the kernel image metadata from all configured docker registries.

    Pass the name (usually hostname or "lablup") of the Docker registry configured as REGISTRY.
    '''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            loop.run_until_complete(
                config_server.rescan_images(registry))
        except Exception:
            log.exception('An error occurred.')


@cli.command()
@click.argument('alias')
@click.argument('target')
@click.pass_obj
def alias(cli_ctx, alias, target):
    '''Add an image alias from the given alias to the target image reference.'''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        loop.run_until_complete(
            config_server.alias(alias, target))


@cli.command()
@click.argument('alias')
@click.pass_obj
def dealias(cli_ctx, alias):
    '''Remove an alias.'''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        loop.run_until_complete(
            config_server.dealias(alias))
