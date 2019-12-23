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


@contextlib.asynccontextmanager
async def etcd_ctx(cli_ctx):
    config = cli_ctx.config
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
    yield etcd


@contextlib.asynccontextmanager
async def config_ctx(cli_ctx):
    config = cli_ctx.config
    ctx = {}
    ctx['config'] = config
    # scope_prefix_map is created inside ConfigServer
    config_server = ConfigServer(
        ctx, config['etcd']['addr'],
        config['etcd']['user'], config['etcd']['password'],
        config['etcd']['namespace'])
    raw_redis_config = await config_server.etcd.get_prefix('config/redis')
    config['redis'] = redis_config_iv.check(raw_redis_config)
    ctx['redis_image'] = await aioredis.create_redis(
        config['redis']['addr'].as_sockaddr(),
        password=config['redis']['password'] if config['redis']['password'] else None,
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB)
    try:
        yield config_server
    finally:
        ctx['redis_image'].close()
        await ctx['redis_image'].wait_closed()


@cli.command()
@click.argument('key')
@click.argument('value')
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes), default=ConfigScopes.GLOBAL,
              help='The configuration scope to put the value.')
@click.pass_obj
def put(cli_ctx, key, value, scope):
    '''Put a single key-value pair into the etcd.'''
    async def _put():
        async with etcd_ctx(cli_ctx) as etcd:
            await etcd.put(key, value, scope=scope)
    with cli_ctx.logger:
        asyncio.run(_put())


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
    async def _put_json():
        async with etcd_ctx(cli_ctx) as etcd:
            with contextlib.closing(io.BytesIO()) as buf:
                while True:
                    part = file.read(65536)
                    if not part:
                        break
                    buf.write(part)
                value = json.loads(buf.getvalue())
                value = {f'{key}/{k}': v for k, v in value.items()}
                await etcd.put_dict(value, scope=scope)
    with cli_ctx.logger:
        asyncio.run(_put_json())


@cli.command()
@click.argument('src_prefix', type=str)
@click.argument('dst_prefix', type=str)
@click.option('-s', '--scope', type=EnumChoice(ConfigScopes),
              default=ConfigScopes.GLOBAL,
              help='The configuration scope to get/put the subtree. '
                   'To move between different scopes, use the global scope '
                   'and specify the per-scope prefixes manually.')
@click.pass_obj
def move_subtree(cli_ctx, src_prefix, dst_prefix, scope):
    '''
    Move a subtree to another key prefix.
    '''
    async def _impl():
        async with etcd_ctx(cli_ctx) as etcd:
            subtree = await etcd.get_prefix(src_prefix, scope=scope)
            subtree = {f'{dst_prefix}/{k}': v for k, v in subtree.items()}
            await etcd.put_dict(subtree, scope=scope)
            await etcd.delete_prefix(src_prefix, scope=scope)
    with cli_ctx.logger:
        asyncio.run(_impl())


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
    async def _get():
        async with etcd_ctx(cli_ctx) as etcd:
            if prefix:
                data = await etcd.get_prefix(key, scope=scope)
                print(json.dumps(dict(data), indent=4))
            else:
                val = await etcd.get(key, scope=scope)
                if val is None:
                    sys.exit(1)
                print(val)
    with cli_ctx.logger:
        asyncio.run(_get())


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
    async def _delete():
        async with etcd_ctx(cli_ctx) as etcd:
            if prefix:
                await etcd.delete_prefix(key, scope=scope)
            else:
                await etcd.delete(key, scope=scope)
    with cli_ctx.logger:
        asyncio.run(_delete())


@cli.command()
@click.option('-s', '--short', is_flag=True,
              help='Show only the image references and digests.')
@click.option('-i', '--installed', is_flag=True,
              help='Show only the installed images.')
@click.pass_obj
def list_images(cli_ctx, short, installed):
    '''List everything about images.'''
    async def _impl():
        async with config_ctx(cli_ctx) as config_server:
            displayed_items = []
            try:
                items = await config_server.list_images()
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
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('reference')
@click.pass_obj
def inspect_image(cli_ctx, reference):
    '''List everything about images.'''
    async def _impl():
        async with config_ctx(cli_ctx) as config_server:
            try:
                item = await config_server.inspect_image(reference)
                pprint(item)
            except Exception:
                log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('reference')
@click.pass_obj
def forget_image(cli_ctx, reference):
    '''
    Forget (delete) a specific image.
    NOTE: aliases to the given reference are NOT deleted.
    '''
    async def _impl():
        async with config_ctx(cli_ctx) as config_server:
            try:
                await config_server.forget_image(reference)
                log.info('Done.')
            except Exception:
                log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('reference')
@click.argument('slot_type')
@click.argument('range_value', type=MinMaxRange)
@click.pass_obj
def set_image_resource_limit(cli_ctx, reference, slot_type, range_value):
    '''Set the MIN:MAX values of a SLOT_TYPE limit for the given image REFERENCE.'''
    async def _set_image_resource_limit():
        async with config_ctx(cli_ctx) as config_server:
            try:
                await config_server.set_image_resource_limit(
                    reference, slot_type, range_value)
            except Exception:
                log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_set_image_resource_limit())


@cli.command()
@click.argument('registry')
@click.pass_obj
def rescan_images(cli_ctx, registry):
    '''
    Update the kernel image metadata from all configured docker registries.

    Pass the name (usually hostname or "lablup") of the Docker registry configured as REGISTRY.
    '''
    async def _rescan_images():
        async with config_ctx(cli_ctx) as config_server:
            try:
                await config_server.rescan_images(registry)
            except Exception:
                log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_rescan_images())


@cli.command()
@click.argument('alias')
@click.argument('target')
@click.pass_obj
def alias(cli_ctx, alias, target):
    '''Add an image alias from the given alias to the target image reference.'''
    async def _alias():
        async with config_ctx(cli_ctx) as config_server:
            await config_server.alias(alias, target)
    with cli_ctx.logger:
        asyncio.run(_alias())


@cli.command()
@click.argument('alias')
@click.pass_obj
def dealias(cli_ctx, alias):
    '''Remove an alias.'''
    async def _dealias():
        async with config_ctx(cli_ctx) as config_server:
            await config_server.dealias(alias)
    with cli_ctx.logger:
        asyncio.run(_dealias())
