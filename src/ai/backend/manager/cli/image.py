import asyncio
import contextlib
import logging
from pprint import pprint
from typing import TYPE_CHECKING, AsyncIterator

import click
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy.orm as sessionmaker
from tabulate import tabulate

from ai.backend.common.cli import MinMaxRange
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.exception import UnknownImageReference
from ai.backend.common.logging import BraceStyleAdapter

from ai.backend.manager.models.utils import connect_database
from ai.backend.manager.models.image import (
    ImageAliasRow,
    ImageRow,
    rescan_images as rescan_images_func,
)

from ai.backend.manager.defs import DEFAULT_IMAGE_ARCH
if TYPE_CHECKING:
    from .context import CLIContext

log = BraceStyleAdapter(logging.getLogger(__name__))


@click.group()
def cli() -> None:
    pass


@contextlib.asynccontextmanager
async def etcd_ctx(cli_ctx: CLIContext) -> AsyncIterator[AsyncEtcd]:
    local_config = cli_ctx.local_config
    creds = None
    if local_config['etcd']['user']:
        creds = {
            'user': local_config['etcd']['user'],
            'password': local_config['etcd']['password'],
        }
    scope_prefix_map = {
        ConfigScopes.GLOBAL: '',
        # TODO: provide a way to specify other scope prefixes
    }
    etcd = AsyncEtcd(local_config['etcd']['addr'], local_config['etcd']['namespace'],
                     scope_prefix_map, credentials=creds)
    try:
        yield etcd
    finally:
        await etcd.close()


@cli.command()
@click.option('-s', '--short', is_flag=True,
              help='Show only the image references and digests.')
@click.option('-i', '--installed', is_flag=True,
              help='Show only the installed images.')
@click.pass_obj
def list_images(cli_ctx: CLIContext, short, installed) -> None:
    '''List all configured images.'''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                displayed_items = []
                try:
                    items = await ImageRow.list(session)
                    # NOTE: installed/installed_agents fields are no longer provided in CLI,
                    #       until we finish the epic refactoring of image metadata db.
                    for item in items:
                        if installed and not item.installed:
                            continue
                        if short:
                            displayed_items.append((item.image_ref.canonical, item.config_digest))
                        else:
                            pprint(item)
                    if short:
                        print(tabulate(displayed_items, tablefmt='plain'))
                except Exception:
                    log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('canonical_or_alias')
@click.argument('architecture', default=DEFAULT_IMAGE_ARCH)
@click.pass_obj
def inspect_image(cli_ctx: CLIContext, canonical_or_alias, architecture) -> None:
    '''Show the details of the given image or alias.'''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                try:
                    image_row = await ImageRow.resolve(session, [
                        ImageRef(canonical_or_alias, architecture, ['*']),
                        canonical_or_alias,
                    ])
                    pprint(await image_row.inspect(session))
                except UnknownImageReference:
                    log.exception('Image not found.')
                except Exception:
                    log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('canonical_or_alias')
@click.argument('architecture', default=DEFAULT_IMAGE_ARCH)
@click.pass_obj
def forget_image(cli_ctx: CLIContext, canonical_or_alias, architecture) -> None:
    '''
    Forget (delete) a specific image.
    '''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                try:
                    image_row = await ImageRow.resolve(session, [
                        ImageRef(canonical_or_alias, architecture, ['*']),
                        canonical_or_alias,
                    ])
                    async with session.begin():
                        await session.delete(image_row)
                except UnknownImageReference:
                    log.exception('Image not found.')
                except Exception:
                    log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('canonical_or_alias')
@click.argument('slot_type')
@click.argument('range_value', type=MinMaxRange)
@click.argument('architecture', default=DEFAULT_IMAGE_ARCH)
@click.pass_obj
def set_image_resource_limit(
    cli_ctx: CLIContext,
    canonical_or_alias,
    slot_type,
    range_value,
    architecture,
) -> None:
    '''Set the MIN:MAX values of a SLOT_TYPE limit for the given image REFERENCE.'''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                try:
                    image_row = await ImageRow.resolve(session, [
                        ImageRef(canonical_or_alias, architecture, ['*']),
                        canonical_or_alias,
                    ])
                    async with session.begin():
                        await image_row.set_resource_limit(slot_type, range_value)
                except UnknownImageReference:
                    log.exception('Image not found.')
                except Exception:
                    log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('registry')
@click.pass_obj
def rescan_images(cli_ctx: CLIContext, registry) -> None:
    '''
    Update the kernel image metadata from all configured docker registries.

    Pass the name (usually hostname or "lablup") of the Docker registry configured as REGISTRY.
    '''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            async with db.begin_readonly() as conn:
                async with etcd_ctx(cli_ctx) as etcd:
                    try:
                        await rescan_images_func(etcd, conn)
                    except Exception:
                        log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('alias')
@click.argument('target')
@click.argument('architecture', default=DEFAULT_IMAGE_ARCH)
@click.pass_obj
def alias(cli_ctx: CLIContext, alias, target, architecture) -> None:
    '''Add an image alias from the given alias to the target image reference.'''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                try:
                    image_row = await ImageRow.resolve(session, [
                        ImageRef(target, architecture, ['*']),
                    ])
                    async with session.begin():
                        await ImageAliasRow.create(session, alias, image_row)
                except UnknownImageReference:
                    log.exception('Image not found.')
                except Exception:
                    log.exception('An error occurred.')
    with cli_ctx.logger:
        asyncio.run(_impl())


@cli.command()
@click.argument('alias')
@click.pass_obj
def dealias(cli_ctx: CLIContext, alias) -> None:
    '''Remove an alias.'''
    async def _impl():
        async with connect_database(cli_ctx.local_config) as db:
            create_db_session = sessionmaker(db, class_=AsyncSession)
            async with create_db_session() as session:
                alias_row = await session.scalar(
                    sa.select(ImageAliasRow)
                    .where(ImageAliasRow.alias == alias),
                )
                if alias_row is None:
                    log.exception('Alias not found.')
                    return
                async with session.begin():
                    await session.delete(alias_row)
    with cli_ctx.logger:
        asyncio.run(_impl())
