from __future__ import annotations
from decimal import Decimal
import json

import logging
from typing import (
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
)
import aiotools

import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
import sqlalchemy as sa
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection
import trafaret as t

from ai.backend.common import redis
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import BinarySize, ResourceSlot

from ai.backend.manager.models.utils import ExtendedAsyncSAEngine
if TYPE_CHECKING:
    from ai.backend.manager.background import ProgressReporter
from ai.backend.manager.container_registry import get_container_registry
from ai.backend.manager.config import SharedConfig, container_registry_iv

from .user import UserRole
from .base import BigInt, ForeignKeyIDColumn, IDColumn, KVPair, ResourceLimit, batch_result, metadata

if TYPE_CHECKING:
    from ..background import ProgressReporter
    from .gql import GraphQueryContext

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    'images',
    'image_aliases',
    'get_image_slot_ranges',
    'Image',
    'PreloadImage',
    'RescanImages',
    'ForgetImage',
    'AliasImage',
    'DealiasImage',
)


images = sa.Table(
    'images',
    metadata,
    IDColumn('id'),
    sa.Column('name', sa.String, nullable=False),
    sa.Column('image', sa.String, nullable=False),
    sa.Column(
        'created_at', sa.DateTime(timezone=True),
        server_default=sa.func.now(), index=True),
    sa.Column('tag', sa.TEXT),
    sa.Column('registry', sa.String, nullable=False),
    sa.Column('architecture', sa.String, nullable=False, default='amd64'),
    sa.Column('config_digest', sa.CHAR(length=72), nullable=False),
    sa.Column('size_bytes', sa.Integer, nullable=False),
    sa.Column('accelerators', sa.String, nullable=False),
    sa.Column('labels', sa.JSON, nullable=False),
    sa.Column('resources', sa.JSON, nullable=False),
)

image_aliases = sa.Table(
    'image_aliases',
    metadata,
    sa.Column('alias', sa.String(128), primary_key=True),
    ForeignKeyIDColumn('image', 'images.id', nullable=False),
)


async def get_image_slot_ranges(
    image: Row,
    shared_config: SharedConfig,
) -> Tuple[ResourceSlot, ResourceSlot]:
    slot_units = await shared_config.get_resource_slots()
    min_slot = ResourceSlot()
    max_slot = ResourceSlot()

    for slot_key, resource in json.loads(image['resources']):
        slot_unit = slot_units.get(slot_key)
        if slot_unit is None:
            # ignore unknown slots
            continue
        min_value = resource.get('min')
        if min_value is None:
            min_value = Decimal(0)
        max_value = resource.get('max')
        if max_value is None:
            max_value = Decimal('Infinity')
        if slot_unit == 'bytes':
            if not isinstance(min_value, Decimal):
                min_value = BinarySize.from_str(min_value)
            if not isinstance(max_value, Decimal):
                max_value = BinarySize.from_str(max_value)
        else:
            if not isinstance(min_value, Decimal):
                min_value = Decimal(min_value)
            if not isinstance(max_value, Decimal):
                max_value = Decimal(max_value)
        min_slot[slot_key] = min_value
        max_slot[slot_key] = max_value

    # fill missing
    for slot_key in slot_units.keys():
        if slot_key not in min_slot:
            min_slot[slot_key] = Decimal(0)
        if slot_key not in max_slot:
            max_slot[slot_key] = Decimal('Infinity')

    return min_slot, max_slot


async def alias_image(
    conn: AsyncConnection,
    alias: str,
    target: ImageRef,
):
    existing_image = (await conn.execute(
        sa.select([images.c.id, images.c.name, images.c.architecture, image_aliases.c.alias])
        .select_from(
            sa.join(
                images, image_aliases,
                (image_aliases.c.image == images.c.id and image_aliases.c.alias == alias),
            ),
        )
        .where((
            images.c.name == target.canonical and
            images.c.architecture == target.architecture
        )),
    )).fetchone()
    if existing_image is None:
        raise ValueError('no such image')
    if existing_image['alias'] is not None:
        raise ValueError(
            f'alias already created with {existing_image["name"]} ({existing_image["architecture"]})',
        )
    await conn.execute(
        sa.insert(image_aliases)
        .values({
            'alias': alias,
            'image': existing_image['id'],
        }),
    )


async def dealias_image(
    conn: AsyncConnection,
    alias: str,
):
    existing_alias = (
        await conn.execute(
            sa.select([image_aliases])
            .select_from(image_aliases)
            .where(image_aliases.c.alias == alias),
        )
    ).fetchone()
    if existing_alias is None:
        raise ValueError('no such alias')
    await conn.execute(sa.delete(image_aliases).where({'alias': alias}))


async def rescan_images(
    etcd: AsyncEtcd,
    db: ExtendedAsyncSAEngine,
    registry: str = None,
    *,
    reporter: ProgressReporter = None,
) -> None:
    registry_config_iv = t.Mapping(t.String, container_registry_iv)
    latest_registry_config = registry_config_iv.check(
        await etcd.get_prefix('config/docker/registry'),
    )
    # TODO: delete images from registries removed from the previous config?
    if registry is None:
        # scan all configured registries
        registries = latest_registry_config
    else:
        try:
            registries = {registry: latest_registry_config[registry]}
        except KeyError:
            raise RuntimeError("It is an unknown registry.", registry)
    async with aiotools.TaskGroup() as tg:
        for registry_name, registry_info in registries.items():
            log.info('Scanning kernel images from the registry "{0}"', registry_name)
            scanner_cls = get_container_registry(registry_info)
            scanner = scanner_cls(db, registry_name, registry_info)
            tg.create_task(scanner.rescan_single_registry(reporter))
    # TODO: delete images removed from registry?


class Image(graphene.ObjectType):
    name = graphene.String()
    humanized_name = graphene.String()
    tag = graphene.String()
    registry = graphene.String()
    architecture = graphene.String()
    digest = graphene.String()
    labels = graphene.List(KVPair)
    aliases = graphene.List(graphene.String)
    size_bytes = BigInt()
    resource_limits = graphene.List(ResourceLimit)
    supported_accelerators = graphene.List(graphene.String)
    installed = graphene.Boolean()
    installed_agents = graphene.List(graphene.String)
    # legacy field
    hash = graphene.String()

    @classmethod
    async def _convert_from_row(
        cls,
        ctx: GraphQueryContext,
        row: Row,
    ) -> Image:
        # TODO: add architecture
        installed = (
            await redis.execute(ctx.redis_image, lambda r: r.scard(row['name']))
        ) > 0
        installed_agents = await redis.execute(
            ctx.redis_image,
            lambda r: r.smembers(row['name']),
        )
        if installed_agents is None:
            installed_agents = []
        is_superadmin = (ctx.user['role'] == UserRole.SUPERADMIN)
        hide_agents = False if is_superadmin else ctx.local_config['manager']['hide-agents']
        async with ctx.db.begin_readonly() as conn:
            result = await conn.execte(
                sa.select([image_aliases.c.alias])
                .select_from(image_aliases)
                .where(image_aliases.c.image == row['id']),
            )
            aliases = list(map(lambda x: x['alias'], result.fetchall()))
        return cls(
            name=row['name'],
            humanized_name=row['name'],
            tag=row['tag'],
            registry=row['registry'],
            architecture=row['architecture'],
            digest=row['config_digest'],
            labels=[
                KVPair(key=k, value=v)
                for k, v in json.loads(row['labels']).items()],
            aliases=aliases,
            size_bytes=row['size_bytes'],
            resource_limits=[
                ResourceLimit(key=k, min=v['min'], max=v.get('max'))
                for k, v in json.loads(row['resources'])],
            supported_accelerators=row['accelerators'],
            installed=installed,
            installed_agents=installed_agents if not hide_agents else None,
            # legacy
            hash=row['config_digest'],
        )

    @classmethod
    async def batch_load_by_canonical(
        cls,
        graph_ctx: GraphQueryContext,
        image_names: Sequence[str],
    ) -> Sequence[Optional[Image]]:
        query = (
            sa.select([images])
            .select_from(images)
            .where(images.c.name.in_(image_names))
        )
        async with graph_ctx.db.begin_readonly() as conn:
            return await batch_result(
                graph_ctx, conn, query, cls,
                image_names, lambda row: row['name'],
            )

    @classmethod
    async def batch_load_by_image_ref(
        cls,
        graph_ctx: GraphQueryContext,
        image_refs: Sequence[ImageRef],
    ) -> Sequence[Optional[Image]]:
        image_names = [x.canonical for x in image_refs]
        return await cls.batch_load_by_canonical(graph_ctx, image_names)

    @classmethod
    async def load_item(cls, ctx: GraphQueryContext, reference: str) -> Image:
        async with ctx.db.begin_readonly() as conn:
            result = await conn.execute(
                sa.select([images])
                .select_from(images)
                .where(images.c.name == reference),
            )
            r = result.fetchone()
        return await cls._convert_from_dict(ctx, r)

    @classmethod
    async def load_all(
        cls,
        ctx: GraphQueryContext,
        *,
        is_installed: bool = None,
        is_operation: bool = None,
    ) -> Sequence[Image]:
        async with ctx.db.begin_readonly() as conn:
            raw_images = await conn.execute(
                sa.select([images])
                .select_from(images),
            )
        items: List[Image] = []
        # Convert to GQL objects
        for r in raw_images:
            item = await cls._convert_from_row(ctx, r)
            items.append(item)
        if is_installed is not None:
            items = [*filter(lambda item: item.installed == is_installed, items)]
        if is_operation is not None:

            def _filter_operation(item):
                for label in item.labels:
                    if label.key == 'ai.backend.features' and 'operation' in label.value:
                        return not is_operation
                return not is_operation

            items = [*filter(_filter_operation, items)]
        return items

    @staticmethod
    async def filter_allowed(
        ctx: GraphQueryContext,
        items: Sequence[Image],
        domain_name: str,
        *,
        is_installed: bool = None,
        is_operation: bool = None,
    ) -> Sequence[Image]:
        from .domain import domains
        async with ctx.db.begin() as conn:
            query = (
                sa.select([domains.c.allowed_docker_registries])
                .select_from(domains)
                .where(domains.c.name == domain_name)
            )
            result = await conn.execute(query)
            allowed_docker_registries = result.scalar()
        items = [
            item for item in items
            if item.registry in allowed_docker_registries
        ]
        if is_installed is not None:
            items = [*filter(lambda item: item.installed == is_installed, items)]
        if is_operation is not None:

            def _filter_operation(item):
                for label in item.labels:
                    if label.key == 'ai.backend.features' and 'operation' in label.value:
                        return not is_operation
                return not is_operation

            items = [*filter(_filter_operation, items)]
        return items


class PreloadImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        references = graphene.List(graphene.String, required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        references: Sequence[str],
        target_agents: Sequence[str],
    ) -> PreloadImage:
        return PreloadImage(ok=False, msg='Not implemented.', task_id=None)


class UnloadImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        references = graphene.List(graphene.String, required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        references: Sequence[str],
        target_agents: Sequence[str],
    ) -> UnloadImage:
        return UnloadImage(ok=False, msg='Not implemented.', task_id=None)


class RescanImages(graphene.Mutation):

    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        registry = graphene.String()

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.UUID()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        registry: str = None,
    ) -> RescanImages:
        log.info('rescanning docker registry {0} by API request',
                 f'({registry})' if registry else '(all)')
        ctx: GraphQueryContext = info.context

        async def _rescan_task(reporter: ProgressReporter) -> None:
            await rescan_images(ctx.etcd, ctx.db, registry, reporter=reporter)

        task_id = await ctx.background_task_manager.start(_rescan_task)
        return RescanImages(ok=True, msg='', task_id=task_id)


class ForgetImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        reference = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        reference: str,
    ) -> ForgetImage:
        log.info('forget image {0} by API request', reference)
        ctx: GraphQueryContext = info.context
        async with ctx.db.begin() as conn:
            query = sa.delete(images).where(images.c.name == reference)
            await conn.execute(query)
        return ForgetImage(ok=True, msg='')


class AliasImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        alias = graphene.String(required=True)
        target = graphene.String(required=True)
        architecture = graphene.String(default='x86_64')

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        alias: str,
        target: str,
        architecture: str,
    ) -> AliasImage:
        image_ref = ImageRef(target, architecture)
        log.info('alias image {0} -> {1} by API request', alias, image_ref)
        ctx: GraphQueryContext = info.context
        try:
            async with ctx.db.begin() as conn:
                await alias_image(conn, alias, image_ref)
        except ValueError as e:
            return AliasImage(ok=False, msg=str(e))
        return AliasImage(ok=True, msg='')


class DealiasImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        alias = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        alias: str,
    ) -> DealiasImage:
        log.info('dealias image {0} by API request', alias)
        ctx: GraphQueryContext = info.context
        try:
            async with ctx.db.begin() as conn:
                await dealias_image(conn, alias)
        except ValueError as e:
            return DealiasImage(ok=False, msg=str(e))
        return DealiasImage(ok=True, msg='')
