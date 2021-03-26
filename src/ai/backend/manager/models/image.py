from __future__ import annotations

import logging
from typing import (
    Any,
    List,
    Mapping,
    Sequence,
    TYPE_CHECKING,
)

import graphene
import sqlalchemy as sa
from graphql.execution.executors.asyncio import AsyncioExecutor

from ai.backend.common.logging import BraceStyleAdapter

from .user import UserRole
from .base import BigInt, KVPair, ResourceLimit

if TYPE_CHECKING:
    from ..background import ProgressReporter
    from .gql import GraphQueryContext

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    'Image',
    'PreloadImage',
    'RescanImages',
    'ForgetImage',
    'AliasImage',
    'DealiasImage',
)


class Image(graphene.ObjectType):
    name = graphene.String()
    humanized_name = graphene.String()
    tag = graphene.String()
    registry = graphene.String()
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
    async def _convert_from_dict(
        cls,
        ctx: GraphQueryContext,
        data: Mapping[str, Any],
    ) -> Image:
        installed = (
            await ctx.redis_image.scard(data['canonical_ref'])
        ) > 0
        installed_agents = await ctx.redis_image.smembers(data['canonical_ref'])
        if installed_agents is None:
            installed_agents = []
        is_superadmin = (ctx.user['role'] == UserRole.SUPERADMIN)
        hide_agents = False if is_superadmin else ctx.local_config['manager']['hide-agents']
        return cls(
            name=data['name'],
            humanized_name=data['humanized_name'],
            tag=data['tag'],
            registry=data['registry'],
            digest=data['digest'],
            labels=[
                KVPair(key=k, value=v)
                for k, v in data['labels'].items()],
            aliases=data['aliases'],
            size_bytes=data['size_bytes'],
            resource_limits=[
                ResourceLimit(key=v['key'], min=v['min'], max=v['max'])
                for v in data['resource_limits']],
            supported_accelerators=data['supported_accelerators'],
            installed=installed,
            installed_agents=installed_agents if not hide_agents else None,
            # legacy
            hash=data['digest'],
        )

    @classmethod
    async def load_item(cls, ctx: GraphQueryContext, reference: str) -> Image:
        r = await ctx.shared_config.inspect_image(reference)
        return await cls._convert_from_dict(ctx, r)

    @classmethod
    async def load_all(
        cls,
        ctx: GraphQueryContext,
        *,
        is_installed: bool = None,
        is_operation: bool = None,
    ) -> Sequence[Image]:
        raw_items = await ctx.shared_config.list_images()
        items: List[Image] = []
        # Convert to GQL objects
        for r in raw_items:
            item = await cls._convert_from_dict(ctx, r)
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
            await ctx.shared_config.rescan_images(registry, reporter=reporter)

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
        await ctx.shared_config.forget_image(reference)
        return ForgetImage(ok=True, msg='')


class AliasImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        alias = graphene.String(required=True)
        target = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        alias: str,
        target: str,
    ) -> AliasImage:
        log.info('alias image {0} -> {1} by API request', alias, target)
        ctx: GraphQueryContext = info.context
        try:
            await ctx.shared_config.alias(alias, target)
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
        await ctx.shared_config.dealias(alias)
        return DealiasImage(ok=True, msg='')
