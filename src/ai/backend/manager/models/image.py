from __future__ import annotations
import logging
from typing import (
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

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.admin'))

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
    def _convert_from_dict(cls, context, data):
        is_superadmin = (context['user']['role'] == UserRole.SUPERADMIN)
        hide_agents = False if is_superadmin else context['local_config']['manager']['hide-agents']
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
            installed=data['installed'],
            installed_agents=data.get('installed_agents', []) if not hide_agents else None,
            # legacy
            hash=data['digest'],
        )

    @classmethod
    async def load_item(cls, context, reference):
        r = await context['shared_config'].inspect_image(reference)
        return cls._convert_from_dict(context, r)

    @classmethod
    async def load_all(cls, context, is_installed=None, is_operation=None):
        raw_items = await context['shared_config'].list_images()
        items = []
        # Convert to GQL objects
        for r in raw_items:
            item = cls._convert_from_dict(context, r)
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
    async def filter_allowed(context, items, domain_name,
                             is_installed=None, is_operation=None):
        from .domain import domains
        async with context['dbpool'].acquire() as conn:
            query = (
                sa.select([domains.c.allowed_docker_registries])
                .select_from(domains)
                .where(domains.c.name == domain_name)
            )
            result = await conn.execute(query)
            allowed_docker_registries = await result.scalar()
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
        config_server = info.context['shared_config']

        async def _rescan_task(reporter: ProgressReporter) -> None:
            await config_server.rescan_images(registry, reporter=reporter)

        task_id = await info.context['background_task_manager'].start(_rescan_task)
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
        config_server = info.context['shared_config']
        await config_server.forget_image(reference)
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
        config_server = info.context['shared_config']
        try:
            await config_server.alias(alias, target)
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
        config_server = info.context['shared_config']
        await config_server.dealias(alias)
        return DealiasImage(ok=True, msg='')
