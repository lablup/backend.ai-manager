import logging

import graphene
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from .user import UserRole
from .base import BigInt, KVPair, ResourceLimit

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
        hide_agents = False if is_superadmin else context['config']['manager']['hide-agents']
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
        r = await context['config_server'].inspect_image(reference)
        return cls._convert_from_dict(context, r)

    @classmethod
    async def load_all(cls, context, is_installed=None, is_operation=None):
        raw_items = await context['config_server'].list_images()
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
                .as_scalar()
            )
            result = await conn.execute(query)
            row = await result.fetchone()
            allowed_docker_registries = row[0]
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

    @staticmethod
    async def mutate(root, info, references, target_agents):
        return PreloadImage(ok=False, msg='Not implemented.')


class RescanImages(graphene.Mutation):

    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        registry = graphene.String()

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, registry=None):
        log.info('rescanning docker registry {0} by API request',
                 f'({registry})' if registry else '(all)')
        config_server = info.context['config_server']
        await config_server.rescan_images(registry)
        return RescanImages(ok=True, msg='')


class ForgetImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        reference = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, reference: str):
        log.info('forget image {0} by API request', reference)
        config_server = info.context['config_server']
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
    async def mutate(root, info, alias, target):
        log.info('alias image {0} -> {1} by API request', alias, target)
        config_server = info.context['config_server']
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
    async def mutate(root, info, alias):
        log.info('dealias image {0} by API request', alias)
        config_server = info.context['config_server']
        await config_server.dealias(alias)
        return DealiasImage(ok=True, msg='')
