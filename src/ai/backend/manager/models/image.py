import logging

import graphene

from ai.backend.common.logging import BraceStyleAdapter
from .base import BigInt, KVPair, ResourceLimit

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.admin'))

__all__ = (
    'Image',
    'PreloadImage',
    'RescanImages',
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
    # legacy field
    hash = graphene.String()

    @classmethod
    def _convert_from_dict(cls, data):
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
            # legacy
            hash=data['digest'],
        )

    @classmethod
    async def load_item(cls, context, reference):
        r = await context['config_server'].inspect_image(reference)
        return cls._convert_from_dict(r)

    @classmethod
    async def load_all(cls, context):
        raw_items = await context['config_server'].list_images()
        items = []
        # Convert to GQL objects
        for r in raw_items:
            item = cls._convert_from_dict(r)
            items.append(item)
        return items


class PreloadImage(graphene.Mutation):

    class Arguments:
        references = graphene.List(graphene.String, required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, references, target_agents):
        return PreloadImage(ok=False, msg='Not implemented.')


class RescanImages(graphene.Mutation):

    class Arguments:
        registry = graphene.String()

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, registry=None):
        log.info('rescanning docker registry {0} by API request', registry if registry else '')
        config_server = info.context['config_server']
        await config_server.rescan_images(registry)
        return RescanImages(ok=True, msg='')


class AliasImage(graphene.Mutation):

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
