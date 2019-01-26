import logging

import graphene

from ai.backend.common.logging import BraceStyleAdapter
from .base import KVPair, ResourceLimit
# from ai.backend.common.types import ImageRef

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
    size_bytes = graphene.Int()
    resource_limits = graphene.List(ResourceLimit)
    supported_accelerators = graphene.List(graphene.String)

    @classmethod
    async def load_item(cls, config_server, reference):
        r = await config_server.inspect_image(reference)
        return cls(
            name=r['name'],
            humanized_name=r['humanized_name'],
            tag=r['tag'],
            registry=r['registry'],
            digest=r['digest'],
            aliases=r['aliases'],
            labels=[
                KVPair(key=k, value=v)
                for k, v in r['labels'].items()],
            size_bytes=r['size_bytes'],
            supported_accelerators=r['supported_accelerators'],
            resource_limits=[
                ResourceLimit(key=v['key'], min=v['min'], max=v['max'])
                for v in r['resource_limits']],
        )

    @classmethod
    async def load_all(cls, config_server):
        raw_items = await config_server.list_images()
        items = []
        # Convert to GQL objects
        for r in raw_items:
            item = cls(
                name=r['name'],
                humanized_name=r['humanized_name'],
                tag=r['tag'],
                registry=r['registry'],
                digest=r['digest'],
                aliases=r['aliases'],
                labels=[
                    KVPair(key=k, value=v)
                    for k, v in r['labels'].items()],
                size_bytes=r['size_bytes'],
                supported_accelerators=r['supported_accelerators'],
                resource_limits=[
                    ResourceLimit(key=v['key'], min=v['min'], max=v['max'])
                    for v in r['resource_limits']],
            )
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
    async def mutate(root, info, registry):
        log.info('rescanning docker registry {0} by API request', registry)
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
