from decimal import Decimal
import logging
from pathlib import Path
from collections import defaultdict

from aiohttp import web
import aiotools
import yaml

from ai.backend.common.identity import get_instance_id, get_instance_ip
from ..manager.models.agent import ResourceSlot
from .exceptions import ImageNotFound

log = logging.getLogger('ai.backend.gateway.etcd')


class ConfigServer:

    def __init__(self, etcd_addr, namespace):
        # WARNING: importing etcd3/grpc must be done after forks.
        from ai.backend.common.etcd import AsyncEtcd
        self.etcd = AsyncEtcd(etcd_addr, namespace)

    async def register_myself(self, app_config):
        instance_id = await get_instance_id()
        if app_config.advertised_manager_host:
            instance_ip = app_config.advertised_manager_host
            log.info(f'manually set advertised manager host: {instance_ip}')
        else:
            # fall back 1: read private IP from cloud instance metadata
            # fall back 2: read hostname and resolve it
            # fall back 3: "127.0.0.1"
            instance_ip = await get_instance_ip()
        event_addr = f'{instance_ip}:{app_config.events_port}'
        await self.etcd.put_multi(
            ['nodes/manager', 'nodes/redis', 'nodes/manager/event_addr'],
            [instance_id, app_config.redis_addr, event_addr])

    async def deregister_myself(self):
        await self.etcd.delete_prefix('nodes/manager')

    async def update_kernel_images_from_file(self, file: Path):
        log.info(f'Loading kernel image data from "{file}"')
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error(f'Cannot open "{file}".')
            return
        for image in data['images']:
            name = image['name']
            print(f"Updating {name}")

            inserted_aliases = []
            for tag, hash in image['tags']:
                assert hash
                if hash.startswith(':'):  # tag-level alias
                    inserted_aliases.append(
                        (f'images/_aliases/{name}:{tag}', f'{name}:{hash[1:]}')
                    )
            if inserted_aliases:
                await self.etcd.put_multi(*zip(*inserted_aliases))

            cpu_share = image['slots']['cpu']
            cpu_share = 'null' if cpu_share is None else f'{cpu_share:.2f}'
            mem_share = image['slots']['mem']
            mem_share = 'null' if mem_share is None else f'{mem_share:.2f}'
            gpu_share = image['slots']['gpu']
            gpu_share = 'null' if gpu_share is None else f'{gpu_share:.2f}'
            await self.etcd.put_multi(
                [f'images/{name}',
                 f'images/{name}/cpu',
                 f'images/{name}/mem',
                 f'images/{name}/gpu'],
                ['1', cpu_share, mem_share, gpu_share])

            inserted_tags = [(f'images/{name}/tags/{tag}', hash)
                             for tag, hash in image['tags']]
            await self.etcd.put_multi(*zip(*inserted_tags))
        log.info('Done.')

    async def update_aliases_from_file(self, file: Path):
        log.info(f'Updating image aliases from "{file}"')
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error(f'Cannot open "{file}".')
            return
        for item in data['aliases']:
            alias = item[0]
            target = item[1]
            await self.etcd.put(f'images/_aliases/{alias}', target)
            print(f'{alias} -> {target}')
        log.info('Done.')

    async def update_kernel_images_from_registry(self, registry_addr):
        log.info(f'Scanning kernel image versions from "{registry_addr}"')
        # TODO: a method to scan docker hub and update kernel image versions
        # TODO: a cli command to execute the above method
        raise NotImplementedError

    async def update_volumes_from_file(self, file: Path):
        log.info(f'Updating network volumes from "{file}"')
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error(f'Cannot open "{file}".')
            return
        for item in data['volumes']:
            name = item['name']
            ks = []
            vs = []
            for k, v in item['mount'].items():
                ks.append(f'volumes/{name}/mount/{k}')
                vs.append(v)
            await self.etcd.put_multi(ks, vs)
        log.info('done')

    @aiotools.lru_cache(maxsize=1)
    async def get_overbook_factors(self):
        '''
        Retrieves the overbook parameters which is used to
        scale the resource slot values reported by the agent
        to increase server utilization.

        TIP: If your users run mostly compute-intesive sessions,
        lower these values towards 1.0.
        '''

        cpu = await self.etcd.get('config/overbook/cpu')
        cpu = 6.0 if cpu is None else float(cpu)
        mem = await self.etcd.get('config/overbook/mem')
        mem = 2.0 if mem is None else float(mem)
        gpu = await self.etcd.get('config/overbook/gpu')
        gpu = 1.0 if gpu is None else float(gpu)
        return {
            'mem': mem,
            'cpu': cpu,
            'gpu': gpu,
        }

    @aiotools.lru_cache()
    async def get_image_required_slots(self, name, tag):
        installed = await self.etcd.get(f'images/{name}')
        if installed is None:
            raise RuntimeError('Image metadata is not available!')
        cpu = await self.etcd.get(f'images/{name}/cpu')
        cpu = None if cpu == 'null' else Decimal(cpu)
        mem = await self.etcd.get(f'images/{name}/mem')
        mem = None if mem == 'null' else Decimal(mem)
        if 'gpu' in tag:
            gpu = await self.etcd.get(f'images/{name}/gpu')
            gpu = None if gpu == 'null' else Decimal(gpu)
        else:
            gpu = Decimal(0)
        return ResourceSlot(mem=mem, cpu=cpu, gpu=gpu)

    @aiotools.lru_cache()
    async def get_image_metadata(self):
        image_metadata_list = list(await self.etcd.get_prefix('images'))
        image_metadata = defaultdict(dict)
        for key, metadata in image_metadata_list:
            key_tokens = key.split('/')
            if key_tokens[1] == '_aliases':
                # e.g. key, metadata = \
                #   ('images/_aliases/python3.6', 'python:3.6-debian')
                image_alias = key_tokens[2]
                image_real_name = metadata
                image_metadata['aliases'][image_alias] = image_real_name
            else:
                image_name = key_tokens[1]
                if len(key_tokens) == 2:
                    # e.g. key, metadata = ('images/python', '1')
                    image_metadata[image_name]['tag'] = {}
                elif key_tokens[2] == 'tags':
                    # e.g. key, metadata = \
                    #   ('images/python/tags/3.6-debian', 'ca7b9f52b6c2')
                    image_tag = key_tokens[3]
                    image_hash = metadata
                    if image_hash == 'x':
                        # Some images does not have image hash.
                        continue
                    image_hash_dict = image_metadata[image_name].get('tag', None)
                    assert image_hash_dict is not None, image_name
                    image_hash_dict[image_tag] = image_hash
                else:
                    # e.g. key, metadata = ('images/python/cpu', '1.00')
                    resource_type = key_tokens[2]
                    resource_slot = metadata
                    image_metadata[image_name][resource_type] = resource_slot

        return image_metadata

    @aiotools.lru_cache()
    async def resolve_image_name(self, name_or_alias):

        async def resolve_alias(alias_key):
            alias_target = None
            while True:
                prev_alias_key = alias_key
                alias_key = await self.etcd.get(f'images/_aliases/{alias_key}')
                if alias_key is None:
                    alias_target = prev_alias_key
                    break
            return alias_target

        alias_target = await resolve_alias(name_or_alias)
        if alias_target == name_or_alias and name_or_alias.rfind(':') == -1:
            alias_target = await resolve_alias(f'{name_or_alias}:latest')
        assert alias_target is not None
        name, _, tag = alias_target.partition(':')
        hash = await self.etcd.get(f'images/{name}/tags/{tag}')
        if hash is None:
            raise ImageNotFound(f'{name_or_alias}: Unregistered image '
                                'or unknown alias.')
        return name, tag

    # TODO: invalidate config cache when etcd content is updated


async def init(app):
    if app['pidx'] == 0:
        await app['config_server'].register_myself(app['config'])


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()


def create_app():
    app = web.Application()
    app['api_versions'] = (3,)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
