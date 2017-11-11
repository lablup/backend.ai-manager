import logging
from pathlib import Path

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

            tags = {tag: hash for tag, hash in image['tags']}
            for tag, hash in tags.items():
                assert hash
                if hash.startswith(':'):
                    tags[tag] = tags[hash[1:]]

            cpu_count = image['slots']['cpu']
            mem_mbytes = int(image['slots']['mem'] * 1024)
            gpu_fraction = image['slots']['gpu']
            await self.etcd.put_multi(
                [f'images/{name}',
                 f'images/{name}/cpu',
                 f'images/{name}/mem',
                 f'images/{name}/gpu'],
                ['1',
                 '{0:d}'.format(cpu_count),
                 '{0:d}'.format(mem_mbytes),
                 '{0:.2f}'.format(gpu_fraction)])

            ks = []
            vs = []
            for tag, hash in tags.items():
                ks.append(f'images/{name}/tags/{tag}')
                vs.append(hash)
            await self.etcd.put_multi(ks, vs)
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
        mem = await self.etcd.get(f'images/{name}/mem')
        cpu = await self.etcd.get(f'images/{name}/cpu')
        if 'gpu' in tag:
            gpu = await self.etcd.get(f'images/{name}/gpu')
        else:
            gpu = 0
        return ResourceSlot(
            mem=int(mem),
            cpu=float(cpu),
            gpu=float(gpu),
        )

    @aiotools.lru_cache()
    async def resolve_image_name(self, name_or_alias):
        orig_name = await self.etcd.get(f'images/_aliases/{name_or_alias}')
        if orig_name is None:
            orig_name = name_or_alias  # try the given one as-is
        name, _, tag = orig_name.partition(':')
        if not tag:
            tag = 'latest'
        hash = await self.etcd.get(f'images/{name}/tags/{tag}')
        if hash is None:
            raise ImageNotFound(f'{name_or_alias}: Unregistered image '
                                'or unknown alias.')
        return name, tag

    # TODO: invalidate config cache when etcd content is updated


async def init(app):
    app['config_server'] = ConfigServer(app.config.etcd_addr, app.config.namespace)
    if app['pidx'] == 0:
        await app['config_server'].register_myself(app.config)


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()
