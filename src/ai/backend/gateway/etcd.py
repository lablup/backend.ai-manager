from decimal import Decimal
import logging
from pathlib import Path

from aiohttp import web
import aiotools
import yaml

from ai.backend.common.identity import get_instance_id, get_instance_ip
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ImageRef

from ..manager.models.agent import ResourceSlot
from .manager import ManagerStatus

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.etcd'))


class ConfigServer:

    def __init__(self, etcd_addr, namespace):
        # WARNING: importing etcd3/grpc must be done after forks.
        from ai.backend.common.etcd import AsyncEtcd
        self.etcd = AsyncEtcd(etcd_addr, namespace)

    async def register_myself(self, app_config):
        instance_id = await get_instance_id()
        if app_config.advertised_manager_host:
            instance_ip = app_config.advertised_manager_host
            log.info('manually set advertised manager host: {0}', instance_ip)
        else:
            # fall back 1: read private IP from cloud instance metadata
            # fall back 2: read hostname and resolve it
            # fall back 3: "127.0.0.1"
            instance_ip = await get_instance_ip()
        event_addr = f'{instance_ip}:{app_config.events_port}'
        await self.etcd.put_multi(
            ['nodes/manager', 'nodes/redis',
             'nodes/manager/event_addr', 'nodes/docker_registry'],
            [instance_id, app_config.redis_addr,
             event_addr, app_config.docker_registry])

    async def deregister_myself(self):
        await self.etcd.delete_prefix('nodes/manager')

    async def update_kernel_images_from_file(self, file: Path):
        log.info('Loading kernel image data from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
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
            tpu_share = image['slots']['tpu']
            tpu_share = 'null' if tpu_share is None else f'{tpu_share:.2f}'
            await self.etcd.put_multi(
                [f'images/{name}',
                 f'images/{name}/cpu',
                 f'images/{name}/mem',
                 f'images/{name}/gpu',
                 f'images/{name}/tpu'],
                ['1', cpu_share, mem_share, gpu_share, tpu_share])

            inserted_tags = [(f'images/{name}/tags/{tag}', hash)
                             for tag, hash in image['tags']]
            await self.etcd.put_multi(*zip(*inserted_tags))
        log.info('Done.')

    async def update_aliases_from_file(self, file: Path):
        log.info('Updating image aliases from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for item in data['aliases']:
            alias = item[0]
            target = item[1]
            await self.etcd.put(f'images/_aliases/{alias}', target)
            print(f'{alias} -> {target}')
        log.info('Done.')

    async def update_kernel_images_from_registry(self, registry_addr):
        log.info('Scanning kernel image versions from "{0}"', registry_addr)
        # TODO: a method to scan docker hub and update kernel image versions
        # TODO: a cli command to execute the above method
        raise NotImplementedError

    async def update_volumes_from_file(self, file: Path):
        log.info('Updating network volumes from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
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

    async def manager_status_update(self):
        async for ev in self.etcd.watch('manager/status'):
            yield ev

    async def update_manager_status(self, status):
        await self.etcd.put('manager/status', status.value)

    @aiotools.lru_cache(maxsize=1)
    async def get_manager_status(self):
        status = await self.etcd.get('manager/status')
        return ManagerStatus(status)

    @aiotools.lru_cache(maxsize=1, expire_after=60.0)
    async def get_allowed_origins(self):
        origins = await self.etcd.get('config/api/allow-origins')
        if origins is None:
            origins = '*'
        return origins

    @aiotools.lru_cache(maxsize=1)
    async def get_docker_registry(self):
        docker_registry = await self.etcd.get('nodes/docker_registry')
        return docker_registry

    @aiotools.lru_cache(expire_after=60.0)
    async def get_image_required_slots(self, image_ref: ImageRef):
        installed = await self.etcd.get(f'images/{image_ref.name}')
        if installed is None:
            raise RuntimeError('Image metadata is not available!')
        cpu = await self.etcd.get(f'images/{image_ref.name}/cpu')
        cpu = None if cpu == 'null' else Decimal(cpu)
        mem = await self.etcd.get(f'images/{image_ref.name}/mem')
        mem = None if mem == 'null' else Decimal(mem)
        _, platform_tags = image_ref.tag_set
        if 'gpu' in platform_tags or 'cuda' in platform_tags:
            gpu = await self.etcd.get(f'images/{image_ref.name}/gpu')
            gpu = Decimal(0) if gpu == 'null' else Decimal(gpu)
        else:
            gpu = Decimal(0)
        if 'tpu' in platform_tags:
            tpu = await self.etcd.get(f'images/{image_ref.name}/tpu')
            tpu = Decimal(0) if tpu == 'null' else Decimal(tpu)
        else:
            tpu = Decimal(0)
        return ResourceSlot(mem=mem, cpu=cpu, gpu=gpu, tpu=tpu)

    # TODO: invalidate config cache when etcd content is updated


async def init(app):
    if app['pidx'] == 0:
        await app['config_server'].register_myself(app['config'])


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
