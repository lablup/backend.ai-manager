import logging
from pathlib import Path

import yaml
from ai.backend.common.identity import get_instance_id, get_instance_ip

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
            # TODO: image['tags'] -> (name, hash)
            await self.etcd.put_multi(
                [f'images/{name}',
                 f'images/{name}/cpu',
                 f'images/{name}/mem',
                 f'images/{name}/gpu'],
                ['installed',
                 '{0},{1}'.format(*image['resources']['cpu']),
                 '{0},{1}'.format(*image['resources']['mem']),
                 '{0},{1}'.format(*image['resources']['gpu'])])
        log.info('Done.')

    async def update_kernel_images_from_registry(self, registry_addr):
        log.info(f'Scanning kernel image versions from "{registry_addr}"')
        # TODO: a method to scan docker hub and update kernel image versions
        # TODO: a cli command to execute the above method
        raise NotImplementedError

    async def get_image_resource_range(self, name):
        installed = self.etcd.get(f'images/{name}')
        if installed is None:
            raise RuntimeError('Image metadata is not available!')
        cpu = await self.etcd.get(f'images/{name}/cpu')
        mem = await self.etcd.get(f'images/{name}/mem')
        gpu = await self.etcd.get(f'images/{name}/gpu')
        log.warning(f'name: {name}')
        log.warning(f'resource: {cpu}, {mem}, {gpu}')
        return {
            'cpu': tuple(map(int, cpu.split(','))),
            'mem': tuple(map(int, mem.split(','))),
            'gpu': tuple(map(int, gpu.split(','))),
        }


async def init(app):
    app['config_server'] = ConfigServer(app.config.etcd_addr, app.config.namespace)
    if app['pidx'] == 0:
        await app['config_server'].register_myself(app.config)


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()
