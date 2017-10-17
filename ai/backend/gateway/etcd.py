import logging

from ai.backend.common.identity import get_instance_id, get_instance_ip

log = logging.getLogger('ai.backend.gateway.etcd')


class ConfigServer:

    def __init__(self, config):
        # WARNING: importing etcd3/grpc must be done after forks.
        self.config = config
        from ai.backend.common.etcd import AsyncEtcd
        self.etcd = AsyncEtcd(config.etcd_addr, config.namespace)

    async def register_myself(self):
        instance_id = await get_instance_id()
        instance_ip = await get_instance_ip()
        event_addr = f'{instance_ip}:{self.config.events_port}'
        await self.etcd.put_multi(
            ['nodes/manager', 'nodes/redis', 'nodes/manager/event_addr'],
            [instance_id, self.config.redis_addr, event_addr])

    async def deregister_myself(self):
        await self.etcd.delete_prefix('nodes/manager')

    async def update_kernel_images(self):
        pass
        # TODO: a method to scan docker hub and update kernel image versions
        # TODO: a cli command to execute the above method


async def init(app):
    app['config_server'] = ConfigServer(app.config)
    await app['config_server'].register_myself()


async def shutdown(app):
    await app['config_server'].deregister_myself()
