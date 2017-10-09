import logging

from sorna.common.etcd import AsyncEtcd
from sorna.common.identity import get_instance_id, get_instance_ip

log = logging.getLogger('sorna.gateway.etcd')


class ConfigServer:

    def __init__(self, etcd):
        self.etcd = etcd

    async def register_myself(self, instance_id):
        await self.etcd.put('nodes/manager', instance_id)

    async def deregister_myself(self):
        await self.etcd.delete_prefix('nodes/manager')

    async def update_kernel_images(self):
        pass
        # TODO: a method to scan docker hub and update kernel image versions
        # TODO: a cli command to execute the above method


async def init(app):
    etcd = AsyncEtcd(app.config.etcd_addr, app.config.namespace)
    instance_id = await get_instance_id()
    instance_ip = await get_instance_ip()
    app['config_server'] = ConfigServer(etcd)
    await app['config_server'].register_myself(instance_id)
    await etcd.put('nodes/redis', app.config.redis_addr)
    event_addr = f'{instance_ip}:{app.config.events_port}'
    await etcd.put('nodes/manager/event_addr', event_addr)


async def shutdown(app):
    await app['config_server'].deregister_myself()
