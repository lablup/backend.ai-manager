import logging

from sorna.common.etcd import AsyncEtcd
from sorna.common.identity import get_instance_id

log = logging.getLogger('sorna.gateway.etcd')


class ConfigServer:

    def __init__(self, etcd):
        self.etcd = etcd

    async def register_myself(self):
        instance_id = await get_instance_id()
        await self.etcd.put(f'nodes/manager', instance_id)

    async def deregister_myself(self):
        await self.etcd.delete_prefix(f'nodes/manager')


async def init(app):
    etcd = AsyncEtcd(app.config.etcd_addr, app.config.namespace)
    app['config_server'] = ConfigServer(etcd)
    await app['config_server'].register_myself()
    # TODO: extract as message bus service
    await app['config_server'].etcd.put('mq/addr', f'{app.config.mq_addr}')
    log.info(f'set mq addr: {app.config.mq_addr}')


async def shutdown(app):
    await app['config_server'].deregister_myself()
