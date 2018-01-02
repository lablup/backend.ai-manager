import pytest

from ai.backend.gateway.etcd import ConfigServer


@pytest.fixture
async def config_server(pre_app):
    server = ConfigServer(pre_app.config.etcd_addr, pre_app.config.namespace)
    yield server
    await server.etcd.delete_prefix('nodes/manager')


class TestConfigServer:
    @pytest.mark.asyncio
    async def test_register_myself(self, pre_app, config_server):
        await config_server.register_myself(pre_app.config)

        assert await config_server.etcd.get('nodes/manager')
        assert await config_server.etcd.get('nodes/redis')
        assert await config_server.etcd.get('nodes/manager/event_addr')

    @pytest.mark.asyncio
    async def test_deregister_myself(self, pre_app, config_server):
        await config_server.register_myself(pre_app.config)
        await config_server.deregister_myself()

        data = list(await config_server.etcd.get_prefix('nodes/manager'))
        assert len(data) == 0
