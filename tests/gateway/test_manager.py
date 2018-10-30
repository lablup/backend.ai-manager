from argparse import Namespace
import asyncio
import json
import os

import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.exceptions import ServerFrozen
from ai.backend.gateway.manager import server_unfrozen_required, ManagerStatus
from ai.backend.manager.cli.etcd import put


def test_manager_status_is_member():
    assert ManagerStatus.is_member('running')
    assert ManagerStatus.is_member('frozen')
    assert not ManagerStatus.is_member('freeze')
    assert not ManagerStatus.is_member('stop')
    assert not ManagerStatus.is_member('terminated')
    assert not ManagerStatus.is_member('')


@pytest.fixture
def dirty_etcd(test_ns):
    etcd_addr = host_port_pair(os.environ['BACKEND_ETCD_ADDR'])
    args = Namespace(key='manager/status', value='frozen',
                     etcd_addr=etcd_addr, namespace=test_ns)
    put(args)


@pytest.mark.asyncio
async def test_server_status_initalization(create_app_and_client, dirty_etcd):
    app, client = await create_app_and_client(modules=['etcd', 'manager'])
    # Wait until initialization of manager subapp is complete.
    await asyncio.sleep(0.5)

    status = await app['config_server'].get_manager_status()
    assert status == ManagerStatus.RUNNING


@pytest.fixture
async def prepare_manager(create_app_and_client):
    app, client = await create_app_and_client(modules=['etcd', 'manager'])
    return app, client


@pytest.mark.asyncio
async def test_server_unfrozen_required(prepare_manager):
    app, client = prepare_manager

    await app['config_server'].update_manager_status('frozen')

    @server_unfrozen_required
    async def _dummy_handler(request):
        return 'dummy-response'

    class DummyRequest:
        def __init__(self, app):
            self.app = app

    with pytest.raises(ServerFrozen):
        await _dummy_handler(DummyRequest(app))

    await app['config_server'].update_manager_status('running')
    await asyncio.sleep(0.5)  # Wait until manager detects status update

    result = await _dummy_handler(DummyRequest(app))
    assert result == 'dummy-response'


@pytest.mark.asyncio
async def test_server_unfrozen_required_gql(prepare_manager):
    app, client = prepare_manager

    await app['config_server'].update_manager_status('frozen')

    @server_unfrozen_required(gql=True)
    async def _dummy_handler(request):
        return 'dummy-response'

    class DummyQueryRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {'query': 'query() {}'}

    class DummyMutationRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {'query': 'mutation() {}'}

    result = await _dummy_handler(DummyQueryRequest(app))
    assert result == 'dummy-response'
    with pytest.raises(ServerFrozen):
        await _dummy_handler(DummyMutationRequest(app))

    await app['config_server'].update_manager_status('running')
    await asyncio.sleep(0.5)  # Wait until manager detects status update

    result = await _dummy_handler(DummyQueryRequest(app))
    assert result == 'dummy-response'
    result = await _dummy_handler(DummyMutationRequest(app))
    assert result == 'dummy-response'


@pytest.mark.asyncio
async def test_server_unfrozen_required_invalid_usage():
    with pytest.raises(ValueError):
        @server_unfrozen_required()
        async def _dummy_handler(request):
            return 'dummy-response'


@pytest.mark.asyncio
async def test_fetch_manager_status(prepare_manager, get_headers):
    app, client = prepare_manager

    url = '/manager/status'
    req_bytes = b''
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json.get('status', None) == ManagerStatus.RUNNING.value
    assert rsp_json.get('active_sessions', None) == 0


@pytest.mark.asyncio
async def test_update_manager_status(prepare_manager, get_headers):
    app, client = prepare_manager

    url = '/manager/status'
    req_bytes = json.dumps({'status': 'frozen'}).encode()
    headers = get_headers('PUT', url, req_bytes)
    ret = await client.put(url, data=req_bytes, headers=headers)

    assert ret.status == 204

    status = await app['config_server'].get_manager_status()
    assert status == ManagerStatus.FROZEN

    req_bytes = json.dumps({'status': 'running'}).encode()
    headers = get_headers('PUT', url, req_bytes)
    ret = await client.put(url, data=req_bytes, headers=headers)

    assert ret.status == 204

    status = await app['config_server'].get_manager_status()
    assert status == ManagerStatus.RUNNING


@pytest.mark.asyncio
async def test_update_manager_status_with_wrong_params(prepare_manager, get_headers):
    app, client = prepare_manager

    url = '/manager/status'
    req_bytes = b''
    headers = get_headers('PUT', url, req_bytes)
    ret = await client.put(url, data=req_bytes, headers=headers)
    assert ret.status == 400

    req_bytes = json.dumps({'status': 'malformed-status'}).encode()
    headers = get_headers('PUT', url, req_bytes)
    ret = await client.put(url, data=req_bytes, headers=headers)
    assert ret.status == 400
