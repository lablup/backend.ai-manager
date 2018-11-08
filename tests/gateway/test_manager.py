from argparse import Namespace
import asyncio
import json
import os
from unittest import mock

import pytest

from ai.backend.common.argparse import host_port_pair
from ai.backend.gateway.exceptions import ServerFrozen
from ai.backend.gateway.manager import (
    server_unfrozen_required, GQLMutationUnfrozenRequiredMiddleware, ManagerStatus)
from ai.backend.manager.cli.etcd import put


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

    await app['config_server'].update_manager_status(ManagerStatus.FROZEN)

    @server_unfrozen_required
    async def _dummy_handler(request):
        return 'dummy-response'

    class DummyRequest:
        def __init__(self, app):
            self.app = app

    with pytest.raises(ServerFrozen):
        await _dummy_handler(DummyRequest(app))

    await app['config_server'].update_manager_status(ManagerStatus.RUNNING)
    await asyncio.sleep(0.5)  # Wait until manager detects status update

    result = await _dummy_handler(DummyRequest(app))
    assert result == 'dummy-response'


@pytest.mark.asyncio
async def test_gql_mutation_unfrozen_required_middleware():
    middleware = GQLMutationUnfrozenRequiredMiddleware()
    mock_operation = mock.MagicMock(operation='query')
    mock_info = mock.MagicMock(
        operation=mock_operation,
        context={'manager_status': ManagerStatus.RUNNING}
    )
    middleware.resolve(lambda root, info: print(root, info), None, mock_info)

    mock_operation = mock.MagicMock(operation='mutation')
    mock_info = mock.MagicMock(
        operation=mock_operation,
        context={'manager_status': ManagerStatus.RUNNING}
    )
    middleware.resolve(lambda root, info: print(root, info), None, mock_info)

    mock_operation = mock.MagicMock(operation='query')
    mock_info = mock.MagicMock(
        operation=mock_operation,
        context={'manager_status': ManagerStatus.FROZEN}
    )
    middleware.resolve(lambda root, info: print(root, info), None, mock_info)

    mock_operation = mock.MagicMock(operation='mutation')
    mock_info = mock.MagicMock(
        operation=mock_operation,
        context={'manager_status': ManagerStatus.FROZEN}
    )
    with pytest.raises(ServerFrozen):
        middleware.resolve(lambda root, info: print(root, info), None, mock_info)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_manager_status(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel

    url = '/manager/status'
    req_bytes = b''
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json.get('status', None) == ManagerStatus.RUNNING.value
    assert rsp_json.get('active_sessions', None) == 0

    kernel_info = await create_kernel()

    assert 'kernelId' in kernel_info
    assert kernel_info.get('created', None)

    ret = await client.get(url, data=req_bytes, headers=headers)

    rsp_json = await ret.json()
    assert rsp_json.get('status', None) == ManagerStatus.RUNNING.value
    assert rsp_json.get('active_sessions', None) == 1


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_manager_status_opt_force_kill(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel

    kernel_info = await create_kernel()

    assert 'kernelId' in kernel_info
    assert kernel_info.get('created', None)

    url = '/manager/status'
    req_bytes = json.dumps({'status': 'frozen', 'force_kill': True}).encode()
    headers = get_headers('PUT', url, req_bytes)
    ret = await client.put(url, data=req_bytes, headers=headers)

    assert ret.status == 204

    status = await app['config_server'].get_manager_status()
    assert status == ManagerStatus.FROZEN

    url = '/manager/status'
    req_bytes = b''
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json.get('status', None) == ManagerStatus.FROZEN.value
    assert rsp_json.get('active_sessions', None) == 0


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
