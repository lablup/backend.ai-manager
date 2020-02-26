from unittest.mock import MagicMock, AsyncMock

import snappy
from sqlalchemy.sql.dml import Insert, Update

from ai.backend.manager.registry import AgentRegistry
from ai.backend.manager.models import AgentStatus
from ai.backend.common import msgpack
from ai.backend.common.types import ResourceSlot


async def test_handle_heartbeat(mocker):
    mock_config_server = MagicMock()
    mock_config_server.update_resource_slots = AsyncMock()
    mock_config_server.etcd = None
    mock_dbpool = MagicMock()
    mock_dbconn = MagicMock()
    mock_dbconn_ctx = MagicMock()
    mock_dbtxn_ctx = MagicMock()
    mock_dbresult = MagicMock()
    mock_dbresult.rowcount = 1
    mock_dbpool.acquire = MagicMock(return_value=mock_dbconn_ctx)
    mock_dbconn_ctx.__aenter__ = AsyncMock(return_value=mock_dbconn)
    mock_dbconn_ctx.__aexit__ = AsyncMock()
    mock_dbconn.execute = AsyncMock(return_value=mock_dbresult)
    mock_dbconn.begin = MagicMock(return_value=mock_dbtxn_ctx)
    mock_dbtxn_ctx.__aenter__ = AsyncMock()
    mock_dbtxn_ctx.__aexit__ = AsyncMock()
    mock_redis_stat = MagicMock()
    mock_redis_live = MagicMock()
    mock_redis_live.hset = AsyncMock()
    mock_redis_image = MagicMock()
    mock_event_dispatcher = MagicMock()
    mock_event_dispatcher.produce_event = AsyncMock()
    mock_get_known_registries = AsyncMock(return_value=[
        {'index.docker.io': 'https://registry-1.docker.io'},
    ])
    mocker.patch('ai.backend.manager.registry.get_known_registries', mock_get_known_registries)
    mock_redis_wrapper = MagicMock()
    mock_redis_wrapper.execute_with_retries = AsyncMock()
    mocker.patch('ai.backend.manager.registry.redis', mock_redis_wrapper)
    image_data = snappy.compress(msgpack.packb([
        ('index.docker.io/lablup/python:3.6-ubuntu18.04', ),
    ]))

    registry = AgentRegistry(
        config_server=mock_config_server,
        dbpool=mock_dbpool,
        redis_stat=mock_redis_stat,
        redis_live=mock_redis_live,
        redis_image=mock_redis_image,
        event_dispatcher=mock_event_dispatcher,
    )
    await registry.init()

    # Join
    mock_dbresult.first = AsyncMock(return_value=None)
    await registry.handle_heartbeat('i-001', {
        'scaling_group': 'sg-testing',
        'resource_slots': {'cpu': ('count', '1'), 'mem': ('bytes', '1g')},
        'region': 'ap-northeast-2',
        'addr': '10.0.0.5',
        'version': '19.12.0',
        'compute_plugins': [],
        'images': image_data,
    })
    mock_config_server.update_resource_slots.assert_awaited_once()
    q = mock_dbconn.execute.await_args_list[1].args[0]
    assert isinstance(q, Insert)

    # Update alive instance
    mock_config_server.update_resource_slots.reset_mock()
    mock_dbconn.execute.reset_mock()
    mock_dbresult.first = AsyncMock(return_value={
        'status': AgentStatus.ALIVE,
        'addr': '10.0.0.5',
        'scaling_group': 'sg-testing',
        'available_slots': ResourceSlot({'cpu': '1', 'mem': '1g'}),
    })
    await registry.handle_heartbeat('i-001', {
        'scaling_group': 'sg-testing',
        'resource_slots': {'cpu': ('count', '1'), 'mem': ('bytes', '2g')},
        'region': 'ap-northeast-2',
        'addr': '10.0.0.6',
        'version': '19.12.0',
        'compute_plugins': [],
        'images': image_data,
    })
    mock_config_server.update_resource_slots.assert_awaited_once()
    q = mock_dbconn.execute.await_args_list[1].args[0]
    assert isinstance(q, Update)
    assert q.parameters['addr'] == '10.0.0.6'
    assert q.parameters['available_slots'] == ResourceSlot({'cpu': '1', 'mem': '2g'})
    assert 'scaling_group' not in q.parameters

    # Rejoin
    mock_config_server.update_resource_slots.reset_mock()
    mock_dbconn.execute.reset_mock()
    mock_dbresult.first = AsyncMock(return_value={
        'status': AgentStatus.LOST,
        'addr': '10.0.0.5',
        'scaling_group': 'sg-testing',
        'available_slots': ResourceSlot({'cpu': '1', 'mem': '1g'}),
    })
    await registry.handle_heartbeat('i-001', {
        'scaling_group': 'sg-testing2',
        'resource_slots': {'cpu': ('count', '4'), 'mem': ('bytes', '2g')},
        'region': 'ap-northeast-2',
        'addr': '10.0.0.6',
        'version': '19.12.0',
        'compute_plugins': [],
        'images': image_data,
    })
    mock_config_server.update_resource_slots.assert_awaited_once()
    q = mock_dbconn.execute.await_args_list[1].args[0]
    assert isinstance(q, Update)
    assert q.parameters['status'] == AgentStatus.ALIVE
    assert q.parameters['addr'] == '10.0.0.6'
    assert q.parameters['lost_at'] is None
    assert q.parameters['available_slots'] == ResourceSlot({'cpu': '4', 'mem': '2g'})
    assert q.parameters['scaling_group'] == 'sg-testing2'
    assert 'compute_plugins' in q.parameters
    assert 'version' in q.parameters
