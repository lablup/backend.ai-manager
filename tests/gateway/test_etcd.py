from pathlib import Path
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
async def image_aliases(config_server, tmpdir):
    content = '''
aliases:
  - ['my-python',     'test-python:latest']
  - ['my-python:3.6', 'test-python:3.6-debian']  # preferred
'''
    p = Path(tmpdir) / 'test-image-aliases.yml'
    p.write_text(content)

    yield p

    await config_server.etcd.delete('images/_aliases')


@pytest.fixture
async def volumes(config_server, tmpdir):
    content = '''
volumes:
  - name: test-aws-shard-1
    mount:
      at: requested
      fstype: nfs
      path: "...efs...."
      options: "..."
'''
    p = Path(tmpdir) / 'test-image-aliases.yml'
    p.write_text(content)

    yield p

    await config_server.etcd.delete_prefix('volumes/test-aws-shard-1')


@pytest.mark.asyncio
async def test_register_myself(config_server, mocker):
    instance_id = 'i-test-manager'
    from ai.backend.gateway import etcd as etcd_mod
    mocked_get_instance_id = AsyncMock(return_value=instance_id)
    mocker.patch.object(etcd_mod, 'get_instance_id', mocked_get_instance_id)

    await config_server.register_myself()
    mocked_get_instance_id.await_count == 1
    data = await config_server.etcd.get_prefix(f'nodes/manager/{instance_id}')
    assert data[''] == 'up'

    await config_server.deregister_myself()
    mocked_get_instance_id.await_count == 2
    data = await config_server.etcd.get_prefix(f'nodes/manager/{instance_id}')
    assert len(data) == 0


@pytest.mark.asyncio
async def test_update_aliases_from_file(config_server, image_aliases):
    await config_server.update_aliases_from_file(Path(image_aliases))
    alias_data = await config_server.etcd.get_prefix('images/_aliases')
    assert dict(alias_data) == {
        'my-python': 'test-python:latest',
        'my-python:3.6': 'test-python:3.6-debian',
    }


@pytest.mark.asyncio
async def test_update_volumes_from_file(config_server, volumes):
    name = 'test-aws-shard-1'

    img_data = list(await config_server.etcd.get_prefix(f'volumes/{name}/mount'))
    assert 0 == len(img_data)

    await config_server.update_volumes_from_file(volumes)

    img_data = list(await config_server.etcd.get_prefix(f'volumes/{name}/mount'))
    assert 4 == len(img_data)
