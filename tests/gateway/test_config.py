from pathlib import Path
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
async def image_aliases(shared_config, tmpdir):
    content = '''
aliases:
  - ['my-python',     'test-python:latest']
  - ['my-python:3.6', 'test-python:3.6-debian']  # preferred
'''
    p = Path(tmpdir) / 'test-image-aliases.yml'
    p.write_text(content)

    yield p

    await shared_config.etcd.delete('images/_aliases')


@pytest.mark.asyncio
async def test_register_myself(shared_config, mocker):
    instance_id = 'i-test-manager'
    from ai.backend.gateway import config as config_mod
    mocked_get_instance_id = AsyncMock(return_value=instance_id)
    mocker.patch.object(config_mod, 'get_instance_id', mocked_get_instance_id)

    await shared_config.register_myself()
    mocked_get_instance_id.await_count == 1
    data = await shared_config.etcd.get_prefix(f'nodes/manager/{instance_id}')
    assert data[''] == 'up'

    await shared_config.deregister_myself()
    mocked_get_instance_id.await_count == 2
    data = await shared_config.etcd.get_prefix(f'nodes/manager/{instance_id}')
    assert len(data) == 0


@pytest.mark.asyncio
async def test_update_aliases_from_file(shared_config, image_aliases):
    await shared_config.update_aliases_from_file(Path(image_aliases))
    alias_data = await shared_config.etcd.get_prefix('images/_aliases')
    assert dict(alias_data) == {
        'my-python': 'test-python:latest',
        'my-python:3.6': 'test-python:3.6-debian',
    }
