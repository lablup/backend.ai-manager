import pytest

from ai.backend.gateway.server import LATEST_API_VERSION


@pytest.mark.asyncio
async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    ret = await client.get('/')
    assert ret.status == 200
    assert (await ret.json())['version'] == LATEST_API_VERSION


def test_main(mocker, tmpdir):
    import sys
    import aiotools
    from ai.backend.gateway.server import main

    cmd = ['ai.backend.gateway.server']
    mocker.patch.object(sys, 'argv', cmd)
    mocker.patch.object(aiotools, 'start_server')

    main()

    assert aiotools.start_server.called == 1
