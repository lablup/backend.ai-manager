import asyncio
import json

import pytest


@pytest.fixture
async def prepare_kernel(request, create_app_and_client, get_headers, event_loop):
    clientSessionToken = 'test-token-0000'
    app, client = await create_app_and_client(
        extras=['etcd', 'events', 'auth', 'vfolder', 'admin', 'ratelimit', 'kernel'],
        ev_router=True)

    async def wait_for_agent():
        while True:
            all_ids = [inst_id async for inst_id in
                       app['registry'].enumerate_instances()]
            if len(all_ids) > 0:
                break
            await asyncio.sleep(1)
    task = event_loop.create_task(wait_for_agent())
    await task

    async def create_kernel():
        url = '/v3/kernel/'
        req_bytes = json.dumps({
            'lang': 'lua:latest',
            'clientSessionToken': clientSessionToken,
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status == 201
        rsp_json = await ret.json()
        assert rsp_json['kernelId'] == clientSessionToken

        return rsp_json

    return app, client, create_kernel


@pytest.mark.asyncio
async def test_kernel_create(prepare_kernel):
    app, client, create_kernel = prepare_kernel
    rsp_json = await create_kernel()

    assert 'kernelId' in rsp_json
    assert rsp_json['created']
