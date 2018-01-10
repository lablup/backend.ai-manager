import asyncio
import json

import pytest


@pytest.fixture
async def prepare_kernel(request, create_app_and_client, get_headers, event_loop,
                         default_keypair):
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

    def finalizer():
        async def fin():
            sess_id = clientSessionToken
            access_key = default_keypair['access_key']
            try:
                await app['registry'].destroy_session(sess_id, access_key)
            except Exception:
                pass
        event_loop.run_until_complete(fin())
    request.addfinalizer(finalizer)

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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kernel_create(prepare_kernel):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    assert 'kernelId' in kernel_info
    assert kernel_info['created']


@pytest.mark.xfail(reason='TODO: header information is lost during request')
@pytest.mark.integration
@pytest.mark.asyncio
async def test_destroy_kernel(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    url = '/v3/kernel/' + kernel_info['kernelId']
    req_bytes = ''.encode('utf-8')
    headers = get_headers('DELETE', url, req_bytes, ctype='text/plain')
    ret = await client.delete(url, data=req_bytes, headers=headers)

    assert ret.status == 201


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_info(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    url = '/v3/kernel/' + kernel_info['kernelId']
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json['lang'] == 'lua:latest'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_logs(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    url = '/v3/kernel/' + kernel_info['kernelId'] + '/logs'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert 'logs' in rsp_json['result']
