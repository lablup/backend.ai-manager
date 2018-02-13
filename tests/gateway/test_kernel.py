import asyncio
import json
import time

import aiohttp
import pytest

from ai.backend.gateway.exceptions import KernelNotFound


@pytest.fixture
async def prepare_kernel(request, create_app_and_client, get_headers, event_loop,
                         default_keypair):
    clientSessionToken = 'test-token-0000'
    app, client = await create_app_and_client(
        extras=['etcd', 'events', 'auth', 'vfolder', 'admin', 'ratelimit', 'kernel'],
        spawn_agent=True,
        ev_router=True)

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
    return app, client, create_kernel


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kernel_create(prepare_kernel):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    assert 'kernelId' in kernel_info
    assert kernel_info['created']


@pytest.mark.integration
@pytest.mark.asyncio
async def test_destroy_kernel(prepare_kernel, get_headers, default_keypair):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    assert await app['registry'].get_session(kernel_info['kernelId'],
                                             default_keypair['access_key'])

    url = '/v3/kernel/' + kernel_info['kernelId']
    req_bytes = json.dumps({}).encode('utf-8')
    headers = get_headers('DELETE', url, req_bytes)
    ret = await client.delete(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    with pytest.raises(KernelNotFound):
        await app['registry'].get_session(kernel_info['kernelId'],
                                          default_keypair['access_key'])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restart_kernel(prepare_kernel, get_headers, default_keypair):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    sess_id = kernel_info['kernelId']
    access_key = default_keypair['access_key']
    kern = await app['registry'].get_session(sess_id, access_key, field='*')
    original_cid = kern.container_id

    url = '/v3/kernel/' + kernel_info['kernelId']
    req_bytes = json.dumps({}).encode()
    headers = get_headers('PATCH', url, req_bytes)
    ret = await client.patch(url, data=req_bytes, headers=headers)

    assert ret.status == 204
    kern = await app['registry'].get_session(sess_id, access_key, field='*')
    assert original_cid != kern.container_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute(prepare_kernel, get_headers):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    url = '/v2/kernel/' + kernel_info['kernelId']
    req_bytes = json.dumps({
        'runId': 'test-runid',
        'mode': 'query',
        'code': 'print(135)',
    }).encode()
    headers = get_headers('POST', url, req_bytes)
    timeout = time.time() + 20
    while True:
        ret = await client.post(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()
        if time.time() > timeout:
            raise TimeoutError('Code execution timeout')
        if rsp_json['result']['status'] == 'finished':
            break
        elif rsp_json['result']['status'] == 'continued':
            req_bytes = json.dumps({
                'runId': 'test-runid',
                'mode': 'continue',
                'code': '',
            }).encode()
            headers = get_headers('POST', url, req_bytes)
        else:
            raise Exception('Invalid execution status')

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json['result']['status'] == 'finished'
    assert rsp_json['result']['runId'] == 'test-runid'
    assert rsp_json['result']['console'][0][0] == 'stdout'
    assert rsp_json['result']['console'][0][1] == '135\n'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restart_kernel_cancel_execution(prepare_kernel, get_headers,
                                               event_loop):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    async def execute_code():
        url = '/v2/kernel/' + kernel_info['kernelId']
        code = ('local clock = os.clock\n'
                'function sleep(n)\n'
                '  local t0 = clock()\n'
                '  while clock() - t0 <= n do end\n'
                'end\n'
                'sleep(10)\nprint("code executed")')
        req_bytes = json.dumps({
            'runId': 'test-runid',
            'mode': 'query',
            'code': code,
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        timeout = time.time() + 20
        while True:
            ret = await client.post(url, data=req_bytes, headers=headers)
            rsp_json = await ret.json()
            if time.time() > timeout:
                raise TimeoutError('Code execution timeout')
            if ret is None:
                break
            elif rsp_json['result']['status'] == 'finished':
                break
            elif rsp_json['result']['status'] == 'continued':
                req_bytes = json.dumps({
                    'runId': 'test-runid',
                    'mode': 'continue',
                    'code': '',
                }).encode()
                headers = get_headers('POST', url, req_bytes)
            else:
                raise Exception('Invalid execution status')
        return ret

    async def restart():
        url = '/v3/kernel/' + kernel_info['kernelId']
        req_bytes = json.dumps({}).encode()
        headers = get_headers('PATCH', url, req_bytes)
        ret = await client.patch(url, data=req_bytes, headers=headers)

        return ret

    from datetime import datetime
    start = datetime.now()
    t1 = asyncio.ensure_future(execute_code(), loop=event_loop)
    await asyncio.sleep(1)
    t2 = asyncio.ensure_future(restart(), loop=event_loop)
    ret = await t2
    end = datetime.now()

    assert ret.status == 204
    assert t1.exception()
    assert t2.exception() is None
    assert (end - start).total_seconds() < 10


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upload_files(prepare_kernel, get_headers, tmpdir):
    app, client, create_kernel = prepare_kernel
    kernel_info = await create_kernel()

    # Create files
    p1 = tmpdir.join('test1.txt')
    p1.write('1357')
    p2 = tmpdir.join('test2.txt')
    p2.write('35711')

    # Prepare form data
    data = aiohttp.FormData()
    data.add_field('file', open(p1, 'rb'))
    data.add_field('file', open(p2, 'rb'))

    url = '/v3/kernel/' + kernel_info['kernelId'] + '/upload'
    headers = get_headers('POST', url, b'', ctype='multipart/form-data')
    ret = await client.post(url, data=data, headers=headers)

    assert ret.status == 204

    # TODO: Check existence of files in the kernel folder.
    #       Way to get the scratch root of agent from here?
    # from pathlib import Path
    # import sqlalchemy as sa
    # from ai.backend.gateway.vfolder import VF_ROOT
    # from ai.backend.manager.models import agents, kernels
    # async with app['registry'].dbpool.acquire() as conn:
    #     cols = [kernels.c.id, kernels.c.sess_id,
    #             kernels.c.agent_addr, kernels.c.access_key]
    #     cols = [kernels.c.id]
    #     query = (sa.select(cols)
    #                .where(kernel_info['kernelId'] == kernels.c.sess_id)
    #                .limit(1).offset(0))
    # result = await conn.execute(query)
    # row = await result.first()
    # root = '/private/tmp/sorna-volumes'
    # kernel_dir = Path(root) / row.id
    # Get paths for files uploaded to virtual folder
    # vf_fname1 = p1.strpath.split('/')[-1]
    # vf_fname2 = p2.strpath.split('/')[-1]
    # assert 2 == len(list(kernel_dir.glob('**/*.txt')))
    # assert (kernel_dir / vf_fname1).exists()
    # assert (kernel_dir / vf_fname2).exists()


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
