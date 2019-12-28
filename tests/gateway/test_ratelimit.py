import json

import pytest

import ai.backend.gateway.ratelimit as rlim


@pytest.mark.asyncio
async def test_check_rlim_for_non_authrized_query(create_app_and_client):
    app, client = await create_app_and_client(modules=['auth', 'ratelimit'])
    ret = await client.get('/')
    assert ret.status == 200
    assert '1000' == ret.headers['X-RateLimit-Limit']
    assert '1000' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']


@pytest.mark.asyncio
async def test_check_rlim_for_authrized_query(create_app_and_client,
                                              get_headers):
    app, client = await create_app_and_client(modules=['auth', 'ratelimit'])

    url = '/v3/auth/test'
    req_bytes = json.dumps({'echo': 'hello!'}).encode()
    headers = get_headers('POST', url, req_bytes)
    ret = await client.post(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    # The default example keypair's ratelimit is 30000.
    assert '30000' == ret.headers['X-RateLimit-Limit']
    assert '29999' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']
