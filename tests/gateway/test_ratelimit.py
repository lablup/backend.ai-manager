import json

import pytest

import ai.backend.gateway.ratelimit as rlim


@pytest.mark.asyncio
async def test_check_rlim_for_non_authrized_query(create_app_and_client,
                                                  get_headers):
    app, client = await create_app_and_client(extras=['auth', 'ratelimit'])

    url = '/v3'
    req_bytes = json.dumps({'echo': 'hello!'}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=b'no match', headers=headers)

    assert ret.status == 200
    assert '1000' == ret.headers['X-RateLimit-Limit']
    assert '1000' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']


@pytest.mark.asyncio
async def test_check_rlim_for_authrized_query(create_app_and_client,
                                              get_headers):
    app, client = await create_app_and_client(extras=['auth', 'ratelimit'])

    url = '/v3'
    req_bytes = json.dumps({'echo': 'hello!'}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    assert '1000' == ret.headers['X-RateLimit-Limit']
    assert '999' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']
