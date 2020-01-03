import json

import pytest

from ai.backend.gateway.server import (
    config_server_ctx, redis_ctx, database_ctx,
)
import ai.backend.gateway.ratelimit as rlim


@pytest.mark.asyncio
async def test_check_rlim_for_anonymous_query(etcd_fixture, database_fixture,
                                              create_app_and_client):
    app, client = await create_app_and_client(
        [config_server_ctx, redis_ctx, database_ctx],
        ['.auth', '.ratelimit'],
    )
    ret = await client.get('/')
    assert ret.status == 200
    assert '1000' == ret.headers['X-RateLimit-Limit']
    assert '1000' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']


@pytest.mark.asyncio
async def test_check_rlim_for_authorized_query(etcd_fixture, database_fixture,
                                               create_app_and_client,
                                               get_headers):
    app, client = await create_app_and_client(
        [config_server_ctx, redis_ctx, database_ctx],
        ['.auth', '.ratelimit'],
    )
    url = '/auth/test'
    req_bytes = json.dumps({'echo': 'hello!'}).encode()
    headers = get_headers('POST', url, req_bytes)
    ret = await client.post(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    # The default example keypair's ratelimit is 30000.
    assert '30000' == ret.headers['X-RateLimit-Limit']
    assert '29999' == ret.headers['X-RateLimit-Remaining']
    assert str(rlim._rlim_window) == ret.headers['X-RateLimit-Window']
