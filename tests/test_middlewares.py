import pytest
from aiohttp.test_utils import make_mocked_request
from aiohttp.web import Response

from sorna.gateway.server import api_middleware_factory


@pytest.mark.asyncio
async def test_method_override():
    inner_request = None
    dummy_app = dict()

    async def dummy_handler(request):
        nonlocal inner_request
        inner_request = request
        return Response(body=b'test')

    mw = await api_middleware_factory(dummy_app, dummy_handler)

    rqst = make_mocked_request('POST', '/v2/version', headers={
        'X-Method-Override': 'REPORT',
        'X-Sorna-Version': 'v2.20170315',
    })
    resp = await mw(rqst)
    assert inner_request.method == 'REPORT'

    rqst = make_mocked_request('POST', '/v2/version', headers={
        'X-Sorna-Version': 'v2.20170315',
    })
    resp = await mw(rqst)
    assert inner_request.method == 'POST'


@pytest.mark.asyncio
async def test_api_ver():
    inner_request = None
    dummy_app = dict()

    async def dummy_handler(request):
        nonlocal inner_request
        inner_request = request
        return Response(body=b'test')

    mw = await api_middleware_factory(dummy_app, dummy_handler)

    rqst = make_mocked_request('POST', '/v1/version', headers={
        'X-Sorna-Version': 'v1.20160915',
    })
    resp = await mw(rqst)
    assert inner_request['api_version'] == 1

    rqst = make_mocked_request('POST', '/v2/version', headers={
        'X-Sorna-Version': 'v2.20170315',
    })
    resp = await mw(rqst)
    assert inner_request['api_version'] == 2
