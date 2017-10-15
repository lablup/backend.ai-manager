from aiohttp import web

from ai.backend.gateway.server import api_middleware_factory
from ai.backend.gateway.utils import method_placeholder


async def test_api_method_override(test_client):
    observed_method = None
    app = web.Application()

    async def service_handler(request):
        nonlocal observed_method
        observed_method = request.method
        return web.Response(body=b'test')

    app.router.add_route('POST', '/v{version:\d+}/test',
                         method_placeholder('REPORT'))
    app.router.add_route('REPORT', '/v{version:\d+}/test',
                         service_handler)
    app.middlewares.append(api_middleware_factory)
    client = await test_client(app)

    # native method
    resp = await client.request('REPORT', '/v2/test')
    assert resp.status == 200
    assert (await resp.read()) == b'test'
    assert observed_method == 'REPORT'

    # overrided method
    observed_method = None
    resp = await client.post('/v2/test', headers={
        'X-Method-Override': 'REPORT',
    })
    assert resp.status == 200
    assert (await resp.read()) == b'test'
    assert observed_method == 'REPORT'

    # calling placeholder
    observed_method = None
    resp = await client.post('/v2/test')
    assert resp.status == 405
    assert observed_method is None

    # calling with non-relevant method
    observed_method = None
    resp = await client.delete('/v2/test')
    assert resp.status == 405
    assert observed_method is None


async def test_api_ver(test_client):
    inner_request = None
    app = web.Application()

    async def dummy_handler(request):
        nonlocal inner_request
        inner_request = request
        return web.Response(body=b'test')

    app.router.add_post('/v{version:\d+}/test', dummy_handler)
    app.middlewares.append(api_middleware_factory)
    client = await test_client(app)

    # normal call
    resp = await client.post('/v1/test', headers={
        'X-BackendAI-Version': 'v1.20160915',
    })
    assert resp.status == 200
    assert inner_request['api_version'] == 1

    # calling without version prefix
    resp = await client.post('/test', headers={
        'X-BackendAI-Version': 'v2.20170315',
    })
    assert resp.status == 404

    # normal call with different version
    resp = await client.post('/v2/test', headers={
        'X-BackendAI-Version': 'v2.20170315',
    })
    assert resp.status == 200
    assert inner_request['api_version'] == 2

    # calling with invalid version
    resp = await client.post('/v0/test', headers={
        'X-BackendAI-Version': 'v2.20170315',
    })
    assert resp.status == 400
    assert 'Unsupported' in (await resp.text())
