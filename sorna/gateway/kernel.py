'''
Kernel session management.
'''

import asyncio
from datetime import datetime
import logging

from aiohttp import web
from async_timeout import timeout as _timeout
from dateutil.parser import parse as dtparse
from dateutil.tz import tzutc
import simplejson as json

from sorna.utils import odict
from sorna.exceptions import InvalidAPIParameters, QueryNotImplemented, SornaError
from .auth import auth_required
from ..manager.registry import InstanceRegistry

# Shortcuts for str.format (TODO: replace with Python 3.6 f-string literals)
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_json_type = 'application/json'

log = logging.getLogger('sorna.gateway.server')


@auth_required
async def create(request):
    resp = odict()
    # TODO: quota check using user-db
    try:
        with _timeout(2):
            params = await request.json()
        log.info(_f('GET_OR_CREATE (lang:{}, token:{})',
                 params['lang'], params['clientSessionToken']))
        assert 8 <= len(params['clientSessionToken']) <= 40
    except (asyncio.TimeoutError, AssertionError,
            KeyError, json.decoder.JSONDecodeError):
        log.warn('GET_OR_CREATE: invalid/missing parameters')
        raise InvalidAPIParameters
    try:
        # TODO: handle resourceLimits
        kernel = await request.app.registry.get_or_create_kernel(
            params['clientSessionToken'], params['lang'])
        resp['kernelId'] = kernel.id
    except SornaError:
        log.exception('GET_OR_CREATE: API Internal Error')
        raise
    return web.Response(status=201, content_type=_json_type,
                        text=json.dumps(resp))


@auth_required
async def destroy(request):
    kernel_id = request.match_info['kernel_id']
    log.info(_f('DESTROY (k:{})', kernel_id))
    try:
        await request.app.registry.destroy_kernel(kernel_id)
    except SornaError:
        log.exception('DESTROY: API Internal Error')
        raise
    return web.Response(status=204)


@auth_required
async def get_info(request):
    resp = odict()
    kernel_id = request.match_info['kernel_id']
    log.info(_f('GETINFO (k:{})', kernel_id))
    try:
        kern = await request.app.registry.get_kernel(kernel_id)
        resp['lang'] = kern.lang
        age = datetime.now(tzutc()) - dtparse(kern.created_at)
        resp['age'] = age.total_seconds() * 1000
        # Resource limits collected from agent heartbeats
        # TODO: factor out policy/image info as a common repository
        resp['queryTimeout']  = int(float(kern.exec_timeout) * 1000)
        resp['idleTimeout']   = int(float(kern.idle_timeout) * 1000)
        resp['memoryLimit']   = int(kern.mem_limit)
        resp['maxCpuCredit']  = int(float(kern.exec_timeout) * 1000)
        # Stats collected from agent heartbeats
        resp['numQueriesExecuted'] = int(kern.num_queries)
        resp['idle']          = int(float(kern.idle) * 1000)
        resp['memoryUsed']    = int(kern.mem_max_bytes) // 1024
        resp['cpuCreditUsed'] = int(float(kern.cpu_used))
        log.info(_f('information retrieved: {!r}', resp))
        await request.app.registry.update_kernel(kern, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('GETINFO: API Internal Error')
        raise
    return web.Response(status=200, content_type=_json_type,
                        text=json.dumps(resp))


@auth_required
async def restart(request):
    kernel_id = request.match_info['kernel_id']
    log.info(_f('RESTART (k:{})', kernel_id))
    try:
        kern = await request.app.registry.get_kernel(kernel_id)
        await request.app.registry.restart_kernel(kernel_id)
        await request.app.registry.update_kernel(kernel_id, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('RESTART: API Internal Error')
        raise
    return web.Response(status=204)


@auth_required
async def execute_snippet(request):
    resp = odict()
    kernel_id = request.match_info['kernel_id']
    try:
        with _timeout(2):
            params = await request.json()
        log.info(_f('EXECUTE_SNIPPET (k:{})', kernel_id))
    except (asyncio.TimeoutError, json.decoder.JSONDecodeError):
        log.warn('EXECUTE_SNIPPET: invalid/missing parameters')
        raise InvalidAPIParameters
    try:
        kern = await request.app.registry.get_kernel(kernel_id)
        resp['result'] = await request.app.registry.execute_snippet(
            kernel_id, params['codeId'], params['code'])
        await request.app.registry.update_kernel(kernel_id, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('EXECUTE_SNIPPET: API Internal Error')
        raise
    return web.Response(status=200, content_type=_json_type,
                        text=json.dumps(resp))


async def stream_pty(request):
    # TODO: migrate pub/sub part of webterm proxy from neumann
    raise QueryNotImplemented


async def stream_events(request):
    raise QueryNotImplemented


async def init(app):
    app.router.add_route('POST', '/v1/kernel/create', create)
    app.router.add_route('GET', '/v1/kernel/{kernel_id}', get_info)
    app.router.add_route('PATCH', '/v1/kernel/{kernel_id}', restart)
    app.router.add_route('DELETE', '/v1/kernel/{kernel_id}', destroy)
    app.router.add_route('POST', '/v1/kernel/{kernel_id}', execute_snippet)
    app.router.add_route('POST', '/v1/stream/kernel/{kernel_id}/pty', stream_pty)
    app.router.add_route('POST', '/v1/stream/kernel/{kernel_id}/events', stream_events)

    app.registry = InstanceRegistry(app.config.redis_addr)
    await app.registry.init()


async def shutdown(app):
    await app.registry.terminate()
