'''
Kernel session management.
'''

from datetime import datetime
import logging

from aiohttp import web
from dateutil.parser import parse as dtparse
from dateutil.tz import tzutc
import simplejson as json

from sorna.utils import odict
from sorna.exceptions import SornaError
from .auth import auth_required
from ..manager.registry import InstanceRegistry


log = logging.getLogger('sorna.gateway.server')

# Shortcuts for str.format (TODO: replace with Python 3.6 f-string literals)
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)


@auth_required
async def create(request):
    resp = odict()
    req = await request.json()
    status = 200
    content_type = 'application/json'

    # TODO: quota check using user-db
    try:
        log.info(_f('GET_OR_CREATE (lang:{}, token:{})',
                 req['lang'], req['clientSessionToken']))
        assert len(req['clientSessionToken']) <= 40
    except AssertionError:
        log.warn(_f('GET_OR_CREATE: invalid parameters'))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/invalid-params'
        resp['title'] = 'Invalid API request parameters.'
        resp['detail'] = 'Too long client session token.'
    except KeyError:
        log.warn(_f('GET_OR_CREATE: missing parameters'))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/missing-params'
        resp['title'] = 'There are missing API request parameters.'
    else:
        try:
            # TODO: handle resourceLimits
            kernel = await request.app.registry.get_or_create_kernel(
                req['clientSessionToken'], req['lang'])
            log.info(_f('got/created kernel {} successfully.', kernel.id))
            status = 201  # created
            resp['kernelId'] = kernel.id
        except SornaError as e:
            status = e.http_status
            log.exception(_f('SornaError'))
            resp.update(e.serialize())
    if status > 399:
        content_type = 'application/problem+json'
    return web.Response(status=status,
                        content_type=content_type,
                        text=json.dumps(resp))


@auth_required
async def destroy(request):
    resp = odict()
    status = 200
    content_type = 'application/json'
    kernel_id = request.match_info['kernel_id']

    try:
        log.info(_f('DESTROY (k:{})', kernel_id))
    except KeyError:
        log.warn(_f('DESTROY: missing parameters'))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/missing-params'
        resp['title'] = 'There are missing API request parameters.'
    else:
        try:
            await request.app.registry.destroy_kernel(kernel_id)
            log.info(_f('destroyed successfully.'))
            status = 204  # no content
        except SornaError as e:
            status = e.http_status
            log.exception(_f('SornaError'))
            resp.update(e.serialize())
    if status > 399:
        content_type = 'application/problem+json'
    return web.Response(status=status,
                        content_type=content_type,
                        text=json.dumps(resp))


@auth_required
async def get_info(request):
    resp = odict()
    status = 200
    content_type = 'application/json'
    kernel_id = request.match_info['kernel_id']

    try:
        log.info(_f('GETINFO (k:{})', kernel_id))
    except KeyError:
        log.warn(_f('GETINFO: missing parameters'))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/missing-params'
        resp['title'] = 'There are missing API request parameters.'
    else:
        try:
            kern = await request.app.registry.get_kernel(kernel_id)
            resp['lang'] = kern.lang
            age = datetime.now(tzutc()) - dtparse(kern.created_at)
            resp['age'] = age.total_seconds() * 1000
            # Resource limits collected from agent heartbeats
            # TODO: factor out policy/image info as a common repository
            resp['queryTimeout']  = int(kern.exec_timeout) * 1000
            resp['idleTimeout']   = int(kern.idle_timeout) * 1000
            resp['memoryLimit']   = int(kern.mem_limit)
            resp['maxCpuCredit']  = int(kern.exec_timeout) * 1000
            # Stats collected from agent heartbeats
            resp['numQueriesExecuted'] = int(kern.num_queries)
            resp['idle']          = int(kern.idle) * 1000
            resp['memoryUsed']    = int(kern.mem_max_bytes) // 1024
            resp['cpuCreditUsed'] = int(kern.cpu_used)
            log.info(_f('information retrieved: {!r}', resp))
            status = 200
        except SornaError as e:
            status = e.http_status
            log.exception(_f('SornaError'))
            resp.update(e.serialize())
    if status > 399:
        content_type = 'application/problem+json'
    return web.Response(status=status,
                        content_type=content_type,
                        text=json.dumps(resp))


@auth_required
async def restart(request):
    raise NotImplementedError


async def init(app):
    app.router.add_route('POST', '/v1/kernel/create', create)
    app.router.add_route('GET', '/v1/kernel/{kernel_id}', get_info)
    app.router.add_route('PATCH', '/v1/kernel/{kernel_id}', restart)
    app.router.add_route('DELETE', '/v1/kernel/{kernel_id}', destroy)

    app.registry = InstanceRegistry(app.config.redis_addr)
    await app.registry.init()


async def shutdown(app):
    await app.registry.terminate()
