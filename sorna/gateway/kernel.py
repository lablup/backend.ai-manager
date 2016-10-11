'''
Kernel session management.
'''

import asyncio
import logging

from aiohttp import web
import asyncpg
import simplejson as json

from sorna.utils import odict
from sorna.exceptions import *
from .auth import auth_required
from ..manager.registry import InstanceRegistry


log = logging.getLogger('sorna.gateway.server')

# Shortcuts for str.format (TODO: replace with Python 3.6 f-string literals)
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)
_r = lambda fmt, req_id, *args, **kwargs: 'request[{}]: '.format(req_id) + fmt.format(*args, **kwargs)


@auth_required
async def create(request):
    resp = odict()
    req = await request.json()
    status = 200
    content_type = 'application/problem+json'

    # TODO: quota check using user-db
    try:
        log.info(_r('GET_OR_CREATE (lang:{}, token:{})', request_id,
                 req['lang'], req['clientSessionToken']))
        assert len(req['clientSessionToken']) <= 40
    except AssertionError:
        log.warn(_r('GET_OR_CREATE: invalid parameters', request_id))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/invalid-params'
        resp['title'] = 'Invalid API request parameters.'
        resp['detail'] = 'Too long client session token.'
    except KeyError:
        log.warn(_r('GET_OR_CREATE: invalid parameters', request_id))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/invalid-params'
        resp['title'] = 'Invalid API request parameters.'
        resp['detail'] = 'Missing API parameters.'
    if status != 200:  # input validation failed!
        return web.Response(status=status,
                            content_type=content_type,
                            text=json.dumps(resp))

    try:
        # TODO: handle resourceLimits
        kernel = await registry.get_or_create_kernel(req['clientSessionToken'],
                                                     req['lang'])
        log.info(_r('got/created kernel {} successfully.', request_id, kernel.id))
        status = 201  # created
        resp['kernelId'] = kernel.id
    except InstanceNotAvailableError:
        status = 503  # service temporarily unavailable
        log.error(_r('instance not available', request_id))
        resp['type'] = 'https://api.sorna.io/probs/instance-not-available'
        resp['title'] = 'There is no available instance.'
        # TODO: spawn new instances (scale-up)
    except KernelCreationFailedError:
        status = 500  # internal server error
        log.error(_r('kernel creation failed', request_id))
        resp['type'] = 'https://api.sorna.io/probs/kernel-creation-failed'
        resp['title'] = 'Kernel creation has failed.'
    except QuotaExceededError:
        status = 412  # precondition failed
        log.error(_r('quota exceeded', request_id))
        resp['type'] = 'https://api.sorna.io/probs/quota-exceeded'
        resp['title'] = 'You have reached your resource limit.'
        resp['detail'] = 'You cannot create more kernels.'
    if status == 204:
        content_type = 'application/json'
    return web.Response(status=status,
                        content_type=content_type,
                        text=json.dumps(resp))


@auth_required
async def destroy(request):
    resp = odict()
    status = 200
    kernel_id = request.match_info['kernel_id']
    content_type = 'application/problem+json'

    try:
        log.info(_r('DESTROY (k:{})', request_id, kernel_id))
    except KeyError:
        log.warn(_r('DESTROY: invalid parameters', request_id))
        status = 400
        resp['type'] = 'https://api.sorna.io/probs/invalid-params'
        resp['title'] = 'Invalid API request parameters.'
        resp['detail'] = 'Missing API parameters.'
    if status != 200:  # input validation failed
        return web.Response(status=status,
                            content_type=content_type,
                            text=json.dumps(resp))

    # TODO: assert if session matches with the kernel id?
    try:
        await registry.destroy_kernel(kernel_id)
        log.info(_r('destroyed successfully.', request_id))
        status = 204  # no content
    except KernelNotFoundError:
        log.error(_r('kernel not found.', request_id))
        status = 404
        resp['type'] = 'https://api.sorna.io/probs/kernel-not-found'
        resp['title'] = 'No such kernel.'
    except KernelDestructionFailedError:
        log.error(_r('kernel not found.', request_id))
        status = 404
        resp['type'] = 'https://api.sorna.io/probs/kernel-not-found'
        resp['title']  = 'No such kernel.'
    if status == 204:
        content_type = 'application/json'
    return web.Response(status=status,
                        content_type=content_type,
                        text=json.dumps(resp))


@auth_required
async def get_info(request):
    raise NotImplementedError

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
