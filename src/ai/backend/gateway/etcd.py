from __future__ import annotations

import logging
from typing import (
    Any,
    AsyncGenerator,
    Iterable,
    Mapping,
    TYPE_CHECKING,
    Tuple,
)

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import trafaret as t

from ai.backend.common.docker import get_known_registries
from ai.backend.common.logging import BraceStyleAdapter
from .auth import superadmin_required
from .exceptions import InvalidAPIParameters
from .utils import check_api_params
from .types import CORSOptions, WebMiddleware
if TYPE_CHECKING:
    from .config import SharedConfig

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.etcd'))


@atomic
async def get_resource_slots(request: web.Request) -> web.Response:
    log.info('ETCD.GET_RESOURCE_SLOTS ()')
    shared_config: SharedConfig = request.app['shared_config']
    known_slots = await shared_config.get_resource_slots()
    return web.json_response(known_slots, status=200)


@atomic
async def get_vfolder_types(request: web.Request) -> web.Response:
    log.info('ETCD.GET_VFOLDER_TYPES ()')
    shared_config: SharedConfig = request.app['shared_config']
    vfolder_types = await shared_config.get_vfolder_types()
    return web.json_response(vfolder_types, status=200)


@atomic
@superadmin_required
async def get_docker_registries(request: web.Request) -> web.Response:
    """
    Returns the list of all registered docker registries.
    """
    log.info('ETCD.GET_DOCKER_REGISTRIES ()')
    etcd = request.app['shared_config'].etcd
    _registries = await get_known_registries(etcd)
    # ``yarl.URL`` is not JSON-serializable, so we need to represent it as string.
    known_registries: Mapping[str, str] = {k: v.human_repr() for k, v in _registries.items()}
    return web.json_response(known_registries, status=200)


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('prefix', default=False): t.Bool,
    }))
async def get_config(request: web.Request, params: Any) -> web.Response:
    shared_config: SharedConfig = request.app['shared_config']
    etcd = shared_config.etcd
    log.info('ETCD.GET_CONFIG (ak:{}, key:{}, prefix:{})',
             request['keypair']['access_key'], params['key'], params['prefix'])
    if params['prefix']:
        # Flatten the returned ChainMap object for JSON serialization
        value = dict(await etcd.get_prefix_dict(params['key']))
    else:
        value = await etcd.get(params['key'])
    return web.json_response({'result': value})


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('value'): (t.String(allow_blank=True) |
                         t.Mapping(t.String(allow_blank=True), t.Any)),
    }))
async def set_config(request: web.Request, params: Any) -> web.Response:
    shared_config: SharedConfig = request.app['shared_config']
    etcd = shared_config.etcd
    log.info('ETCD.SET_CONFIG (ak:{}, key:{}, val:{})',
             request['keypair']['access_key'], params['key'], params['value'])
    if isinstance(params['value'], Mapping):
        updates = {}

        def flatten(prefix, o):
            for k, v in o.items():
                inner_prefix = prefix if k == '' else f'{prefix}/{k}'
                if isinstance(v, Mapping):
                    flatten(inner_prefix, v)
                else:
                    updates[inner_prefix] = v

        flatten(params['key'], params['value'])
        # TODO: chunk support if there are too many keys
        if len(updates) > 16:
            raise InvalidAPIParameters(
                'Too large update! Split into smaller key-value pair sets.')
        await etcd.put_dict(updates)
    else:
        await etcd.put(params['key'], params['value'])
    return web.json_response({'result': 'ok'})


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('prefix', default=False): t.Bool,
    }))
async def delete_config(request: web.Request, params: Any) -> web.Response:
    shared_config: SharedConfig = request.app['shared_config']
    etcd = shared_config.etcd
    log.info('ETCD.DELETE_CONFIG (ak:{}, key:{}, prefix:{})',
             request['keypair']['access_key'], params['key'], params['prefix'])
    if params['prefix']:
        await etcd.delete_prefix(params['key'])
    else:
        await etcd.delete(params['key'])
    return web.json_response({'result': 'ok'})


async def app_ctx(app: web.Application) -> AsyncGenerator[None, None]:
    if app['pidx'] == 0:
        await app['shared_config'].register_myself()
    yield
    if app['pidx'] == 0:
        await app['shared_config'].deregister_myself()


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.cleanup_ctx.append(app_ctx)
    app['prefix'] = 'config'
    app['api_versions'] = (3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('GET',  r'/resource-slots', get_resource_slots))
    cors.add(app.router.add_route('GET',  r'/vfolder-types', get_vfolder_types))
    cors.add(app.router.add_route('GET',  r'/docker-registries', get_docker_registries))
    cors.add(app.router.add_route('POST', r'/get', get_config))
    cors.add(app.router.add_route('POST', r'/set', set_config))
    cors.add(app.router.add_route('POST', r'/delete', delete_config))
    return app, []
