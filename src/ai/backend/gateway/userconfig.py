import logging
import re
from typing import Any

from aiohttp import web
import aiohttp_cors
import trafaret as t

from ai.backend.common import msgpack
from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .exceptions import (
    InvalidAPIParameters, DotfileCreationFailed,
    DotfileNotFound, DotfileAlreadyExists
)
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params, get_access_key_scopes

from ..manager.models import (
    keypairs, verify_dotfile_name
)
from ..manager.models.keypair import query_owned_dotfiles, MAXIMUM_DOTFILE_SIZE

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.dotfile'))


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict(
    {
        t.Key('data'): t.String(max_length=MAXIMUM_DOTFILE_SIZE),
        t.Key('path'): t.String,
        t.Key('permission'): t.Regexp(r'^[0-7]{3}$', re.ASCII),
        t.Key('owner_access_key', default=None): t.Null | t.String,
    }
))
async def create(request: web.Request, params: Any) -> web.Response:
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('CREATE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    dbpool = request.app['dbpool']

    async with dbpool.acquire() as conn, conn.begin():
        path: str = params['path']
        dotfiles, leftover_space = await query_owned_dotfiles(conn, owner_access_key)
        if leftover_space == 0:
            raise DotfileCreationFailed('No leftover space for dotfile storage')
        if len(dotfiles) == 100:
            raise DotfileCreationFailed('Dotfile creation limit reached')
        if not verify_dotfile_name(path):
            raise InvalidAPIParameters(f'Dotfile {path} is reserved for internal operations.')
        duplicate = [x for x in dotfiles if x['path'] == path]
        if len(duplicate) > 0:
            raise DotfileAlreadyExists
        new_dotfiles = list(dotfiles)
        new_dotfiles.append({'path': path, 'perm': params['permission'], 'data': params['data']})
        dotfile_packed = msgpack.packb(new_dotfiles)
        if len(dotfile_packed) > MAXIMUM_DOTFILE_SIZE:
            raise DotfileCreationFailed('No leftover space for dotfile storage')

        query = (keypairs.update()
                         .values(dotfiles=dotfile_packed)
                         .where(keypairs.c.access_key == owner_access_key))
        await conn.execute(query)
    return web.json_response({})


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(t.Dict({
    t.Key('path', default=None): t.Null | t.String,
    t.Key('owner_access_key', default=None): t.Null | t.String,
}))
async def list_or_get(request: web.Request, params: Any) -> web.Response:
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('LIST_OR_GET (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    async with dbpool.acquire() as conn:
        if params['path']:
            dotfiles, _ = await query_owned_dotfiles(conn, owner_access_key)
            for dotfile in dotfiles:
                if dotfile['path'] == params['path']:
                    return web.json_response(dotfile)
            raise DotfileNotFound
        else:
            dotfiles, _ = await query_owned_dotfiles(conn, access_key)
            for entry in dotfiles:
                resp.append({
                    'path': entry['path'],
                    'permission': entry['perm'],
                    'data': entry['data']
                })
            return web.json_response(resp)


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict(
    {
        t.Key('data'): t.String(max_length=MAXIMUM_DOTFILE_SIZE),
        t.Key('path'): t.String,
        t.Key('permission'): t.Regexp(r'^[0-7]{3}$', re.ASCII),
        t.Key('owner_access_key', default=None): t.Null | t.String,
    }
))
async def update(request: web.Request, params: Any) -> web.Response:
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('CREATE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    dbpool = request.app['dbpool']

    async with dbpool.acquire() as conn, conn.begin():
        path: str = params['path']
        dotfiles, _ = await query_owned_dotfiles(conn, owner_access_key)
        new_dotfiles = [x for x in dotfiles if x['path'] != path]
        if len(new_dotfiles) == len(dotfiles):
            raise DotfileNotFound

        new_dotfiles.append({'path': path, 'perm': params['permission'], 'data': params['data']})
        dotfile_packed = msgpack.packb(new_dotfiles)
        if len(dotfile_packed) > MAXIMUM_DOTFILE_SIZE:
            raise DotfileCreationFailed('No leftover space for dotfile storage')

        query = (keypairs.update()
                         .values(dotfiles=dotfile_packed)
                         .where(keypairs.c.access_key == owner_access_key))
        await conn.execute(query)
    return web.json_response({})


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('path'): t.String,
        t.Key('owner_access_key', default=None): t.Null | t.String,
    })
)
async def delete(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    path = params['path']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('DELETE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    async with dbpool.acquire() as conn, conn.begin():
        dotfiles, _ = await query_owned_dotfiles(conn, owner_access_key)
        new_dotfiles = [x for x in dotfiles if x['path'] != path]
        if len(new_dotfiles) == len(dotfiles):
            raise DotfileNotFound
        dotfile_packed = msgpack.packb(new_dotfiles)
        query = (keypairs.update()
                         .values(dotfiles=dotfile_packed)
                         .where(keypairs.c.access_key == owner_access_key))
        await conn.execute(query)
        return web.json_response({'success': True})


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (4, 5)
    app['prefix'] = 'user-config'
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('POST', '/dotfiles', create))
    cors.add(app.router.add_route('GET', '/dotfiles', list_or_get))
    cors.add(app.router.add_route('PATCH', '/dotfiles', update))
    cors.add(app.router.add_route('DELETE', '/dotfiles', delete))

    return app, []
