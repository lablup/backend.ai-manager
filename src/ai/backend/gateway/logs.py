import logging

from aiohttp import web
import aiohttp_cors
import sqlalchemy as sa
import trafaret as t
from typing import Any, Tuple

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .manager import READ_ALLOWED, server_status_required
from .typing import CORSOptions, Iterable, WebMiddleware
from .utils import check_api_params, get_access_key_scopes

from ..manager.models import (
    error_logs, LogSeverity, UserRole, groups,
    association_groups_users as agus
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.logs'))


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict(
    {
        t.Key('severity'): tx.Enum(LogSeverity),
        t.Key('source'): t.String,
        t.Key('message'): t.String,
        t.Key('context_lang'): t.String,
        t.Key('context_env'): tx.JSON,
        t.Key('request_url', default=None): t.Null | t.String,
        t.Key('request_status', default=None): t.Null | t.Int,
        t.Key('traceback', default=None): t.Null | t.String
    }
))
async def create(request: web.Request, params: Any) -> web.Response:
    if params['domain'] is None:
        params['domain'] = request['user']['domain_name']
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    requester_uuid = request['user']['uuid']
    log.info('CREATE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    dbpool = request.app['dbpool']

    async with dbpool.acquire() as conn, conn.begin():
        log.debug('Params: {0}', params)
        resp = {
            'success': True
        }
        query = error_logs.insert().values({
            'severity': params['severity'],
            'source': params['source'],
            'user': requester_uuid,
            'message': params['message'],
            'context_lang': params['context_lang'],
            'context_env': params['context_env'],
            'request_url': params['request_url'],
            'request_status': params['request_status'],
            'traceback': params['traceback'],
        })
        result = await conn.execute(query)
        assert result.rowcount == 1
    return web.json_response(resp)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('mark_read', default=False): t.Bool,
        t.Key('page_size', default=20): t.Int(lt=101),
        t.Key('page_no', default=1): t.Int
    }),
)
async def list_logs(request: web.Request, params: Any) -> web.Response:
    resp = []
    dbpool = request.app['dbpool']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('LIST (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(error_logs)
                   .order_by(error_logs.c.timestamp.desc())
                   .limit(params['page_size']))
        if params['page_no'] > 1:
            query = query.offset((params['page_no'] - 1) * params['page_size'])
        if request['is_superadmin']:
            pass
        elif user_role == UserRole.ADMIN or user_role == 'admin':
            j = (groups.join(agus, groups.c.id == agus.c.group_id))
            usr_query = (sa.select([agus.c.user_id])
                           .select_from(j)
                           .where([groups.c.domain_name == domain_name]))
            result = await conn.execute(usr_query)
            usrs = await result.fetchall()
            user_ids = [g.id for g in usrs]
            query = query.where(error_logs.c.user_id.in_(user_ids))
        else:
            query = (query.where((error_logs.c.user_id == user_uuid) &
                                 (not error_logs.c.is_cleared)))

        result = await conn.execute(query)
        async for row in result:
            resp.append(row)
        if params['mark_read']:
            update = (sa.update(error_logs)
                        .values(is_read=True)
                        .where(error_logs.c.id.in_([x['id'] for x in resp])))
            await conn.execute(update)
        return web.json_response(resp, status=200)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('log_id'): t.List(tx.UUID)
    })
)
async def clear(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('CLEAR (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.update(error_logs)
                   .values(is_cleared=True))
        if request['is_superadmin']:
            query = query.where(error_logs.c.id.in_(params['log_id']))
        elif user_role == UserRole.ADMIN or user_role == 'admin':
            j = (groups.join(agus, groups.c.id == agus.c.group_id))
            usr_query = (sa.select([agus.c.user_id])
                           .select_from(j)
                           .where([groups.c.domain_name == domain_name]))
            result = await conn.execute(usr_query)
            usrs = await result.fetchall()
            user_ids = [g.id for g in usrs]
            query = query.where((error_logs.c.user_id.in_(user_ids)) &
                                (error_logs.c.id.in_(params['log_id'])))
        else:
            query = (query.where((error_logs.c.user_id == user_uuid) &
                                 (error_logs.c.id.in_(params['log_id']))))

        result = await conn.execute(query)
        assert result.rowcount == len(params['log_id'])

        return web.json_response({'success': True}, status=200)


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (4, 5)
    app['prefix'] = '/logs/error'
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('POST', '', create))
    cors.add(app.router.add_route('GET', '', list_logs))
    cors.add(app.router.add_route('POST', '/clear', clear))

    return app, []
