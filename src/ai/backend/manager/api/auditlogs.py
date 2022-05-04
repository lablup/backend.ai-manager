from __future__ import annotations

from datetime import datetime
import logging


from aiohttp import web
import sqlalchemy as sa
import trafaret as t
from typing import Any, TYPE_CHECKING, MutableMapping

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter


from ..models import (
    audit_logs,
)
from .auth import auth_required
from .manager import READ_ALLOWED, server_status_required
from .utils import check_api_params, get_access_key_scopes

if TYPE_CHECKING:
    from .context import RootContext

log = BraceStyleAdapter(logging.getLogger(__name__))


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict(
    {
        t.Key('data'): tx.JSONString,
    },
))
async def append(request: web.Request, params: Any) -> web.Response:
    root_ctx: RootContext = request.app['_root.context']
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    user_uuid = request['user']['uuid']
    user_email = request['user']['email']
    log.info('AUDITLOGS.CREATE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    async with root_ctx.db.begin() as conn:
        resp = {
            'success': True,
        }
        query = audit_logs.insert().values({
            'type': params['type'],
            'email': user_email,
            'uuid': user_uuid,
            'access_key': requester_access_key,
            'data': params['data'],
            'target': params['target'],

        })
        result = await conn.execute(query)
        assert result.rowcount == 1
    return web.json_response(resp)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('page_size', default=20): t.ToInt(lt=101),
        t.Key('page_no', default=1): t.ToInt(),
    }),
)
async def list_auditlogs(request: web.Request, params: Any) -> web.Response:
    root_ctx: RootContext = request.app['_root.context']
    resp: MutableMapping[str, Any] = {'auditlogs': []}

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('AUDITLOGS.LIST (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    async with root_ctx.db.begin() as conn:

        select_query = (
            sa.select([audit_logs])
            .select_from(audit_logs)
            .order_by(sa.desc(audit_logs.c.created_at))
            .limit(params['page_size'])
        )
        count_query = (
            sa.select([sa.func.count(audit_logs.c.data)])
            .select_from(audit_logs)
        )
        if params['page_no'] > 1:
            select_query = select_query.offset((params['page_no'] - 1) * params['page_size'])

        result = await conn.execute(select_query)
        for row in result:
            result_item = {
                'created_at': datetime.timestamp(row['created_at']),
                'uuid': row['uuid'],
                'email': row['email'],
                'acces_key': row['acces_key'],
                'data': row['data'],
                'type': row['type'],
                'target': row['target'],

            }

            resp['auditlogs'].append(result_item)
        resp['count'] = await conn.scalar(count_query)
        return web.json_response(resp, status=200)


# @server_status_required(READ_ALLOWED)
# @auth_required
# @check_api_params(t.Dict(
#     {
#         t.Key('severity'): tx.Enum(LogSeverity),
#         t.Key('source'): t.String,
#         t.Key('message'): t.String,
#         t.Key('context_lang'): t.String,
#         t.Key('context_env'): tx.JSONString,
#         t.Key('request_url', default=None): t.Null | t.String,
#         t.Key('request_status', default=None): t.Null | t.Int,
#         t.Key('traceback', default=None): t.Null | t.String,
#     },
# ))
# async def init(app: web.Application) -> None:
#     pass


# async def shutdown(app: web.Application) -> None:
#     pass


# def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
#     app = web.Application()
#     app.on_startup.append(init)
#     app.on_shutdown.append(shutdown)
#     app['api_versions'] = (4, 5)
#     app['prefix'] = '/logs/audit'
#     cors = aiohttp_cors.setup(app, defaults=default_cors_options)
#     cors.add(app.router.add_route('POST', '', append))
#     cors.add(app.router.add_route('GET', '', list_auditlogs))

#     return app, []
