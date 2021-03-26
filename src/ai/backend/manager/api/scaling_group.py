import logging
from typing import (
    Any,
    Iterable,
    TYPE_CHECKING,
    Tuple,
)

from aiohttp import web
import aiohttp_cors
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from ..models import (
    query_allowed_sgroups,
)
from .auth import auth_required
from .manager import (
    READ_ALLOWED,
    server_status_required)
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params

if TYPE_CHECKING:
    from .context import RootContext

log = BraceStyleAdapter(logging.getLogger(__name__))


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        tx.AliasedKey(['group', 'group_id', 'group_name']): tx.UUID | t.String,
    }),
)
async def list_available_sgroups(request: web.Request, params: Any) -> web.Response:
    root_ctx: RootContext = request.app['_root.context']
    access_key = request['keypair']['access_key']
    domain_name = request['user']['domain_name']
    group_id_or_name = params['group']
    log.info('SGROUPS.LIST(ak:{}, g:{}, d:{})', access_key, group_id_or_name, domain_name)
    async with root_ctx.db.begin() as conn:
        sgroups = await query_allowed_sgroups(
            conn, domain_name, group_id_or_name, access_key)
        return web.json_response({
            'scaling_groups': [
                {'name': sgroup['name']}
                for sgroup in sgroups
            ],
        }, status=200)


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app['prefix'] = 'scaling-groups'
    app['api_versions'] = (2, 3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    root_resource = cors.add(app.router.add_resource(r''))
    cors.add(root_resource.add_route('GET',  list_available_sgroups))
    return app, []
