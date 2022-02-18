import logging
from typing import (
    Any,
    Iterable,
    TYPE_CHECKING,
    Tuple,
)

from aiohttp import web
import aiohttp
import aiohttp_cors
import aiotools
from dataclasses import dataclass, field
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from ai.backend.manager.api.exceptions import GenericNotFound

from ai.backend.manager.models.scaling_group import sgroups_for_domains
from ai.backend.manager.models.utils import ExtendedAsyncSAEngine

from ..models import (
    query_allowed_sgroups,
    scaling_groups,
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


@dataclass(unsafe_hash=True)
class WSProxyVersionQueryParams:
    db_ctx: ExtendedAsyncSAEngine = field(hash=False)


@aiotools.lru_cache(expire_after=30)  # expire after 30 seconds
async def query_wsproxy_version(
    params: WSProxyVersionQueryParams,
    scaling_group_name: str,
    domain_name: str,
) -> str:
    async with params.db_ctx.begin_readonly() as conn:
        query = (
            sa.select([sgroups_for_domains.c.scaling_group])
            .where((
                (sgroups_for_domains.c.domain == domain_name) &
                (sgroups_for_domains.c.scaling_group == scaling_group_name)
            ))
        )
        matched_sgroup_name = await conn.scalar(query)
        if matched_sgroup_name is None:
            raise GenericNotFound
        result = await conn.execute(
            sa.select([scaling_groups])
            .select_from(scaling_groups)
            .where(scaling_groups.c.name == matched_sgroup_name),
        )
        matched_sgroup = result.fetchone()

    if matched_sgroup is None:
        raise GenericNotFound

    wsproxy_addr = matched_sgroup['wsproxy_addr']
    if not wsproxy_addr:
        return 'v1'
    else:
        async with aiohttp.ClientSession() as session:
            async with session.get(wsproxy_addr + '/status') as resp:
                version_json = await resp.json()
                return version_json['api_version']


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


@auth_required
@server_status_required(READ_ALLOWED)
async def get_wsproxy_version(request: web.Request) -> web.Response:
    root_ctx: RootContext = request.app['_root.context']
    scaling_group_name = request.match_info['scaling_group']
    domain_name = request['user']['domain_name']

    params = WSProxyVersionQueryParams(
        db_ctx=root_ctx.db,
    )
    wsproxy_version = await query_wsproxy_version(params, scaling_group_name, domain_name)
    return web.json_response({'version': wsproxy_version})


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
    cors.add(app.router.add_route('GET',  '/{scaling_group}/wsproxy-version', get_wsproxy_version))
    return app, []
