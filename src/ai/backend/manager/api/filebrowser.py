import logging
from typing import Any, Iterable, Mapping, Tuple

import aiohttp
import aiohttp_cors
import sqlalchemy as sa
import trafaret as t
from ai.backend.common.logging import BraceStyleAdapter
from aiohttp import web

from ..exceptions import InvalidArgument
from ..models import vfolders
from .auth import auth_required
from .context import RootContext
from .manager import READ_ALLOWED, server_status_required
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params

log = BraceStyleAdapter(logging.getLogger(__name__))

VFolderRow = Mapping[str, Any]


async def get_vfid(root_ctx: RootContext, name: str) -> str:
    async with root_ctx.db.begin() as conn:
        query = (
            sa.select([vfolders.c.id])
            .select_from(vfolders)
            .where(vfolders.c.name == name)
        )
        folder_id = await conn.scalar(query)

        query = sa.delete(vfolders).where(vfolders.c.id == folder_id)

        return folder_id.hex


async def get_volume(root_ctx: RootContext, vfid: str) -> str:
    async with root_ctx.db.begin() as conn:
        query = (
            sa.select([vfolders.c.host])
            .select_from(vfolders)
            .where(vfolders.c.id == vfid)
        )
        host = await conn.scalar(query)
        return host


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict(
        {
            t.Key("vfolders"): t.List(t.String),
        },
    ),
)
async def create_or_update_filebrowser(
    request: web.Request,
    params: Any,
) -> web.Response:

    root_ctx: RootContext = request.app["_root.context"]

    vfolders = []
    for vfolder_name in params["vfolders"]:
        vfolders.append(
            {"name": vfolder_name, "vfid": await get_vfid(root_ctx, vfolder_name)},
        )

    host = await get_volume(root_ctx, await get_vfid(root_ctx, vfolder_name))

    proxy_name, _ = root_ctx.storage_manager.split_host(host)

    try:
        proxy_info = root_ctx.storage_manager._proxies[proxy_name]
    except KeyError:
        raise InvalidArgument("There is no such storage proxy", proxy_name)

    headers = {}
    headers["X-BackendAI-Storage-Auth-Token"] = proxy_info.secret

    try:
        async with proxy_info.session.request(
            "POST",
            proxy_info.manager_api_url / "browser/create",
            headers=headers,
            json={"vfolders": vfolders},
        ) as client_resp:
            return web.json_response(await client_resp.json())
    except aiohttp.ClientResponseError:
        raise


@auth_required
@server_status_required(READ_ALLOWED)
async def destroy_filebrowser(
    request: web.Request,
    params: Any,
) -> web.Response:

    root_ctx: RootContext = request.app["_root.context"]

    vfolders = []
    for vfolder_name in params["vfolders"]:
        vfolders.append(
            {"name": vfolder_name, "vfid": await get_vfid(root_ctx, vfolder_name)},
        )

    host = await get_volume(root_ctx, await get_vfid(root_ctx, vfolder_name))

    proxy_name, _ = root_ctx.storage_manager.split_host(host)

    try:
        proxy_info = root_ctx.storage_manager._proxies[proxy_name]
    except KeyError:
        raise InvalidArgument("There is no such storage proxy", proxy_name)

    headers = {}
    headers["X-BackendAI-Storage-Auth-Token"] = proxy_info.secret

    try:
        async with proxy_info.session.request(
            "POST", proxy_info.manager_api_url / "browser/destroy", headers=headers,
        ) as client_resp:
            return web.json_response(await client_resp.json())
    except aiohttp.ClientResponseError:
        raise


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(
    default_cors_options: CORSOptions,
) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app["prefix"] = "browser"
    app["api_versions"] = (
        2,
        3,
        4,
    )
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)

    cors.add(app.router.add_route("POST", r"/create", create_or_update_filebrowser))
    cors.add(app.router.add_route("POST", r"/destroy", destroy_filebrowser))

    return app, []
