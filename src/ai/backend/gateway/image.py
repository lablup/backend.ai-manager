import asyncio
from typing import Any

from aiohttp import web
import aiohttp_cors
import trafaret as t

from ai.backend.common import validators as tx

from .auth import auth_required
from .manager import ALL_ALLOWED, server_status_required
from .utils import (
    check_api_params,
)


@server_status_required(ALL_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
    }))
async def import_image(request: web.Request, params: Any) -> web.Response:
    return web.json_response({'test': 'ok'}, status=200)


async def init(app: web.Application):
    pass


async def shutdown(app: web.Application):
    pass


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'image'
    app['api_versions'] = (4,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('POST', '/import', import_image))
    return app, []
