from typing import (
    Any,
    Iterable,
    Tuple,
    TYPE_CHECKING,
)

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import trafaret as t

from ai.backend.common import validators as tx

from .auth import superadmin_required
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params

if TYPE_CHECKING:
    from ..manager.registry import AgentRegistry


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('agent_id'): t.String,
    }))
async def get_agent_hwinfo(request: web.Request, params: Any) -> web.Response:
    registry: AgentRegistry = request.app['registry']
    data = await registry.gather_hwinfo(params['agent_id'])
    return web.json_response(data)


async def init(app: web.Application) -> None:
    print('agent app init')
    pass


async def shutdown(app: web.Application) -> None:
    print('agent app shutdown')
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app['api_versions'] = (5,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    status_resource = cors.add(app.router.add_resource('/hwinfo'))
    cors.add(status_resource.add_route('GET', get_agent_hwinfo))
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
