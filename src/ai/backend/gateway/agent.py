import asyncio
import logging
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
from ai.backend.common.logging import BraceStyleAdapter

from .auth import superadmin_required
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params

if TYPE_CHECKING:
    from ..manager.registry import AgentRegistry

log = BraceStyleAdapter(logging.getLogger(__name__))


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        tx.MultiKey('agent_ids'): t.String | t.List(t.String),
    }))
async def get_agent_hwinfo(request: web.Request, params: Any) -> web.Response:
    registry: AgentRegistry = request.app['registry']
    tasks = []
    results = []
    for agent_id in params['agent_ids']:
        tasks.append(await registry.gather_hwinfo(params['agent_id']))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    reply = []
    for agent_id, result in zip(params['agent_ids'], results):
        if isinstance(result, Exception):
            log.error("gathering hwinfo failed for agent {}", agent_id, exc_info=result)
            reply.append({
                'agent': agent_id,
                'error': str(result),
            })
        else:
            reply.append({
                'agent': agent_id,
                'error': None,
                **result,  # a mapping of compute plugin keys (e.g., "cpu", "cuda")
                           # to HardwareMetadata dicts
            })
    return web.json_response(reply)


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
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
