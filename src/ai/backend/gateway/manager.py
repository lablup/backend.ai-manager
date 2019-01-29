import asyncio
import functools
import json
import logging
from typing import Set
import sqlalchemy as sa

from aiohttp import web
import aiohttp_cors

from ai.backend.common.logging import BraceStyleAdapter

from . import ManagerStatus
from .auth import admin_required
from .exceptions import InvalidAPIParameters, ServerFrozen, ServiceUnavailable
from ..manager.models import kernels, KernelStatus


log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.manager'))


def server_status_required(allowed_status: Set[ManagerStatus]):

    def decorator(handler):

        @functools.wraps(handler)
        async def wrapped(request):
            status = await request.app['config_server'].get_manager_status()
            if status not in allowed_status:
                if status == ManagerStatus.FROZEN:
                    raise ServerFrozen
                msg = f'Server is not in the required status: {allowed_status}'
                raise ServiceUnavailable(msg)
            return (await handler(request))

        return wrapped

    return decorator


READ_ALLOWED = frozenset({ManagerStatus.RUNNING, ManagerStatus.FROZEN})
ALL_ALLOWED = frozenset({ManagerStatus.RUNNING})


class GQLMutationUnfrozenRequiredMiddleware:

    def resolve(self, next, root, info, **args):
        if info.operation.operation == 'mutation' and \
                info.context['manager_status'] == ManagerStatus.FROZEN:
            raise ServerFrozen
        return next(root, info, **args)


async def detect_status_update(app):
    async for ev in app['config_server'].watch_manager_status():
        if ev.event == 'put':
            app['config_server'].get_manager_status.cache_clear()
            updated_status = await app['config_server'].get_manager_status()
            log.debug('Process-{0} detected manager status update: {1}',
                      app['pidx'], updated_status)


async def fetch_manager_status(request):
    try:
        status = await request.app['config_server'].get_manager_status()

        async with request.app['dbpool'].acquire() as conn, conn.begin():
            query = (sa.select([sa.func.count(kernels.c.id)])
                       .select_from(kernels)
                       .where((kernels.c.role == 'master') &
                              (kernels.c.status != KernelStatus.TERMINATED)))
            active_sessions_num = await conn.scalar(query)

            return web.json_response({
                'status': status.value,
                'active_sessions': active_sessions_num,
            })
    except:
        log.exception('GET_MANAGER_STATUS: exception')
        raise


@admin_required
async def update_manager_status(request):
    try:
        params = await request.json()
        status = params.get('status', None)
        force_kill = params.get('force_kill', None)
        assert status, 'status is missing or empty!'
        status = ManagerStatus(status)
    except json.JSONDecodeError:
        raise InvalidAPIParameters(extra_msg='No request body!')
    except (AssertionError, ValueError) as e:
        raise InvalidAPIParameters(extra_msg=str(e.args[0]))

    if force_kill:
        await request.app['registry'].kill_all_sessions()
    await request.app['config_server'].update_manager_status(status)

    return web.Response(status=204)


async def init(app):
    loop = asyncio.get_event_loop()
    app['status_watch_task'] = loop.create_task(detect_status_update(app))
    if app['pidx'] == 0:
        await app['config_server'].update_manager_status(ManagerStatus.RUNNING)


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].update_manager_status(ManagerStatus.TERMINATED)
    if app['status_watch_task'] is not None:
        app['status_watch_task'].cancel()
        await app['status_watch_task']


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    status_resource = cors.add(app.router.add_resource(r'/status'))
    cors.add(status_resource.add_route('GET', fetch_manager_status))
    cors.add(status_resource.add_route('PUT', update_manager_status))
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
