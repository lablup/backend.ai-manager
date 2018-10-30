import asyncio
import enum
import functools
import logging

from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter

from .exceptions import InvalidAPIParameters, ServerFrozen


log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.manager'))


class ManagerStatus(enum.Enum):
    RUNNING = 'running'
    FROZEN = 'frozen'

    @classmethod
    def is_member(cls, value):
        return any(value == item.value for item in cls)


def server_unfrozen_required(*args, **kwargs):

    def wrapper(handler, gql=False):

        @functools.wraps(handler)
        async def _wrapped(request):
            if gql:
                # Check if server is frozen only for mutation queries.
                try:
                    params = await request.json()
                    query = params['query']
                    if not query.startswith('mutation'):
                        return await handler(request)
                except:
                    # Delegate error handling to the original handler.
                    return await handler(request)

            status = await request.app['config_server'].get_manager_status()
            if status == ManagerStatus.FROZEN:
                raise ServerFrozen

            return await handler(request)

        return _wrapped

    if len(args) == 1:
        return wrapper(args[0])
    elif len(args) == 0 and 'gql' in kwargs:
        return lambda _handler: wrapper(_handler, gql=kwargs['gql'])
    else:
        log.error('Invalid usage of @server_unfrozen_required decorator!')
        raise ValueError('Invalid usage')


async def detect_status_update(app):
    async for _ in app['config_server'].manager_status_update():
        app['config_server'].get_manager_status.cache_clear()
        updated_status = await app['config_server'].get_manager_status()
        log.debug('Process-{0} detected manager status update: {1}',
                  app['pidx'], updated_status)


async def update_manager_status(request):
    params = await request.json()
    try:
        status = params.get('status', None)
        assert status, 'status is missing or empty!'
        assert ManagerStatus.is_member(status), f'Invalid status: {status}'
    except AssertionError as e:
        raise InvalidAPIParameters(extra_msg=str(e.args[0]))

    await request.app['config_server'].update_manager_status(status)

    return web.Response(status=204)


async def init(app):
    loop = asyncio.get_event_loop()
    app['status_watch_task'] = loop.create_task(detect_status_update(app))
    if app['pidx'] == 0:
        await app['config_server'].update_manager_status('running')


async def shutdown(app):
    if app['status_watch_task'] is not None:
        app['status_watch_task'].cancel()
        await app['status_watch_task']


def create_app():
    app = web.Application()
    app['api_versions'] = (2, 3)
    app.router.add_route('GET', r'/status', get_manager_status)
    app.router.add_route('PUT', r'/status', update_manager_status)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
