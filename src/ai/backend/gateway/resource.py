'''
Resource preset APIs.
'''

from decimal import Decimal
import functools
import json
import logging

from aiohttp import web
import aiohttp_cors
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from .auth import auth_required
from .exceptions import (
    InvalidAPIParameters,
)
from .manager import READ_ALLOWED, server_status_required
from ..manager.models import (
    agents, resource_presets,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.kernel'))

_json_loads = functools.partial(json.loads, parse_float=Decimal)


@auth_required
async def list_presets(request) -> web.Response:
    '''
    Returns the list of all resource presets.
    '''
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets))
        # TODO: uncomment when we implement scaling group.
        # scaling_group = request.query.get('scaling_group')
        # if scaling_group is not None:
        #     query = query.where(resource_presets.c.scaling_group == scaling_group)
        result = await conn.execute(query)
        resp = {
            'presets': [],  # TODO: jsnon-ize result
        }
        return web.json_response(resp, status=200)


@server_status_required(READ_ALLOWED)
@auth_required
async def check_presets(request) -> web.Response:
    '''
    Returns the list of all resource presets in the current scaling group,
    with additional information including allocatability of each preset,
    amount of total remaining resources, and the current keypair resource limits.
    '''
    try:
        access_key = request['keypair']['access_key']
        resource_policy = request['keypair']['resource_policy']
        # TODO: uncomment when we implement scaling group.
        # scaling_group = request.query.get('scaling_group')
        # assert scaling_group is not None, 'scaling_group parameter is missing.'
    except (json.decoder.JSONDecodeError, AssertionError) as e:
        raise InvalidAPIParameters(extra_msg=str(e.args[0]))
    registry = request.app['registry']
    keypair_limits = None
    current_remaining = None
    known_slot_types = \
        await registry.config_server.get_resource_slots()
    keypair_limits = \
        await registry.normalize_resource_slot_limits(resource_policy)
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        # fetch all resource presets in the current scaling group.
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets))
        # query = query.where(resource_presets.c.scaling_group == scaling_group)
        key_occupied = \
            await registry.get_keypair_occupancy(access_key, conn=conn)
        current_remaining = keypair_limits - key_occupied
        # TODO: query all agent's capacity and occupancy
        # TODO: check if there are any agent that can allocate the preset for each
        # preset
    resp = {
        'keypair_limits': keypair_limits.as_json_numeric(known_slot_types),
        'current_remaining': current_remaining.as_json_numeric(known_slot_types),
        'presets': [],
    }
    return web.json_response(resp, status=200)


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (4,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', '/presets', list_presets))
    cors.add(add_route('POST', '/check-presets', check_presets))
    return app, []
