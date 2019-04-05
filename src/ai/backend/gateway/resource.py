'''
Resource preset APIs.
'''

from decimal import Decimal
import functools
import json
import logging

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from .auth import auth_required
from .exceptions import (
    InvalidAPIParameters,
)
from .manager import READ_ALLOWED, server_status_required
from ..manager.models import (
    agents, resource_presets,
    AgentStatus,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.kernel'))

_json_loads = functools.partial(json.loads, parse_float=Decimal)


@auth_required
@atomic
async def list_presets(request) -> web.Response:
    '''
    Returns the list of all resource presets.
    '''
    known_slot_types = await request.app['registry'].config_server.get_resource_slots()
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets))
        # TODO: uncomment when we implement scaling group.
        # scaling_group = request.query.get('scaling_group')
        # if scaling_group is not None:
        #     query = query.where(resource_presets.c.scaling_group == scaling_group)
        resp = {'presets': []}
        async for row in conn.execute(query):
            preset_slots = row['resource_slots'].filter_slots(known_slot_types)
            resp['presets'].append({
                'name': row['name'],
                'resource_slots': preset_slots.to_json(),
            })
        return web.json_response(resp, status=200)


@server_status_required(READ_ALLOWED)
@auth_required
@atomic
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
    known_slot_types = await registry.config_server.get_resource_slots()
    keypair_limits = ResourceSlot.from_policy(resource_policy, known_slot_types)
    resp = {
        'keypair_limits': None,
        'keypair_using': None,
        'keypair_remaining': None,
        'scaling_group_remaining': None,
        'presets': [],
    }
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        keypair_occupied = await registry.get_keypair_occupancy(access_key, conn=conn)
        keypair_remaining = keypair_limits - keypair_occupied
        resp['keypair_limits'] = keypair_limits.to_json()
        resp['keypair_using'] = keypair_occupied.to_json()
        resp['keypair_remaining'] = keypair_remaining.to_json()
        # query all agent's capacity and occupancy
        agent_slots = []
        query = (
            sa.select([agents.c.available_slots, agents.c.occupied_slots])
            .select_from(agents)
            .where(agents.c.status == AgentStatus.ALIVE))
        # TODO: add scaling_group as filter condition
        # query = query.where(resource_presets.c.scaling_group == scaling_group)
        sgroup_remaining = ResourceSlot({
            k: Decimal(0) for k in known_slot_types.keys()
        })
        async for row in conn.execute(query):
            remaining = row['available_slots'] - row['occupied_slots']
            sgroup_remaining += remaining
            agent_slots.append(remaining)
        resp['scaling_group_remaining'] = sgroup_remaining.to_json()
        # fetch all resource presets in the current scaling group.
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets))
        async for row in conn.execute(query):
            # check if there are any agent that can allocate each preset
            allocatable = False
            preset_slots = row['resource_slots'].filter_slots(known_slot_types)
            for agent_slot in agent_slots:
                if agent_slot >= preset_slots and keypair_remaining >= preset_slots:
                    allocatable = True
                    break
            resp['presets'].append({
                'name': row['name'],
                'resource_slots': preset_slots.to_json(),
                'allocatable': allocatable,
            })
    return web.json_response(resp, status=200)


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (4,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET', '/presets', list_presets))
    cors.add(add_route('POST', '/check-presets', check_presets))
    return app, []
