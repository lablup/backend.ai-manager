'''
Resource preset APIs.
'''

from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from decimal import Decimal
import functools
import json
import logging
import re
from typing import Any

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from .auth import auth_required, superadmin_required
from .exceptions import (
    InvalidAPIParameters, GenericForbidden,
)
from .manager import READ_ALLOWED, server_status_required
from ..manager.models import (
    agents, resource_presets,
    groups, kernels, keypairs,
    AgentStatus, KernelStatus,
)
from .utils import check_api_params

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


@server_status_required(READ_ALLOWED)
@superadmin_required
@atomic
async def recalculate_usage(request) -> web.Response:
    '''
    Update `keypairs.c.concurrency_used` and `agents.c.occupied_slots`.

    Those two values are sometimes out of sync. In that case, calling this API
    re-calculates the values for running containers and updates them in DB.
    '''
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        # Query running containers and calculate concurrency_used per AK and
        # occupied_slots per agent.
        query = (sa.select([kernels.c.access_key, kernels.c.agent, kernels.c.occupied_slots])
                   .where(kernels.c.status != KernelStatus.TERMINATED)
                   .order_by(sa.asc(kernels.c.access_key)))
        concurrency_used_per_key = defaultdict(lambda: 0)
        occupied_slots_per_agent = defaultdict(lambda: ResourceSlot({'cpu': 0, 'mem': 0}))
        async for row in conn.execute(query):
            concurrency_used_per_key[row.access_key] += 1
            occupied_slots_per_agent[row.agent] += ResourceSlot(row.occupied_slots)

        # Update concurrency_used for keypairs with running containers.
        for ak, used in concurrency_used_per_key.items():
            query = (sa.update(keypairs)
                       .values(concurrency_used=used)
                       .where(keypairs.c.access_key == ak))
            await conn.execute(query)
        # Update all other keypairs to have concurrency_used = 0.
        query = (sa.update(keypairs)
                   .values(concurrency_used=0)
                   .where(keypairs.c.concurrency_used != 0)
                   .where(sa.not_(keypairs.c.access_key.in_(concurrency_used_per_key.keys()))))
        await conn.execute(query)

        # Update occupied_slots for agents with running containers.
        for aid, slots in occupied_slots_per_agent.items():
            query = (sa.update(agents)
                       .values(occupied_slots=slots)
                       .where(agents.c.id == aid))
            await conn.execute(query)
        # Update all other agents to have empty occupied_slots.
        query = (sa.update(agents)
                   .values(occupied_slots=ResourceSlot({}))
                   .where(agents.c.status == AgentStatus.ALIVE)
                   .where(sa.not_(agents.c.id.in_(occupied_slots_per_agent.keys()))))
        await conn.execute(query)
    return web.json_response({}, status=200)


async def get_container_stats_for_period(request, start_date, end_date, group_ids=None):
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        j = (sa.join(kernels, groups, kernels.c.group_id == groups.c.id))
        query = (sa.select([kernels, groups.c.name])
                   .select_from(j)
                   .where(kernels.c.terminated_at >= start_date)
                   .where(kernels.c.terminated_at < end_date)
                   .order_by(sa.asc(kernels.c.terminated_at)))
        if group_ids:
            query = query.where(kernels.c.group_id.in_(group_ids))
        result = await conn.execute(query)
        rows = await result.fetchall()
    objs_per_group = {}
    local_tz = request.app['config']['system']['timezone']

    for row in rows:
        group_id = str(row.group_id)
        last_stat = row.last_stat
        c_info = {
            # TODO: fill in these values when fields spec is fixed.
            'id': str(row['id']),
            'name': row['sess_id'],
            'access_key': row['access_key'],
            'cpu_used': float(last_stat['cpu_used']['current']) if last_stat else 0,
            'mem_allocated': int(row.occupied_slots['mem']),
            'mem_used': int(last_stat['mem']['capacity']) if last_stat else 0,
            'shared_memory': 0,  # TODO: how to get?
            'disk_used': int(last_stat['io_scratch_size']['stats.max']) if last_stat else 0,
            'io_read': int(last_stat['io_read']['current']) if last_stat else 0,
            'io_write': int(last_stat['io_write']['current']) if last_stat else 0,
            'used_time': str(row['terminated_at'] - row['created_at']),
            'used_days': (row['terminated_at'].astimezone(local_tz).toordinal() -
                          row['created_at'].astimezone(local_tz).toordinal() + 1),
            'device_type': None,  # TODO: gpu device type
            'smp': None,  # TODO: gpu smp?
            'nfs': None,  # TODO: what value to write here?
            'image_name': row['image'],
            'created_at': str(row['created_at']),
            'terminated_at': str(row['terminated_at']),
        }
        if group_id not in objs_per_group:
            objs_per_group[group_id] = {
                'domain_name': row['domain_name'],
                'g_id': group_id,
                'g_name': row['name'],  # this is group's name
                'g_cpu_used': 0,
                'g_mem_used': 0,
                'g_shared_memory': 0,
                'g_disk_used': 0,
                'g_io_read': 0,
                'g_io_write': 0,
                'g_device_type': list(),
                'g_smp': 0,
                'c_infos': [c_info],
            }
        else:
            objs_per_group[group_id]['g_cpu_used'] += c_info['cpu_used']
            objs_per_group[group_id]['g_mem_used'] += c_info['mem_used']
            objs_per_group[group_id]['g_shared_memory'] += c_info['shared_memory']
            objs_per_group[group_id]['g_disk_used'] += c_info['disk_used']
            objs_per_group[group_id]['g_io_read'] += c_info['io_read']
            objs_per_group[group_id]['g_io_write'] += c_info['io_write']
            if c_info['device_type'] not in objs_per_group[group_id]['g_device_type']:
                objs_per_group[group_id]['g_device_type'].append(c_info['device_type'])
            objs_per_group[group_id]['g_smp'] += c_info['smp']
            objs_per_group[group_id]['c_infos'].append(c_info)
    return list(objs_per_group.values())


@atomic
@server_status_required(READ_ALLOWED)
@superadmin_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['group_ids', 'project_ids', 'group', 'project']): t.List(t.String),
        t.Key('month'): t.Regexp(r'^\d{6}', re.ASCII),
    }),
    loads=_json_loads)
async def usage_per_month(request: web.Request, params: Any) -> web.Response:
    '''
    Return usage statistics of terminated containers belonged to the given group for a specified
    period in dates.
    The date/time comparison is done using the configured timezone.

    :param year int: The year.
    :param month int: The month.
    '''
    log.info('USAGE_PER_MONTH (g:[{0}], month:{1})',
             ','.join(params['group_ids']), params['month'])
    local_tz = request.app['config']['system']['timezone']
    try:
        start_date = datetime.strptime(params['month'], '%Y%m').replace(tzinfo=local_tz)
        end_date = start_date + relativedelta(months=+1)
    except ValueError:
        raise InvalidAPIParameters(extra_msg='Invalid date values')
    resp = await get_container_stats_for_period(request, start_date, end_date, params['group_ids'])
    log.debug('container list are retrieved for month {0}', params['month'])
    return web.json_response(resp, status=200)


@atomic
@server_status_required(READ_ALLOWED)
@superadmin_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['group_id', 'project_id', 'group', 'project']): t.String,
        t.Key('start_date'): t.Regexp(r'^\d{8}$', re.ASCII),
        t.Key('end_date'): t.Regexp(r'^\d{8}$', re.ASCII),
    }),
    loads=_json_loads)
async def usage_per_period(request: web.Request, params: Any) -> web.Response:
    '''
    Return usage statistics of terminated containers belonged to the given group for a specified
    period in dates.
    The date/time comparison is done using the configured timezone.

    :param start_date str: "yyyymmdd" format.
    :param end_date str: "yyyymmdd" format.
    '''
    group_id = params['group_id']
    local_tz = request.app['config']['system']['timezone']
    try:
        start_date = datetime.strptime(params['start_date'], '%Y%m%d').replace(tzinfo=local_tz)
        end_date = datetime.strptime(params['end_date'], '%Y%m%d').replace(tzinfo=local_tz)
    except ValueError:
        raise InvalidAPIParameters(extra_msg='Invalid date values')
    if end_date <= start_date:
        raise InvalidAPIParameters(extra_msg='end_date must be later than start_date.')
    log.info('USAGE_PER_MONTH (g:{0}, start_date:{1}, end_date:{2})',
             group_id, start_date, end_date)
    resp = await get_container_stats_for_period(request, start_date, end_date, group_ids=[group_id])
    resp = resp[0]  # only one group (project)
    resp['start_date'] = params['start_date']
    resp['end_date'] = params['end_date']
    log.debug('container list are retrieved from {0} to {1}', start_date, end_date)
    return web.json_response(resp, status=200)


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (4,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    cors.add(add_route('GET',  '/presets', list_presets))
    cors.add(add_route('POST', '/check-presets', check_presets))
    cors.add(add_route('POST', '/recalculate-usage', recalculate_usage))
    cors.add(add_route('GET',  '/usage/month', usage_per_month))
    cors.add(add_route('GET',  '/usage/period', usage_per_period))
    return app, []
