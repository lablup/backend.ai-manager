'''
Kernel session management.
'''

import asyncio
from collections import defaultdict
from datetime import datetime
import logging

from aiohttp import web
from async_timeout import timeout as _timeout
from dateutil.parser import parse as dtparse
from dateutil.tz import tzutc
import simplejson as json
import sqlalchemy as sa

from sorna.exceptions import InvalidAPIParameters, QuotaExceeded, \
                             QueryNotImplemented, KernelNotFound, SornaError  # noqa
from .auth import auth_required
from .models import KeyPair
from ..manager.registry import InstanceRegistry

_json_type = 'application/json'

log = logging.getLogger('sorna.gateway.kernel')

kernel_owners = {}
last_instance_states = {}


@auth_required
async def create(request):
    try:
        with _timeout(2):
            params = await request.json()
        log.info(f"GET_OR_CREATE (lang:{params['lang']}, token:{params['clientSessionToken']})")
        assert 8 <= len(params['clientSessionToken']) <= 40
    except (asyncio.TimeoutError, AssertionError,
            KeyError, json.decoder.JSONDecodeError):
        log.warn('GET_OR_CREATE: invalid/missing parameters')
        raise InvalidAPIParameters
    resp = {}
    try:
        access_key = request.keypair['access_key']
        concurrency_limit = request.keypair['concurrency_limit']
        async with request.app.dbpool.acquire() as conn, conn.transaction():
            query = sa.select([KeyPair.c.concurrency_used], for_update=True) \
                      .select_from(KeyPair) \
                      .where(KeyPair.c.access_key == access_key)  # noqa
            concurrency_used = await conn.fetchval(query)
            log.debug(f'access_key: {access_key} ({concurrency_used} / {concurrency_limit})')
            if concurrency_used < concurrency_limit:
                query = sa.update(KeyPair) \
                          .values(concurrency_used=KeyPair.c.concurrency_used + 1) \
                          .where(KeyPair.c.access_key == access_key)  # noqa
                await conn.fetchval(query)
            else:
                raise QuotaExceeded
        kernel = await request.app.registry.get_or_create_kernel(
            params['clientSessionToken'], params['lang'], access_key)
        resp['kernelId'] = kernel.id
        kernel_owners[kernel.id] = access_key
    except SornaError:
        log.exception('GET_OR_CREATE: API Internal Error')
        raise
    return web.Response(status=201, content_type=_json_type,
                        text=json.dumps(resp))


async def kernel_terminated(app, reason, kern_id):

    # TODO: enqueue termination event to streaming response queue

    affected_key = kernel_owners.get(kern_id, None)
    if affected_key is None:
        try:
            kern = await app.registry.get_kernel(kern_id)
            affected_key = kern.access_key
        except KernelNotFound:
            log.warning(f'kernel_terminated: owner access key of {kern_id} is missing!')
            return

    # TODO: record usage & trigger billing update task (celery?)

    await app.registry.clean_kernel(kern_id)
    async with app.dbpool.acquire() as conn, conn.transaction():
        query = sa.update(KeyPair) \
                  .values(concurrency_used=KeyPair.c.concurrency_used - 1) \
                  .where(KeyPair.c.access_key == affected_key)  # noqa
        await conn.fetchval(query)
        if kern_id in kernel_owners:
            del kernel_owners[kern_id]


async def instance_started(app, inst_id):
    await app.registry.reset_instance(inst_id)
    last_instance_states[inst_id] = 'running'


async def instance_terminated(app, reason, inst_id):
    if reason == 'agent-lost':
        log.warning(f'agent@{inst_id} heartbeat timeout detected.')
        last_instance_states[inst_id] = 'lost'

        # In heartbeat timeouts, we do NOT clear Redis keys because
        # the timeout may be a transient one.
        kern_ids = await app.registry.get_kernels_in_instance(inst_id)

        # TODO: enqueue termination event to streaming response queue

        kern_counts = defaultdict(int)
        for kid in kern_ids:
            try:
                kern_counts[kernel_owners[kid]] += 1
                del kernel_owners[kid]
            except KeyError:
                pass
        affected_keys = [ak for ak, cnt in kern_counts.items() if cnt > 0]
        log.warning(f'instance_terminated: {kern_ids!r} {kern_counts}')

        if affected_keys:

            # TODO: record usage & trigger billing update task (celery?)

            async with app.dbpool.acquire() as conn, conn.transaction():
                for access_key in affected_keys:
                    query = sa.update(KeyPair) \
                              .values(concurrency_used=KeyPair.c.concurrency_used
                                                       - kern_counts[access_key]) \
                              .where(KeyPair.c.access_key == access_key)  # noqa
                    await conn.fetchval(query)

    else:
        # In other cases, kernel termination will be triggered by the agent.
        # We don't have to clear them manually.
        if inst_id in last_instance_states:
            del last_instance_states[inst_id]


async def instance_heartbeat(app, inst_id, inst_info, running_kernels, interval):
    if inst_id not in last_instance_states:
        app['event_server'].local_dispatch('instance_started', inst_id)
    if inst_id in last_instance_states and last_instance_states[inst_id] == 'lost':
        log.warning('agent@{} revived.'.format(inst_id))
        # As we have cleared all kernel information on this instance,
        # the instance should destroy all existing kernels if any.
        await app.registry.revive_instance(inst_id, inst_info['addr'])
        last_instance_states[inst_id] = 'running'
    await app.registry.handle_heartbeat(inst_id, inst_info, running_kernels, interval)


async def instance_stats(app, inst_id, kern_stats, interval):
    await app.registry.handle_stats(inst_id, kern_stats, interval)


@auth_required
async def destroy(request):
    kern_id = request.match_info['kernel_id']
    log.info(f'DESTROY (k:{kern_id})')
    try:
        await request.app.registry.destroy_kernel(kern_id)
    except SornaError:
        log.exception('DESTROY: API Internal Error')
        raise
    return web.Response(status=204)


@auth_required
async def get_info(request):
    resp = {}
    kern_id = request.match_info['kernel_id']
    log.info(f'GETINFO (k:{kern_id})')
    try:
        kern = await request.app.registry.get_kernel(kern_id)
        resp['lang'] = kern.lang
        age = datetime.now(tzutc()) - dtparse(kern.created_at)
        resp['age'] = age.total_seconds() * 1000
        # Resource limits collected from agent heartbeats
        # TODO: factor out policy/image info as a common repository
        resp['queryTimeout']  = int(float(kern.exec_timeout) * 1000)
        resp['idleTimeout']   = int(float(kern.idle_timeout) * 1000)
        resp['memoryLimit']   = int(kern.mem_limit)
        resp['maxCpuCredit']  = int(float(kern.exec_timeout) * 1000)
        # Stats collected from agent heartbeats
        resp['numQueriesExecuted'] = int(kern.num_queries)
        resp['idle']          = int(float(kern.idle) * 1000)
        resp['memoryUsed']    = int(kern.mem_max_bytes) // 1024
        resp['cpuCreditUsed'] = int(float(kern.cpu_used))
        log.info(f'information retrieved: {resp!r}')
        await request.app.registry.update_kernel(kern, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('GETINFO: API Internal Error')
        raise
    return web.Response(status=200, content_type=_json_type,
                        text=json.dumps(resp))


@auth_required
async def restart(request):
    kern_id = request.match_info['kernel_id']
    log.info(f'RESTART (k:{kern_id})')
    try:
        kern = await request.app.registry.get_kernel(kern_id)
        await request.app.registry.restart_kernel(kern_id)
        await request.app.registry.update_kernel(kern_id, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('RESTART: API Internal Error')
        raise
    return web.Response(status=204)


@auth_required
async def execute_snippet(request):
    resp = {}
    kern_id = request.match_info['kernel_id']
    try:
        with _timeout(2):
            params = await request.json()
        log.info(f'EXECUTE_SNIPPET (k:{kern_id})')
    except (asyncio.TimeoutError, json.decoder.JSONDecodeError):
        log.warn('EXECUTE_SNIPPET: invalid/missing parameters')
        raise InvalidAPIParameters
    try:
        kern = await request.app.registry.get_kernel(kern_id)
        resp['result'] = await request.app.registry.execute_snippet(
            kern_id, params['codeId'], params['code'])
        await request.app.registry.update_kernel(kern_id, {
            'num_queries': int(kern.num_queries) + 1,
        })
    except SornaError:
        log.exception('EXECUTE_SNIPPET: API Internal Error')
        raise
    return web.Response(status=200, content_type=_json_type,
                        text=json.dumps(resp))


async def stream_pty(request):
    kern_id = request.match_info['kernel_id']
    # TODO: migrate pub/sub part of webterm proxy from neumann
    raise QueryNotImplemented


async def stream_events(request):
    kern_id = request.match_info['kernel_id']
    # TODO: dequeue the streaming response queue for the given kernel id
    raise QueryNotImplemented


async def init(app):
    app.router.add_route('POST', '/v1/kernel/create', create)
    app.router.add_route('GET', '/v1/kernel/{kernel_id}', get_info)
    app.router.add_route('PATCH', '/v1/kernel/{kernel_id}', restart)
    app.router.add_route('DELETE', '/v1/kernel/{kernel_id}', destroy)
    app.router.add_route('POST', '/v1/kernel/{kernel_id}', execute_snippet)
    app.router.add_route('POST', '/v1/stream/kernel/{kernel_id}/pty', stream_pty)
    app.router.add_route('POST', '/v1/stream/kernel/{kernel_id}/events', stream_events)

    app['event_server'].add_handler('kernel_terminated', kernel_terminated)
    app['event_server'].add_handler('instance_started', instance_started)
    app['event_server'].add_handler('instance_terminated', instance_terminated)
    app['event_server'].add_handler('instance_heartbeat', instance_heartbeat)
    app['event_server'].add_handler('instance_stats', instance_stats)

    app.registry = InstanceRegistry(app.config.redis_addr)
    await app.registry.init()


async def shutdown(app):
    await app.registry.terminate()
