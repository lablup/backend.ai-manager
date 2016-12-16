'''
Kernel session management.
'''

import asyncio
from collections import defaultdict
from datetime import datetime
import functools
import logging
import time

from aiohttp import web
from async_timeout import timeout as _timeout
from dateutil.parser import parse as dtparse
from dateutil.tz import tzutc
import simplejson as json
import sqlalchemy as sa

from sorna.exceptions import ServiceUnavailable, InvalidAPIParameters, QuotaExceeded, \
                             QueryNotImplemented, InstanceNotFound, KernelNotFound, SornaError  # noqa
from . import GatewayStatus
from .auth import auth_required
from .models import KeyPair
from ..manager.registry import InstanceRegistry

_json_type = 'application/json'

log = logging.getLogger('sorna.gateway.kernel')

grace_events = []


@auth_required
async def create(request):
    if request.app['status'] != GatewayStatus.RUNNING:
        raise ServiceUnavailable('Server is initializing...')
    try:
        with _timeout(2):
            params = await request.json()
        log.info(f"GET_OR_CREATE (lang:{params['lang']}, token:{params['clientSessionToken']})")
        assert 8 <= len(params['clientSessionToken']) <= 80
    except (asyncio.TimeoutError, AssertionError,
            KeyError, json.decoder.JSONDecodeError) as e:
        log.warn(f'GET_OR_CREATE: invalid/missing parameters, {e!r}')
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
    except SornaError:
        log.exception('GET_OR_CREATE: API Internal Error')
        raise
    return web.Response(status=201, content_type=_json_type,
                        text=json.dumps(resp))


def grace_event_catcher(func):
    '''
    Catches events during grace periods and prevent event handlers from running.
    '''
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        app = args[0]
        if app['status'] == GatewayStatus.STARTING:
            evinfo = {
                '_type': func.__name__,
                '_handler': func,
                '_when': time.monotonic(),
                '_args': args[1:],
                '_kwargs': kwargs,
            }
            grace_events.append(evinfo)
        else:
            return (await func(*args, **kwargs))
    return wrapped


@grace_event_catcher
async def kernel_terminated(app, reason, kern_id):

    # TODO: enqueue termination event to streaming response queue

    try:
        affected_key = await app.registry.get_kernel(kern_id, field='access_key')
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


@grace_event_catcher
async def instance_started(app, inst_id):
    await app.registry.reset_instance(inst_id)


@grace_event_catcher
async def instance_terminated(app, inst_id, reason):
    if reason == 'agent-lost':
        log.warning(f'agent@{inst_id} heartbeat timeout detected.')

        # In heartbeat timeouts, we do NOT clear Redis keys because
        # the timeout may be a transient one.
        kern_ids = await app.registry.get_kernels_in_instance(inst_id)
        affected_keys = await app.registry.get_kernels(kern_ids, 'access_key')
        await app.registry.update_instance(inst_id, {'status': 'lost'})

        # TODO: enqueue termination event to streaming response queue

        per_key_counts = defaultdict(int)
        for ak in filter(lambda ak: ak is not None, affected_keys):
            per_key_counts[ak] += 1
        log.warning(f'instance_terminated: {kern_ids!r} {per_key_counts}')

        if not affected_keys:
            return

        # TODO: record usage & trigger billing update task (celery?)

        async with app.dbpool.acquire() as conn, conn.transaction():
            for access_key in affected_keys:
                query = sa.update(KeyPair) \
                          .values(concurrency_used=KeyPair.c.concurrency_used
                                                   - per_key_counts[access_key]) \
                          .where(KeyPair.c.access_key == access_key)  # noqa
                await conn.fetchval(query)

    else:
        # In other cases, kernel termination will be triggered by the agent.
        # We don't have to clear them manually.
        pass


@grace_event_catcher
async def instance_heartbeat(app, inst_id, inst_info, running_kernels, interval):
    revived = False
    try:
        inst_status = await app.registry.get_instance(inst_id, 'status')
        if inst_status == 'lost':
            revived = True
    except InstanceNotFound:
        revived = True

    if revived:
        log.warning(f'agent@{inst_id} revived.')
        await app.registry.revive_instance(inst_id, inst_info['addr'])
    else:
        await app.registry.handle_heartbeat(inst_id, inst_info, running_kernels, interval)


@grace_event_catcher
async def instance_stats(app, inst_id, kern_stats, interval):
    if app['status'] == GatewayStatus.RUNNING:
        await app.registry.handle_stats(inst_id, kern_stats, interval)


async def collect_agent_events(app, heartbeat_interval):
    '''
    Collects agent-generated events for a while (via :func:`grace_event_catcher`).
    This allows synchronization of Redis/DB with the current cluster status
    upon (re)starts of the gateway.
    '''

    log.info('running a grace period to detect live agents...')
    app['status'] = GatewayStatus.STARTING
    grace_events.clear()

    await asyncio.sleep(heartbeat_interval * 2.1)

    per_inst_events = defaultdict(list)
    processed_events = []

    for ev in grace_events:
        if ev['_type'] in ('instance_started', 'instance_terminated', 'instance_heartbeat'):
            per_inst_events[ev['_args'][0]].append(ev)

    # Keep only the latest event for each instance.
    for inst_id, events in per_inst_events.items():
        last_event = max(events, key=lambda v: v['_when'])
        processed_events.append(last_event)
        # TODO: sometimes the restarted gateway receives duplicate "instance_terminated" events...

    # Mark instances not detected during event collection to be cleared.
    valid_inst_ids = set(ev['_args'][0] for ev in processed_events)
    terminated_inst_ids = [
        inst_id async for inst_id in app.registry.enumerate_instances(check_shadow=False)
        if inst_id not in valid_inst_ids
    ]

    log.info('bulk-dispatching latest events...')
    app['status'] = GatewayStatus.SYNCING

    for inst_id in terminated_inst_ids:
        log.warning(f'instance {inst_id} is not running!')
        # TODO: clear instance information completely (with access_key updates)

    for ev in processed_events:
        await ev['_handler'](app, *ev['_args'], **ev['_kwargs'])

    log.info('entering normal operation mode...')
    app['status'] = GatewayStatus.RUNNING


@auth_required
async def destroy(request):
    if request.app['status'] != GatewayStatus.RUNNING:
        raise ServiceUnavailable('Server is initializing...')
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
    if request.app['status'] != GatewayStatus.RUNNING:
        raise ServiceUnavailable('Server is initializing...')
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
    if request.app['status'] != GatewayStatus.RUNNING:
        raise ServiceUnavailable('Server is initializing...')
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
    if request.app['status'] != GatewayStatus.RUNNING:
        raise ServiceUnavailable('Server is initializing...')
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

    heartbeat_interval = 3.0
    asyncio.ensure_future(collect_agent_events(app, heartbeat_interval))


async def shutdown(app):
    await app.registry.terminate()
