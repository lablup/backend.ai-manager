'''
REST-style kernel session management APIs.
'''

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import functools
import json
import logging
import re
import secrets

from aiohttp import web
import aiotools
from aiojobs.aiohttp import atomic
from async_timeout import timeout as _timeout
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.sql.expression import true, null

from .exceptions import (InvalidAPIParameters, QuotaExceeded,
                         KernelNotFound, FolderNotFound,
                         BackendError, InternalServerError)
from . import GatewayStatus
from .auth import auth_required
from .utils import catch_unexpected, server_ready_required
from ..manager.models import keypairs, kernels, vfolders, AgentStatus, KernelStatus

log = logging.getLogger('ai.backend.gateway.kernel')

grace_events = []

_rx_sess_token = re.compile(r'\w[\w.-]*\w', re.ASCII)


@auth_required
@server_ready_required
@atomic
async def create(request) -> web.Response:
    try:
        with _timeout(2):
            params = await request.json(loads=json.loads)
        assert params.get('lang'), \
               'lang is missing or empty!'
        assert params.get('clientSessionToken'), \
               'clientSessionToken is missing or empty!'
        sess_id = params['clientSessionToken']
        assert 4 <= len(sess_id) <= 64, \
               'clientSessionToken is too short or long (4 to 64 bytes required)!'
        assert _rx_sess_token.fullmatch(sess_id), \
               'clientSessionToken contains invalid characters.'
        log.info(f"GET_OR_CREATE (u:{request['keypair']['access_key']}, "
                 f"lang:{params['lang']}, token:{sess_id})")
    except (asyncio.TimeoutError, AssertionError,
            json.decoder.JSONDecodeError) as e:
        log.warning(f'GET_OR_CREATE: invalid/missing parameters, {e!r}')
        raise InvalidAPIParameters(extra_msg=str(e.args[0]))
    resp = {}
    try:
        access_key = request['keypair']['access_key']
        concurrency_limit = request['keypair']['concurrency_limit']
        async with request.app['dbpool'].acquire() as conn, conn.begin():
            query = (sa.select([keypairs.c.concurrency_used], for_update=True)
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == access_key))
            concurrency_used = await conn.scalar(query)
            log.debug(f'access_key: {access_key} '
                      f'({concurrency_used} / {concurrency_limit})')
            if concurrency_used >= concurrency_limit:
                raise QuotaExceeded
            creation_config = {
                'mounts': None,
                'environ': None,
                'clusterSize': None,
                'instanceMemory': None,
                'instanceCores': None,
                'instanceGPUs': None,
            }
            if request['api_version'] == 1:
                # custom resource limit unsupported
                pass
            elif request['api_version'] in (2, 3):
                creation_config.update(params.get('config', {}))
            # sanity check for vfolders
            if creation_config['mounts']:
                mount_details = []
                for mount in creation_config['mounts']:
                    query = (sa.select('*').select_from(vfolders)
                                .where((vfolders.c.belongs_to == access_key) &
                                       (vfolders.c.name == mount)))
                    result = await conn.execute(query)
                    row = await result.first()
                    if row is None:
                        raise FolderNotFound
                    else:
                        mount_details.append([
                            row.name,
                            row.host,
                            row.id.hex
                        ])
                creation_config['mounts'] = mount_details
            kernel, created = await request.app['registry'].get_or_create_session(
                sess_id, access_key,
                params['lang'], creation_config,
                conn=conn)
            resp['kernelId'] = str(kernel['sess_id'])
            resp['created'] = bool(created)
            if created:
                query = (sa.update(keypairs)
                           .values(concurrency_used=keypairs.c.concurrency_used + 1)
                           .where(keypairs.c.access_key == access_key))
                await conn.execute(query)
    except BackendError:
        log.exception('GET_OR_CREATE: exception')
        raise
    except Exception:
        request.app['sentry'].captureException()
        log.exception('GET_OR_CREATE: unexpected error!')
        raise InternalServerError
    return web.json_response(resp, status=201)


async def update_instance_usage(app, inst_id):
    sess_ids = await app['registry'].get_sessions_in_instance(inst_id)
    all_sessions = await app['registry'].get_sessions(sess_ids)
    affected_keys = [kern['access_key'] for kern in all_sessions if kern is not None]

    # TODO: enqueue termination event to streaming response queue

    per_key_counts = defaultdict(int)
    for ak in filter(lambda ak: ak is not None, affected_keys):
        per_key_counts[ak] += 1
    per_key_counts_str = ', '.join(f'{k}:{v}' for k, v in per_key_counts.items())
    log.info(f'-> cleaning {sess_ids!r}')
    log.info(f'-> per-key usage: {per_key_counts_str}')

    if not affected_keys:
        return

    async with app['dbpool'].acquire() as conn, conn.begin():
        log.debug(f'update_instance_usage({inst_id})')
        for kern in all_sessions:
            if kern is None:
                continue
            query = (sa.update(keypairs)
                       .values({
                           'concurrency_used': keypairs.c.concurrency_used -
                                               per_key_counts[kern['access_key']],  # noqa
                       })
                       .where(keypairs.c.access_key == kern['access_key']))
            await conn.execute(query)


async def kernel_terminated(app, agent_id, kernel_id, reason, kern_stat):
    try:
        kernel = await app['registry'].get_kernel(
            kernel_id, (kernels.c.role, kernels.c.status), allow_stale=True)
    except KernelNotFound:
        return
    if kernel.status != KernelStatus.RESTARTING:
        await app['registry'].mark_kernel_terminated(kernel_id)
        # TODO: spawn another kernel to keep the capacity of multi-container bundle?
    if kernel.role == 'master' and kernel.status != KernelStatus.RESTARTING:
        await app['registry'].mark_session_terminated(
            kernel['sess_id'],
            kernel['access_key'])


async def instance_started(app, agent_id):
    # TODO: make feedback to our auto-scaler
    await app['registry'].update_instance(agent_id, {
        'status': AgentStatus.ALIVE,
    })


async def instance_terminated(app, agent_id, reason):
    if reason == 'agent-lost':
        await app['registry'].mark_agent_terminated(agent_id, AgentStatus.LOST)
    elif reason == 'agent-restart':
        log.info(f'agent@{agent_id} restarting for maintenance.')
        await app['registry'].update_instance(agent_id, {
            'status': AgentStatus.RESTARTING,
        })
    else:
        # On normal instance termination, kernel_terminated events were already
        # triggered by the agent.
        await app['registry'].mark_agent_terminated(agent_id, AgentStatus.TERMINATED)


async def instance_heartbeat(app, agent_id, agent_info):
    await app['registry'].handle_heartbeat(agent_id, agent_info)


@catch_unexpected(log)
async def check_agent_lost(app, interval):
    try:
        now = datetime.now(tzutc())
        timeout = timedelta(seconds=app['config'].heartbeat_timeout)
        async for agent_id, prev in app['redis_live'].ihscan('last_seen'):
            prev = datetime.fromtimestamp(float(prev), tzutc())
            if now - prev > timeout:
                await app['event_dispatcher'].dispatch('instance_terminated',
                                                       agent_id, ('agent-lost', ))
    except asyncio.CancelledError:
        pass


# NOTE: This event is ignored during the grace period.
async def instance_stats(app, agent_id, kern_stats):
    await app['registry'].handle_stats(agent_id, kern_stats)


async def datadog_update(app):
    with app['datadog'].statsd as statsd:

        statsd.gauge('ai.backend.gateway.coroutines', len(asyncio.Task.all_tasks()))

        all_inst_ids = [
            inst_id async for inst_id
            in app['registry'].enumerate_instances()]
        statsd.gauge('ai.backend.gateway.agent_instances', len(all_inst_ids))

        async with app['dbpool'].acquire() as conn, conn.begin():
            query = (sa.select([sa.func.sum(keypairs.c.concurrency_used)])
                       .select_from(keypairs))
            n = await conn.scalar(query)
            statsd.gauge('ai.backend.gateway.active_kernels', n)

            subquery = (sa.select([sa.func.count()])
                          .select_from(keypairs)
                          .where(keypairs.c.is_active == true())
                          .group_by(keypairs.c.user_id))
            query = sa.select([sa.func.count()]).select_from(subquery.alias())
            n = await conn.scalar(query)
            statsd.gauge('ai.backend.users.has_active_key', n)

            subquery = subquery.where(keypairs.c.last_used != null())
            query = sa.select([sa.func.count()]).select_from(subquery.alias())
            n = await conn.scalar(query)
            statsd.gauge('ai.backend.users.has_used_key', n)

            '''
            query = sa.select([sa.func.count()]).select_from(usage)
            n = await conn.scalar(query)
            statsd.gauge('ai.backend.gateway.accum_kernels', n)
            '''


async def datadog_update_timer(app):
    if app['datadog'] is None:
        return
    while True:
        try:
            await datadog_update(app)
        except asyncio.CancelledError:
            break
        except:
            app['sentry'].captureException()
            log.exception('datadog_update unexpected error')
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break


@auth_required
@server_ready_required
@atomic
async def destroy(request) -> web.Response:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"DESTROY (u:{access_key}, k:{sess_id})")
    try:
        last_stat = await registry.destroy_session(sess_id, access_key)
    except BackendError:
        log.exception('DESTROY: exception')
        raise
    else:
        resp = {
            'stats': last_stat,
        }
        return web.json_response(resp, status=200)


@auth_required
@server_ready_required
@atomic
async def get_info(request) -> web.Response:
    # NOTE: This API should be replaced with GraphQL version.
    resp = {}
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"GETINFO (u:{access_key}, k:{sess_id})")
    try:
        await registry.increment_session_usage(sess_id, access_key)
        kern = await registry.get_session(sess_id, access_key, field='*')
        resp['lang'] = kern.lang
        age = datetime.now(tzutc()) - kern.created_at
        resp['age'] = age.total_seconds() * 1000
        # Resource limits collected from agent heartbeats
        # TODO: factor out policy/image info as a common repository
        resp['queryTimeout']  = -1  # deprecated
        resp['idleTimeout']   = -1  # deprecated
        resp['memoryLimit']   = kern.mem_max_bytes >> 10  # KiB
        resp['maxCpuCredit']  = -1  # deprecated
        # Stats collected from agent heartbeats
        resp['numQueriesExecuted'] = kern.num_queries
        resp['idle']          = -1  # deprecated
        resp['memoryUsed']    = -1  # deprecated
        resp['cpuCreditUsed'] = kern.cpu_used
        log.info(f'information retrieved: {resp!r}')
    except BackendError:
        log.exception('GETINFO: exception')
        raise
    return web.json_response(resp, status=200)


@auth_required
@server_ready_required
@atomic
async def restart(request) -> web.Response:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"RESTART (u:{access_key}, k:{sess_id})")
    try:
        await registry.increment_session_usage(sess_id, access_key)
        await registry.restart_session(sess_id, access_key)
    except BackendError:
        log.exception('RESTART: exception')
        raise
    except:
        request.app['sentry'].captureException()
        log.exception('RESTART: unexpected error')
        raise web.HTTPInternalServerError
    return web.Response(status=204)


@auth_required
@server_ready_required
async def execute(request) -> web.Response:
    resp = {}
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    try:
        with _timeout(2):
            params = await request.json(loads=json.loads)
        log.info(f"EXECUTE(u:{access_key}, k:{sess_id})")
    except (asyncio.TimeoutError, json.decoder.JSONDecodeError):
        log.warning('EXECUTE: invalid/missing parameters')
        raise InvalidAPIParameters
    try:
        await registry.increment_session_usage(sess_id, access_key)
        api_version = request['api_version']
        if api_version == 1:
            run_id = params.get('runId', secrets.token_hex(8))
            mode = 'query'
            code = params.get('code', '')
            opts = {}
        elif api_version >= 2:
            assert 'runId' in params, 'runId is missing!'
            run_id = params['runId']  # maybe None
            assert params.get('mode'), 'mode is missing or empty!'
            mode = params['mode']
            assert mode in {'query', 'batch', 'complete', 'continue', 'input'}, \
                   'mode has an invalid value.'
            if mode in {'continue', 'input'}:
                assert run_id is not None, 'continuation requires explicit run ID'
            code = params.get('code', '')
            opts = params.get('options', None) or {}
        if mode == 'complete':
            # For legacy
            resp['result'] = await registry.get_completions(
                sess_id, access_key, code, opts)
        else:
            raw_result = await registry.execute(
                sess_id, access_key,
                api_version, run_id, mode, code, opts)
            if raw_result is None:
                # the kernel may have terminated from its side,
                # or there was interruption of agents.
                resp['result'] = {
                    'status': 'finished',
                    'runId': run_id,
                    'exitCode': 130,
                    'options': {},
                    'files': [],
                    'console': [],
                }
                return web.json_response(resp, status=200)
            # Keep internal/public API compatilibty
            result = {
                'status': raw_result['status'],
                'runId': raw_result['runId'],
                'exitCode': raw_result.get('exitCode'),
                'options': raw_result.get('options'),
                'files': raw_result.get('files'),
            }
            if api_version == 1:
                result['stdout'] = raw_result.get('stdout')
                result['stderr'] = raw_result.get('stderr')
                result['media'] = raw_result.get('media')
                result['html'] = raw_result.get('html')
            else:
                result['console'] = raw_result.get('console')
            resp['result'] = result
    except AssertionError as e:
        log.warning(f'EXECUTE: invalid/missing parameters: {e}')
        raise InvalidAPIParameters(extra_msg=e.args[0])
    except BackendError:
        log.exception('EXECUTE: exception')
        raise
    return web.json_response(resp, status=200)


@auth_required
@server_ready_required
@atomic
async def interrupt(request) -> web.Response:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"INTERRUPT(u:{access_key}, k:{sess_id})")
    try:
        await registry.increment_session_usage(sess_id, access_key)
        await registry.interrupt_session(sess_id, access_key)
    except BackendError:
        log.exception('INTERRUPT: exception')
        raise
    return web.Response(status=204)


@auth_required
@server_ready_required
@atomic
async def complete(request) -> web.Response:
    resp = {'result': {
        'status': 'finished',
        'completions': [],
    }}
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    try:
        with _timeout(2):
            params = await request.json(loads=json.loads)
        log.info(f"COMPLETE(u:{access_key}, k:{sess_id})")
    except (asyncio.TimeoutError, json.decoder.JSONDecodeError):
        log.warning('COMPLETE: invalid/missing parameters')
        raise InvalidAPIParameters
    try:
        code = params.get('code', '')
        opts = params.get('options', None) or {}
        await registry.increment_session_usage(sess_id, access_key)
        resp['result'] = await request.app['registry'].get_completions(
            sess_id, code, opts)
    except AssertionError:
        log.warning('COMPLETE: invalid/missing parameters')
        raise InvalidAPIParameters
    except BackendError:
        log.exception('COMPLETE: exception')
        raise
    return web.json_response(resp, status=200)


@auth_required
@server_ready_required
async def upload_files(request) -> web.Response:
    loop = asyncio.get_event_loop()
    reader = await request.multipart()
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    try:
        await registry.increment_session_usage(sess_id, access_key)
        file_count = 0
        upload_tasks = []
        async for file in aiotools.aiter(reader.next, None):
            if file_count == 20:
                raise InvalidAPIParameters('Too many files')
            file_count += 1
            # This API handles only small files, so let's read it at once.
            chunk = await file.read_chunk(size=1048576)
            if not file.at_eof():
                raise InvalidAPIParameters('Too large file')
            data = file.decode(chunk)
            log.debug(f'received file: {file.filename} ({len(data):,} bytes)')
            t = loop.create_task(
                registry.upload_file(sess_id, access_key, file.filename, data))
            upload_tasks.append(t)
        await asyncio.gather(*upload_tasks)
    except BackendError:
        log.exception('UPLOAD_FILES: exception')
        raise
    return web.Response(status=204)


@auth_required
@server_ready_required
@atomic
async def get_logs(request) -> web.Response:
    resp = {'result': {'logs': ''}}
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"GETLOG (u:{request['keypair']['access_key']}, k:{sess_id})")
    try:
        await registry.increment_session_usage(sess_id, access_key)
        resp['result'] = await registry.get_logs(sess_id, access_key)
        log.info(f'container log retrieved: {resp!r}')
    except BackendError:
        log.exception('GETLOG: exception')
        raise
    return web.json_response(resp, status=200)


async def init(app):
    event_dispatcher = app['event_dispatcher']
    event_dispatcher.add_handler('kernel_terminated', app, kernel_terminated)
    event_dispatcher.add_handler('instance_started', app, instance_started)
    event_dispatcher.add_handler('instance_terminated', app, instance_terminated)
    event_dispatcher.add_handler('instance_heartbeat', app, instance_heartbeat)
    event_dispatcher.add_handler('instance_stats', app, instance_stats)

    # Scan ALIVE agents
    if app['pidx'] == 0:
        log.debug(f'initializing agent status checker at proc:{app["pidx"]}')
        app['agent_lost_checker'] = aiotools.create_timer(
            functools.partial(check_agent_lost, app), 1.0)

    app['status'] = GatewayStatus.RUNNING


async def shutdown(app):
    if app['pidx'] == 0:
        app['agent_lost_checker'].cancel()
        await app['agent_lost_checker']

    checked_tasks = ('kernel_agent_event_collector', 'kernel_ddtimer')
    for tname in checked_tasks:
        t = app.get(tname, None)
        if t and not t.done():
            t.cancel()
            await t


def create_app():
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (1, 2, 3)
    app.router.add_route('POST',   r'/create', create)  # legacy
    app.router.add_route('POST',   r'', create)
    app.router.add_route('GET',    r'/{sess_id}', get_info)
    app.router.add_route('GET',    r'/{sess_id}/logs', get_logs)
    app.router.add_route('PATCH',  r'/{sess_id}', restart)
    app.router.add_route('DELETE', r'/{sess_id}', destroy)
    app.router.add_route('POST',   r'/{sess_id}', execute)
    app.router.add_route('POST',   r'/{sess_id}/interrupt', interrupt)
    app.router.add_route('POST',   r'/{sess_id}/complete', complete)
    app.router.add_route('POST',   r'/{sess_id}/upload', upload_files)
    return app, []
