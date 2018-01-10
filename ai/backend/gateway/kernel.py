'''
Kernel session management.
'''

import asyncio
import base64
from collections import defaultdict
from datetime import datetime
import functools
import json
import logging
import re
import secrets
import time
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
import aiotools
import aiozmq
from aiozmq import create_zmq_stream as aiozmq_sock
from async_timeout import timeout as _timeout
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.sql.expression import true, null
import zmq

from .exceptions import (ServiceUnavailable, InvalidAPIParameters, QuotaExceeded,
                         QueryNotImplemented, KernelNotFound, FolderNotFound,
                         BackendError, InternalServerError)
from . import GatewayStatus
from .auth import auth_required
from .utils import catch_unexpected, method_placeholder
from ..manager.models import keypairs, kernels, vfolders, AgentStatus, KernelStatus
from ..manager.registry import InstanceRegistry

log = logging.getLogger('ai.backend.gateway.kernel')

grace_events = []

_rx_sess_token = re.compile(r'\w[\w.-]*\w', re.ASCII)


def server_ready_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        if request.app['status'] != GatewayStatus.RUNNING:
            raise ServiceUnavailable('Server not ready.')
        return (await handler(request))
    return wrapped


@auth_required
@server_ready_required
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
    return web.json_response(resp, status=201, dumps=json.dumps)


@catch_unexpected(log)
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

    async with app['dbpool'].acquire() as conn:
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


@catch_unexpected(log)
async def kernel_terminated(app, agent_id, kernel_id, reason, kern_stat):
    try:
        kernel = await app['registry'].get_kernel(
            kernel_id, (kernels.c.role, kernels.c.status), allow_stale=True)
    except KernelNotFound:
        # Skip if missing
        return
    # Skip if restarting
    if kernel.status != KernelStatus.RESTARTING:
        await app['registry'].mark_kernel_terminated(kernel_id)
        # TODO: spawn another kernel to keep the capacity of multi-container bundle?
    if kernel.role == 'master':
        sess_id = kernel['sess_id']
        access_key = kernel['access_key']
        stream_key = (sess_id, access_key)
        for handler in app['stream_pty_handlers'][stream_key].copy():
            handler.cancel()
            await handler
        if kernel.status != KernelStatus.RESTARTING:
            await app['registry'].mark_session_terminated(sess_id, access_key)


@catch_unexpected(log)
async def instance_started(app, agent_id):
    # TODO: make feedback to our auto-scaler
    await app['registry'].update_instance(agent_id, {
        'status': AgentStatus.ALIVE,
    })


@catch_unexpected(log)
async def instance_terminated(app, agent_id, reason):
    with app['shared_states'].lock:
        try:
            del app['shared_states'].agent_last_seen[agent_id]
        except KeyError:
            pass
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


@catch_unexpected(log)
async def instance_heartbeat(app, agent_id, agent_info):
    with app['shared_states'].lock:
        app['shared_states'].agent_last_seen[agent_id] = time.monotonic()
    await app['registry'].handle_heartbeat(agent_id, agent_info)


@catch_unexpected(log)
async def check_agent_lost(app, interval):
    try:
        now = time.monotonic()
        with app['shared_states'].lock:
            copied = app['shared_states'].agent_last_seen.copy()
        for agent_id, prev in copied.items():
            if now - prev >= app.config.heartbeat_timeout:
                # TODO: change this to "send_event" (actual zeromq push)
                #       for non-duplicate events
                app['event_dispatcher'].dispatch('instance_terminated',
                                                 agent_id, ('agent-lost', ))
    except asyncio.CancelledError:
        pass


# NOTE: This event is ignored during the grace period.
@catch_unexpected(log)
async def instance_stats(app, agent_id, kern_stats):
    await app['registry'].handle_stats(agent_id, kern_stats)


async def datadog_update(app):
    with app['datadog'].statsd as statsd:

        statsd.gauge('ai.backend.gateway.coroutines', len(asyncio.Task.all_tasks()))

        all_inst_ids = [
            inst_id async for inst_id
            in app['registry'].enumerate_instances()]
        statsd.gauge('ai.backend.gateway.agent_instances', len(all_inst_ids))

        async with app['dbpool'].acquire() as conn:
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
        return web.json_response(resp, status=200, dumps=json.dumps)


@auth_required
@server_ready_required
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
    return web.json_response(resp, status=200, dumps=json.dumps)


@auth_required
@server_ready_required
async def restart(request) -> web.Response:
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    log.info(f"RESTART (u:{access_key}, k:{sess_id})")
    try:
        await registry.increment_session_usage(sess_id, access_key)
        await registry.restart_session(sess_id, access_key)
        for sock in request.app['stream_stdin_socks'][sess_id]:
            sock.close()
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
    return web.json_response(resp, status=200, dumps=json.dumps)


@auth_required
@server_ready_required
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
async def complete(request) -> web.Response:
    resp = {}
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
    return web.json_response(resp, status=200, dumps=json.dumps)


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
        while True:
            if file_count == 20:
                raise InvalidAPIParameters('Too many files')
            file = await reader.next()
            if file is None:
                break
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
async def get_logs(request) -> web.Response:
    resp = {}
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
    return web.json_response(resp, status=200, dumps=json.dumps)


@server_ready_required
async def stream_pty(request) -> web.Response:
    app = request.app
    registry = request.app['registry']
    sess_id = request.match_info['sess_id']
    access_key = request['keypair']['access_key']
    stream_key = (sess_id, access_key)
    extra_fields = (kernels.c.stdin_port, kernels.c.stdout_port)
    try:
        kernel = await registry.get_session(
            sess_id, access_key, field=extra_fields)
    except KernelNotFound:
        raise

    await registry.increment_session_usage(sess_id, access_key)

    # Upgrade connection to WebSocket.
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    app['stream_pty_handlers'][stream_key].add(asyncio.Task.current_task())

    async def connect_streams(kernel):
        kernel_ip = urlparse(kernel.agent_addr).hostname
        stdin_addr = f'tcp://{kernel_ip}:{kernel.stdin_port}'
        log.debug(f'stream_pty({stream_key}): stdin: {stdin_addr}')
        stdin_sock = await aiozmq_sock(zmq.PUB, connect=stdin_addr)
        stdin_sock.transport.setsockopt(zmq.LINGER, 100)
        stdout_addr = f'tcp://{kernel_ip}:{kernel.stdout_port}'
        log.debug(f'stream_pty({stream_key}): stdout: {stdout_addr}')
        stdout_sock = await aiozmq_sock(zmq.SUB, connect=stdout_addr)
        stdout_sock.transport.setsockopt(zmq.LINGER, 100)
        stdout_sock.transport.subscribe(b'')
        return stdin_sock, stdout_sock

    # Wrap sockets in a list so that below coroutines can share reference changes.
    socks = list(await connect_streams(kernel))
    app['stream_stdin_socks'][stream_key].add(socks[0])
    stream_sync = asyncio.Event()

    async def stream_stdin():
        nonlocal socks
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data['type'] == 'stdin':
                        raw_data = base64.b64decode(data['chars'].encode('ascii'))
                        try:
                            socks[0].write([raw_data])
                        except (AttributeError, aiozmq.ZmqStreamClosed):
                            # AttributeError occurs when stdin_sock._transport
                            # is None because it's already closed somewhere
                            # else.
                            app['stream_stdin_socks'][stream_key].remove(socks[0])
                            socks[1].close()
                            kernel = await registry.get_session(
                                sess_id, access_key, field=extra_fields)
                            stdin_sock, stdout_sock = await connect_streams(kernel)
                            socks[0] = stdin_sock
                            socks[1] = stdout_sock
                            app['stream_stdin_socks'][stream_key].add(socks[0])
                            socks[0].write([raw_data])
                            log.debug(f'stream_stdin({stream_key}): '
                                      'zmq stream reset')
                            stream_sync.set()
                            continue
                    else:
                        await registry.increment_session_usage(sess_id, access_key)
                        api_version = 2
                        run_id = secrets.token_hex(8)
                        if data['type'] == 'resize':
                            code = f"%resize {data['rows']} {data['cols']}"
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', code, {})
                        elif data['type'] == 'ping':
                            await registry.execute(
                                sess_id, access_key,
                                api_version, run_id, 'query', '%ping', {})
                        elif data['type'] == 'restart':
                            # Close existing zmq sockets and let stream
                            # handlers get a new one with changed stdin/stdout
                            # ports.
                            log.debug('stream_stdin: restart requested')
                            if not socks[0].at_closing():
                                await registry.restart_session(sess_id, access_key)
                                socks[0].close()
                            else:
                                log.warning(f'stream_stdin({stream_key}): '
                                            'duplicate kernel restart request; '
                                            'ignoring it.')
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.warning(f'stream_stdin({stream_key}): '
                                f'connection closed ({ws.exception()})')
        except asyncio.CancelledError:
            # Agent or kernel is terminated.
            pass
        except:
            app['sentry'].captureException()
            log.exception(f'stream_stdin({stream_key}): unexpected error')
        finally:
            log.debug(f'stream_stdin({stream_key}): terminated')
            if not socks[0].at_closing():
                socks[0].close()

    async def stream_stdout():
        nonlocal socks
        log.debug(f'stream_stdout({stream_key}): started')
        try:
            while True:
                try:
                    data = await socks[1].read()
                except aiozmq.ZmqStreamClosed:
                    await stream_sync.wait()
                    stream_sync.clear()
                    log.debug(f'stream_stdout({stream_key}): zmq stream reset')
                    continue
                if ws.closed:
                    break
                ws.send_str(json.dumps({
                    'type': 'out',
                    'data': base64.b64encode(data[0]).decode('ascii'),
                }, ensure_ascii=False))
        except asyncio.CancelledError:
            pass
        except:
            app['sentry'].captureException()
            log.exception(f'stream_stdout({stream_key}): unexpected error')
        finally:
            log.debug(f'stream_stdout({stream_key}): terminated')
            socks[1].close()

    # According to aiohttp docs, reading ws must be done inside this task.
    # We execute the stdout handler as another task.
    try:
        stdout_task = asyncio.ensure_future(stream_stdout())
        await stream_stdin()
    except:
        app['sentry'].captureException()
        log.exception(f'stream_pty({stream_key}): unexpected error')
    finally:
        app['stream_pty_handlers'][stream_key].remove(asyncio.Task.current_task())
        app['stream_stdin_socks'][stream_key].remove(socks[0])
        stdout_task.cancel()
        await stdout_task
    return ws


@auth_required
async def not_impl_stub(request) -> web.Response:
    raise QueryNotImplemented


async def init(app):
    rt = app.router.add_route
    rt('POST',   r'/v{version:\d+}/kernel/create', create)  # legacy
    rt('POST',   r'/v{version:\d+}/kernel/', create)
    rt('GET',    r'/v{version:\d+}/kernel/{sess_id}', get_info)
    rt('GET',    r'/v{version:\d+}/kernel/{sess_id}/logs', get_logs)

    rt('PATCH',  r'/v{version:\d+}/kernel/{sess_id}', restart)
    rt('DELETE', r'/v{version:\d+}/kernel/{sess_id}', destroy)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}', execute)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/interrupt', interrupt)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/complete', complete)
    rt('GET',    r'/v{version:\d+}/stream/kernel/{sess_id}/pty', stream_pty)
    rt('GET',    r'/v{version:\d+}/stream/kernel/{sess_id}/events', not_impl_stub)
    rt('POST',   r'/v{version:\d+}/kernel/{sess_id}/upload', upload_files)
    rt('POST',   r'/v{version:\d+}/folder/create', not_impl_stub)
    rt('GET',    r'/v{version:\d+}/folder/{folder_id}', not_impl_stub)
    rt('POST',   r'/v{version:\d+}/folder/{folder_id}', method_placeholder('DELETE'))
    rt('DELETE', r'/v{version:\d+}/folder/{folder_id}', not_impl_stub)

    app['event_dispatcher'].add_handler('kernel_terminated', kernel_terminated)
    app['event_dispatcher'].add_handler('instance_started', instance_started)
    app['event_dispatcher'].add_handler('instance_terminated', instance_terminated)
    app['event_dispatcher'].add_handler('instance_heartbeat', instance_heartbeat)
    app['event_dispatcher'].add_handler('instance_stats', instance_stats)

    app['stream_pty_handlers'] = defaultdict(set)
    app['stream_stdin_socks'] = defaultdict(set)

    app['registry'] = InstanceRegistry(
        app['config_server'],
        app['dbpool'],
        app['redis_stat_pool'])
    await app['registry'].init()

    # Scan ALIVE agents
    if app['pidx'] == 0:
        log.debug(f'initializing agent status checker at proc:{app["pidx"]}')
        now = time.monotonic()
        async for inst in app['registry'].enumerate_instances():
            app['shared_states'].agent_last_seen[inst.id] = now
        app['agent_lost_checker'] = aiotools.create_timer(
            functools.partial(check_agent_lost, app), 1.0)

    app['shared_states'].barrier.wait()
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
    for per_kernel_handlers in app['stream_pty_handlers'].values():
        for handler in per_kernel_handlers.copy():
            handler.cancel()
            await handler
    await app['registry'].terminate()
