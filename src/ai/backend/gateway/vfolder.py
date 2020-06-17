import asyncio
import functools
import json
import logging
import os
from pathlib import Path
import shutil
import stat
from typing import (
    Any, Awaitable, Callable, Union,
    Dict, Mapping, MutableMapping,
    Tuple,
    Set,
)
import urllib.parse
import uuid
import jwt
from datetime import datetime, timedelta

import aiofiles
import aiohttp
from aiohttp import web, hdrs
import aiohttp_cors
import aiojobs
from aiojobs.aiohttp import atomic
import aiotools
import janus
import multidict
import sqlalchemy as sa
import psycopg2
import trafaret as t
import zipstream

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.utils import Fstab

from .auth import auth_required, superadmin_required
from .config import DEFAULT_CHUNK_SIZE, DEFAULT_INFLIGHT_CHUNKS
from .exceptions import (
    VFolderCreationFailed, VFolderNotFound, VFolderAlreadyExists,
    GenericForbidden, GenericNotFound, InvalidAPIParameters, ServerMisconfiguredError,
    BackendAgentError, InternalServerError,
)
from .manager import (
    READ_ALLOWED, ALL_ALLOWED,
    server_status_required,
)
from .resource import get_watcher_info
from .utils import check_api_params, current_loop
from ..manager.models import (
    agents, AgentStatus, kernels, KernelStatus,
    users, groups, keypairs, vfolders, vfolder_invitations, vfolder_permissions,
    VFolderInvitationState, VFolderPermission, VFolderPermissionValidator,
    query_accessible_vfolders,
    get_allowed_vfolder_hosts_by_group, get_allowed_vfolder_hosts_by_user,
    UserRole,
    verify_vfolder_name
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.vfolder'))


class _Sentinel:
    pass


eof_sentinel = _Sentinel()

VFolderRow = Mapping[str, Any]


def vfolder_permission_required(perm: VFolderPermission):
    '''
    Checks if the target vfolder exists and is either:
    - owned by the current access key, or
    - allowed accesses by the access key under the specified permission.

    The decorated handler should accept an extra argument
    which contains a dict object describing the matched VirtualFolder table row.
    '''

    # FIXME: replace ... with [web.Request, VFolderRow, Any...] in the future mypy
    def _wrapper(handler: Callable[..., Awaitable[web.Response]]):

        @functools.wraps(handler)
        async def _wrapped(request: web.Request, *args, **kwargs) -> web.Response:
            dbpool = request.app['dbpool']
            domain_name = request['user']['domain_name']
            user_role = request['user']['role']
            user_uuid = request['user']['uuid']
            folder_name = request.match_info['name']
            allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
            if perm == VFolderPermission.READ_ONLY:
                # if READ_ONLY is requested, any permission accepts.
                perm_cond = vfolder_permissions.c.permission.in_([
                    VFolderPermission.READ_ONLY,
                    VFolderPermission.READ_WRITE,
                    VFolderPermission.RW_DELETE,
                ])
            elif perm == VFolderPermission.READ_WRITE:
                # if READ_WRITE is requested, both READ_WRITE and RW_DELETE accepts.
                perm_cond = vfolder_permissions.c.permission.in_([
                    VFolderPermission.READ_WRITE,
                    VFolderPermission.RW_DELETE,
                ])
            elif perm == VFolderPermission.RW_DELETE:
                # If RW_DELETE is requested, only RW_DELETE accepts.
                perm_cond = (
                    vfolder_permissions.c.permission == VFolderPermission.RW_DELETE
                )
            else:
                # Otherwise, just compare it as-is (for future compatibility).
                perm_cond = (vfolder_permissions.c.permission == perm)
            async with dbpool.acquire() as conn:
                entries = await query_accessible_vfolders(
                    conn, user_uuid,
                    user_role=user_role, domain_name=domain_name,
                    allowed_vfolder_types=allowed_vfolder_types,
                    extra_vf_conds=(vfolders.c.name == folder_name),
                    extra_vfperm_conds=perm_cond)
                if len(entries) == 0:
                    raise VFolderNotFound(
                        'Your operation may be permission denied.')
            return await handler(request, entries[0], *args, **kwargs)

        return _wrapped

    return _wrapper


# FIXME: replace ... with [web.Request, VFolderRow, Any...] in the future mypy
def vfolder_check_exists(handler: Callable[..., Awaitable[web.Response]]):
    '''
    Checks if the target vfolder exists and is owned by the current user.

    The decorated handler should accept an extra "row" argument
    which contains the matched VirtualFolder table row.
    '''

    @functools.wraps(handler)
    async def _wrapped(request: web.Request, *args, **kwargs) -> web.Response:
        dbpool = request.app['dbpool']
        user_uuid = request['user']['uuid']
        folder_name = request.match_info['name']
        async with dbpool.acquire() as conn:
            j = sa.join(
                vfolders, vfolder_permissions,
                vfolders.c.id == vfolder_permissions.c.vfolder, isouter=True)
            query = (
                sa.select('*')
                .select_from(j)
                .where(((vfolders.c.user == user_uuid) |
                        (vfolder_permissions.c.user == user_uuid)) &
                       (vfolders.c.name == folder_name)))
            try:
                result = await conn.execute(query)
            except psycopg2.DataError:
                raise InvalidAPIParameters
            row = await result.first()
            if row is None:
                raise VFolderNotFound()
        return await handler(request, row, *args, **kwargs)

    return _wrapped


@auth_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('name'): tx.Slug(allow_dot=True),
        t.Key('host', default=None) >> 'folder_host': t.String | t.Null,
        tx.AliasedKey(['group', 'groupId', 'group_id'], default=None): tx.UUID | t.String | t.Null,
    }),
)
async def create(request: web.Request, params: Any) -> web.Response:
    resp: Dict[str, Any] = {}
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']
    resource_policy = request['keypair']['resource_policy']
    domain_name = request['user']['domain_name']
    group_id_or_name = params['group']
    log.info('VFOLDER.CREATE (ak:{}, vf:{}, vfh:{})',
             access_key, params['name'], params['folder_host'])
    # Resolve host for the new virtual folder.
    folder_host = params['folder_host']
    if not folder_host:
        folder_host = \
            await request.app['config_server'].etcd.get('volumes/_default_host')
        if not folder_host:
            raise InvalidAPIParameters(
                'You must specify the vfolder host '
                'because the default host is not configured.')
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    for vf_type in allowed_vfolder_types:
        if vf_type not in ('user', 'group'):
            raise ServerMisconfiguredError(
                f'Invalid vfolder type(s): {str(allowed_vfolder_types)}.'
                ' Only "user" or "group" is allowed.')

    if not verify_vfolder_name(params['name']):
        raise InvalidAPIParameters(f'{params["name"]} is reserved for internal operations.')
    if params['name'].startswith('.'):
        if params['group'] is not None:
            raise InvalidAPIParameters('dot-prefixed vfolders cannot be a group folder.')

    async with dbpool.acquire() as conn:
        # Convert group name to uuid if group name is given.
        if isinstance(group_id_or_name, str):
            query = (sa.select([groups.c.id])
                     .select_from(groups)
                     .where(groups.c.domain_name == domain_name)
                     .where(groups.c.name == group_id_or_name))
            group_id = await conn.scalar(query)
        else:
            group_id = group_id_or_name
        # Check resource policy's allowed_vfolder_hosts
        allowed_hosts = await get_allowed_vfolder_hosts_by_group(conn, resource_policy,
                                                                 domain_name, group_id)
        if folder_host not in allowed_hosts:
            raise InvalidAPIParameters('You are not allowed to use this vfolder host.')
        vfroot = (request.app['VFOLDER_MOUNT'] / folder_host /
                  request.app['VFOLDER_FSPREFIX'])
        if not vfroot.is_dir():
            raise InvalidAPIParameters(f'Invalid vfolder host: {folder_host}')

        # Check resource policy's max_vfolder_count
        if resource_policy['max_vfolder_count'] > 0:
            query = (sa.select([sa.func.count()])
                       .where(vfolders.c.user == user_uuid))
            result = await conn.scalar(query)
            if result >= resource_policy['max_vfolder_count']:
                raise InvalidAPIParameters('You cannot create more vfolders.')

        # Prevent creation of vfolder with duplicated name.
        entries = await query_accessible_vfolders(
            conn, user_uuid,
            user_role=user_role, domain_name=domain_name,
            allowed_vfolder_types=allowed_vfolder_types,
            extra_vf_conds=(sa.and_(vfolders.c.name == params['name'],
                                    vfolders.c.host == folder_host))
        )
        if len(entries) > 0:
            raise VFolderAlreadyExists

        # Check if group exists.
        if group_id_or_name and group_id is None:
            raise InvalidAPIParameters('no such group')
        if group_id is not None:
            if 'group' not in allowed_vfolder_types:
                raise InvalidAPIParameters('group vfolder cannot be created in this host')
            if not request['is_admin'] or request['is_superadmin']:
                # Superadmin will not manipulate group's vfolder (at least currently).
                raise GenericForbidden('no permission')
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == domain_name)
                       .where(groups.c.id == group_id))
            _gid = await conn.scalar(query)
            if str(_gid) != str(group_id):
                raise InvalidAPIParameters('No such group.')
        else:
            if 'user' not in allowed_vfolder_types:
                raise InvalidAPIParameters('user vfolder cannot be created in this host')
        try:
            folder_id = uuid.uuid4().hex
            folder_path = (request.app['VFOLDER_MOUNT'] / folder_host /
                           request.app['VFOLDER_FSPREFIX'] / folder_id)
            loop = current_loop()
            await loop.run_in_executor(None, lambda: folder_path.mkdir(parents=True, exist_ok=True))
        except OSError:
            raise VFolderCreationFailed
        user_uuid = str(user_uuid) if group_id is None else None
        group_uuid = str(group_id) if group_id is not None else None
        query = (vfolders.insert().values({
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
            'last_used': None,
            'creator': request['user']['email'],
            'user': user_uuid,
            'group': group_uuid,
        }))
        resp = {
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
            'creator': request['user']['email'],
            'user': user_uuid,
            'group': group_uuid,
        }
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        assert result.rowcount == 1
    return web.json_response(resp, status=201)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('all', default=False): t.Bool | t.StrBool,
        tx.AliasedKey(['group_id', 'groupId'], default=None): tx.UUID | t.String | t.Null,
    }),
)
async def list_folders(request: web.Request, params: Any) -> web.Response:
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']

    log.info('VFOLDER.LIST (ak:{})', access_key)
    async with dbpool.acquire() as conn:
        allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
        if request['is_superadmin'] and params['all']:
            # List all folders for superadmin if all is specified
            j = (vfolders.join(users, vfolders.c.user == users.c.uuid, isouter=True)
                         .join(groups, vfolders.c.group == groups.c.id, isouter=True))
            query = (sa.select([vfolders, users.c.email, groups.c.name], use_labels=True)
                       .select_from(j))
            result = await conn.execute(query)
            entries = []
            async for row in result:
                is_owner = True if row.vfolders_user == user_uuid else False
                permission = VFolderPermission.OWNER_PERM if is_owner \
                        else VFolderPermission.READ_ONLY
                entries.append({
                    'name': row.vfolders_name,
                    'id': row.vfolders_id,
                    'host': row.vfolders_host,
                    'created_at': row.vfolders_created_at,
                    'is_owner': is_owner,
                    'permission': permission,
                    'user': str(row.vfolders_user) if row.vfolders_user else None,
                    'group': str(row.vfolders_group) if row.vfolders_group else None,
                    'creator': row.vfolders_creator,
                    'user_email': row.users_email,
                    'group_name': row.groups_name,
                    'type': 'user' if row['vfolders_user'] is not None else 'group',
                })
        else:
            extra_vf_conds = None
            if params['group_id'] is not None:
                # Note: user folders should be returned even when group_id is specified.
                extra_vf_conds = ((vfolders.c.group == params['group_id']) |
                                  (vfolders.c.user.isnot(None)))
            entries = await query_accessible_vfolders(
                conn, user_uuid,
                user_role=user_role, domain_name=domain_name,
                allowed_vfolder_types=allowed_vfolder_types,
                extra_vf_conds=extra_vf_conds,
            )
        for entry in entries:
            resp.append({
                'name': entry['name'],
                'id': entry['id'].hex,
                'host': entry['host'],
                'created_at': str(entry['created_at']),
                'is_owner': entry['is_owner'],
                'permission': entry['permission'].value,
                'user': str(entry['user']),
                'group': str(entry['group']),
                'creator': entry['creator'],
                'user_email': entry['user_email'],
                'group_name': entry['group_name'],
                'type': 'user' if entry['user'] is not None else 'group',
            })
    return web.json_response(resp, status=200)


@superadmin_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('id'): t.String,
    }),
)
async def delete_by_id(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.DELETE_BY_ID (ak:{}, vf:{})', access_key, params['id'])
    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select([vfolders.c.host])
                   .select_from(vfolders)
                   .where(vfolders.c.id == params['id']))
        folder_host = await conn.scalar(query)
        folder_id = uuid.UUID(params['id'])
        folder_hex = folder_id.hex
        folder_path = (request.app['VFOLDER_MOUNT'] / folder_host /
                       request.app['VFOLDER_FSPREFIX'] / folder_hex)
        query = (vfolders.delete().where(vfolders.c.id == folder_id))
        await conn.execute(query)
        try:
            loop = current_loop()
            await loop.run_in_executor(None, lambda: shutil.rmtree(folder_path))  # type: ignore
        except IOError:
            pass
    return web.Response(status=204)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def list_hosts(request: web.Request) -> web.Response:
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_HOSTS (ak:{})', access_key)
    config = request.app['config_server']
    dbpool = request.app['dbpool']
    domain_name = request['user']['domain_name']
    domain_admin = request['user']['role'] == UserRole.ADMIN
    resource_policy = request['keypair']['resource_policy']
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    async with dbpool.acquire() as conn:
        allowed_hosts: Set[str] = set()
        if 'user' in allowed_vfolder_types:
            allowed_hosts_by_user = await get_allowed_vfolder_hosts_by_user(
                conn, resource_policy, domain_name, request['user']['uuid'])
            allowed_hosts = allowed_hosts | allowed_hosts_by_user
        if 'group' in allowed_vfolder_types:
            allowed_hosts_by_group = await get_allowed_vfolder_hosts_by_group(
                conn, resource_policy, domain_name, group_id=None, domain_admin=domain_admin)
            allowed_hosts = allowed_hosts | allowed_hosts_by_group
    mount_prefix = await config.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'
    mounted_hosts = set(p.name for p in Path(mount_prefix).iterdir() if p.is_dir())
    allowed_hosts = allowed_hosts & mounted_hosts
    default_host = await config.get('volumes/_default_host')
    if default_host not in allowed_hosts:
        default_host = None
    resp = {
        'default': default_host,
        'allowed': sorted(allowed_hosts),
    }
    return web.json_response(resp, status=200)


@atomic
@superadmin_required
@server_status_required(READ_ALLOWED)
async def list_all_hosts(request: web.Request) -> web.Response:
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_ALL_HOSTS (ak:{})', access_key)
    config = request.app['config_server']
    mount_prefix = await config.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'
    mounted_hosts = set(p.name for p in Path(mount_prefix).iterdir() if p.is_dir())
    default_host = await config.get('volumes/_default_host')
    if default_host not in mounted_hosts:
        default_host = None
    resp = {
        'default': default_host,
        'allowed': sorted(mounted_hosts),
    }
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def list_allowed_types(request: web.Request) -> web.Response:
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_ALLOWED_TYPES (ak:{})', access_key)
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    return web.json_response(allowed_vfolder_types, status=200)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def get_info(request: web.Request, row: VFolderRow) -> web.Response:
    resp: Dict[str, Any] = {}
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.GETINFO (ak:{}, vf:{})', access_key, folder_name)
    if row['permission'] is None:
        is_owner = True
        permission = VFolderPermission.OWNER_PERM
    else:
        is_owner = row['is_owner']
        permission = row['permission']
    # TODO: handle nested directory structure
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    loop = current_loop()
    num_files = await loop.run_in_executor(None, lambda: len(list(folder_path.iterdir())))
    resp = {
        'name': row['name'],
        'id': row['id'].hex,
        'host': row['host'],
        'numFiles': num_files,  # legacy
        'num_files': num_files,
        'created': str(row['created_at']),  # legacy
        'created_at': str(row['created_at']),
        'last_used': str(row['created_at']),
        'user': str(row['user']),
        'group': str(row['group']),
        'type': 'user' if row['user'] is not None else 'group',
        'is_owner': is_owner,
        'permission': permission,
    }
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
@vfolder_permission_required(VFolderPermission.OWNER_PERM)
@check_api_params(
    t.Dict({
        t.Key('new_name'): tx.Slug(allow_dot=True),
    }))
async def rename(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    dbpool = request.app['dbpool']
    old_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']
    new_name = params['new_name']
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    log.info('VFOLDER.RENAME (ak:{}, vf.old:{}, vf.new:{})',
             access_key, old_name, new_name)
    async with dbpool.acquire() as conn:
        entries = await query_accessible_vfolders(
            conn, user_uuid,
            user_role=user_role, domain_name=domain_name,
            allowed_vfolder_types=allowed_vfolder_types)
        for entry in entries:
            if entry['name'] == new_name:
                raise InvalidAPIParameters(
                    'One of your accessible vfolders already has '
                    'the name you requested.')
        for entry in entries:
            if entry['name'] == old_name:
                if not entry['is_owner']:
                    raise InvalidAPIParameters(
                        'Cannot change the name of a vfolder '
                        'that is not owned by me.')
                query = (
                    vfolders.update()
                    .values(name=new_name)
                    .where(vfolders.c.id == entry['id']))
                await conn.execute(query)
                break
    return web.Response(status=201)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_WRITE)
@check_api_params(
    t.Dict({
        t.Key('path'): t.String,
    }))
async def mkdir(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    path = Path(params['path'])
    log.info('VFOLDER.MKDIR (ak:{}, vf:{}, path:{})', access_key, folder_name, path)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    assert not path.is_absolute(), 'path must be relative.'
    try:
        loop = current_loop()
        await loop.run_in_executor(None, lambda: (folder_path / path).mkdir(parents=True, exist_ok=True))
    except FileExistsError as e:
        raise InvalidAPIParameters(
            f'"{e.filename}" already exists and is not a directory.')
    return web.Response(status=201)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_WRITE)
async def upload(request: web.Request, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log_fmt = 'VFOLDER.UPLOAD (ak:{}, vf:{})'
    log_args = (access_key, folder_name)
    log.info(log_fmt, *log_args)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    reader = await request.multipart()
    file_count = 0
    async for file in aiotools.aiter(reader.next, None):
        if file_count == 10:  # TODO: make it configurable
            raise InvalidAPIParameters('Too many files!')
        file_count += 1
        try:
            file_path = (folder_path / file.filename).resolve()
            file_path.relative_to(folder_path)
        except ValueError:
            raise InvalidAPIParameters('One of target paths is out of the folder.')
        if file_path.exists() and not file_path.is_file():
            raise InvalidAPIParameters(
                f'Cannot overwrite "{file.filename}" because '
                'it already exists and not a regular file.')
        file_dir = (folder_path / file.filename).parent
        try:
            loop = current_loop()
            await loop.run_in_executor(None, lambda: file_dir.mkdir(parents=True, exist_ok=True))
        except FileExistsError as e:
            raise InvalidAPIParameters(
                'Failed to create parent directories. '
                f'"{e.filename}" already exists and is not a directory.')
        log.info(log_fmt + ': accepted path:{}',
                 *log_args, file.filename)

        q: 'janus.Queue[Union[bytes, _Sentinel]]' = janus.Queue(maxsize=DEFAULT_INFLIGHT_CHUNKS)

        def _write():
            with open(file_path, 'wb') as f:
                while True:
                    chunk = q.sync_q.get()
                    if chunk is eof_sentinel:
                        break
                    f.write(file.decode(chunk))
                    q.sync_q.task_done()

        loop = current_loop()
        try:
            fut = loop.run_in_executor(None, _write)
            while not file.at_eof():
                chunk = await file.read_chunk(size=DEFAULT_CHUNK_SIZE)
                await q.async_q.put(chunk)
            await q.async_q.put(eof_sentinel)
            await fut
        finally:
            q.close()
            await q.wait_closed()

    return web.Response(status=201)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.RW_DELETE)
@check_api_params(
    t.Dict({
        t.Key('path'): t.String,
        t.Key('size'): t.Int,
    }))
async def create_tus_upload_session(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    secret = request.app['config']['manager']['secret']
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    session_id = uuid.uuid4().hex

    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log_fmt = 'VFOLDER.UPLOAD_SESSION (ak:{}, vf:{}, si:{})'
    log_args = (access_key, folder_name, session_id)
    log.info(log_fmt, *log_args)

    upload_base = folder_path / ".upload"
    if not upload_base.exists():
        upload_base.mkdir()
    upload_base_path = folder_path / ".upload" / session_id
    Path(upload_base_path).touch()

    t = params
    t['host'] = row['host']
    t['folder'] = row['id'].hex
    t['session_id'] = session_id
    t['exp'] = datetime.utcnow() + timedelta(days=1)  # TODO: make it configurable

    token = jwt.encode(t, secret, algorithm='HS256').decode('UTF-8')
    resp = {
        'token': token,
    }

    return web.json_response(resp, status=200)


async def tus_check_session(request):
    try:
        secret = request.app['config']['manager']['secret']
        token = request.match_info['session']
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters('Could not validate the upload session token.')

    headers = await tus_session_headers(request, params)
    return web.Response(headers=headers)


async def tus_upload_part(request):
    try:
        secret = request.app['config']['manager']['secret']
        token = request.match_info['session']
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters('Could not validate the upload session token.')

    headers = await tus_session_headers(request, params)

    folder_path = (request.app['VFOLDER_MOUNT'] / params['host'] /
                   request.app['VFOLDER_FSPREFIX'] / params['folder'])
    upload_base = folder_path / ".upload"
    target_filename = upload_base / params['session_id']

    q: 'janus.Queue[Union[bytes, _Sentinel]]' = janus.Queue(maxsize=DEFAULT_INFLIGHT_CHUNKS)

    def _write():
        with open(target_filename, 'ab') as f:
            while True:
                chunk = q.sync_q.get()
                if chunk is eof_sentinel:
                    break
                f.write(chunk)
                q.sync_q.task_done()

    loop = current_loop()
    try:
        fut = loop.run_in_executor(None, _write)
        while not request.content.at_eof():
            chunk = await request.content.read(DEFAULT_CHUNK_SIZE)
            await q.async_q.put(chunk)
        await q.async_q.put(eof_sentinel)
        await fut
    finally:
        q.close()
        await q.wait_closed()

    fs = Path(target_filename).stat().st_size
    if fs >= params['size']:
        target_path = folder_path / params['path']
        Path(target_filename).rename(target_path)
        try:
            await loop.run_in_executor(None, lambda: upload_base.rmdir())
        except OSError:
            pass

    headers['Upload-Offset'] = str(fs)
    return web.Response(status=204, headers=headers)


async def tus_options(request):
    headers = {}
    headers["Access-Control-Allow-Origin"] = "*"
    headers["Access-Control-Allow-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Expose-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Allow-Methods"] = "*"
    headers["Tus-Resumable"] = "1.0.0"
    headers["Tus-Version"] = "1.0.0"
    headers["Tus-Max-Size"] = "107374182400"  # 100G TODO: move to settings
    headers["X-Content-Type-Options"] = "nosniff"
    return web.Response(headers=headers)


async def tus_session_headers(request, params):
    folder_path = (request.app['VFOLDER_MOUNT'] / params['host'] /
                   request.app['VFOLDER_FSPREFIX'] / params['folder'])
    upload_base = folder_path / ".upload"
    base_file = upload_base / params['session_id']
    if not Path(base_file).exists():
        raise web.HTTPNotFound()
    headers = {}
    headers["Access-Control-Allow-Origin"] = "*"
    headers["Access-Control-Allow-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Expose-Headers"] = \
        "Tus-Resumable, Upload-Length, Upload-Metadata, Upload-Offset, Content-Type"
    headers["Access-Control-Allow-Methods"] = "*"
    headers["Cache-Control"] = "no-store"
    headers["Tus-Resumable"] = "1.0.0"
    headers['Upload-Offset'] = str(Path(base_file).stat().st_size)
    headers['Upload-Length'] = str(params['size'])
    return headers


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.RW_DELETE)
@check_api_params(
    t.Dict({
        t.Key('target_path'): t.String,
        t.Key('new_name'): t.String,
    }))
async def rename_file(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.RENAME_FILE (ak:{}, vf:{}, target_path:{}, new_name:{})',
             access_key, folder_name, params['target_path'], params['new_name'])
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    ops = []
    try:
        target_path = (folder_path / params['target_path']).resolve(strict=True)
        target_path.relative_to(folder_path)
        new_path = target_path.parent / params['new_name']
        # Ensure new file is in the same directory.
        if len(params['new_name'].split('/')) > 1:
            raise InvalidAPIParameters('New name should not be a path: ' + params['new_name'])
        if new_path.exists():
            raise InvalidAPIParameters('File already exists: ' + params['new_name'])
    except FileNotFoundError:
        raise InvalidAPIParameters('No such target file: ' + params['target_path'])
    except ValueError:
        raise InvalidAPIParameters('The requested path is out of the folder')
    ops.append(functools.partial(target_path.rename, new_path))

    def _do_ops():
        for op in ops:
            op()

    loop = current_loop()
    await loop.run_in_executor(None, _do_ops)
    resp: Dict[str, Any] = {}
    return web.json_response(resp, status=200)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_WRITE)
@check_api_params(
    t.Dict({
        t.Key('files'): t.List[t.String],
        t.Key('recursive', default=False): t.Bool | t.StrBool,
    }))
async def delete_files(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    recursive = params['recursive']
    log.info('VFOLDER.DELETE_FILES (ak:{}, vf:{}, path:{}, recursive:{})',
             access_key, folder_name, folder_name, recursive)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    ops = []
    for file in params['files']:
        try:
            file_path = (folder_path / file).resolve()
            file_path.relative_to(folder_path)
        except ValueError:
            # path is out of folder
            continue
        if file_path.is_dir():
            if recursive:
                ops.append(functools.partial(shutil.rmtree, file_path))
            else:
                raise InvalidAPIParameters(
                    f'"{file_path}" is a directory. '
                    'Set recursive option to remove it.')
        elif file_path.is_file():
            ops.append(functools.partial(os.unlink, file_path))

    def _do_ops():
        for op in ops:
            op()

    loop = current_loop()
    await loop.run_in_executor(None, _do_ops)
    resp: Dict[str, Any] = {}
    return web.json_response(resp, status=200)


async def download_directory_as_archive(request: web.Request,
                                        file_path: Path,
                                        zip_filename: str = None) -> web.StreamResponse:
    """Serve a directory as a zip archive on the fly."""
    def _iter2aiter(iter):
        """Iterable to async iterable"""
        def _consume(loop, iter, q):
            for item in iter:
                q.put(item)
            q.put(eof_sentinel)

        async def _aiter():
            loop = current_loop()
            q: 'janus.Queue[Union[bytes, _Sentinel]]' = janus.Queue(maxsize=DEFAULT_INFLIGHT_CHUNKS)
            try:
                fut = loop.run_in_executor(None, lambda: _consume(loop, iter, q.sync_q))
                while True:
                    item = await q.async_q.get()
                    if item is eof_sentinel:
                        break
                    yield item
                    q.async_q.task_done()
                await fut
            finally:
                q.close()
                await q.wait_closed()

        return _aiter()

    if zip_filename is None:
        zip_filename = file_path.name + '.zip'
    zf = zipstream.ZipFile(compression=zipstream.ZIP_DEFLATED)
    async for root, dirs, files in _iter2aiter(os.walk(file_path)):
        for file in files:
            zf.write(Path(root) / file, Path(root).relative_to(file_path) / file)
        if len(dirs) == 0 and len(files) == 0:
            # Include an empty directory in the archive as well.
            zf.write(root, Path(root).relative_to(file_path))
    ascii_filename = zip_filename.encode('ascii', errors='ignore').decode('ascii').replace('"', r'\"')
    encoded_filename = urllib.parse.quote(zip_filename, encoding='utf-8')
    response = web.StreamResponse(headers={
        hdrs.CONTENT_TYPE: 'application/zip',
        hdrs.CONTENT_DISPOSITION: " ".join([
            "attachment;"
            f"filename=\"{ascii_filename}\";",       # RFC-2616 sec2.2
            f"filename*=UTF-8''{encoded_filename}",  # RFC-5987
        ])
    })
    await response.prepare(request)
    async for chunk in _iter2aiter(zf):
        await response.write(chunk)
    return response


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
@check_api_params(
    t.Dict({
        t.Key('files'): t.List[t.String],
    }))
async def download(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    files = params['files']
    log.info('VFOLDER.DOWNLOAD (ak:{}, vf:{}, path:{})', access_key, folder_name, files[0])
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    for file in files:
        try:
            file_path = (folder_path / file).resolve()
            file_path.relative_to(folder_path)
        except ValueError:
            raise InvalidAPIParameters('The requested path is out of the folder')
        if not file_path.is_file():
            raise InvalidAPIParameters(
                f'You cannot download "{file}" because it is not a regular file.')
    with aiohttp.MultipartWriter('mixed') as mpwriter:
        total_payloads_length = 0
        headers = multidict.MultiDict({'Content-Encoding': 'identity'})
        try:
            for file in files:
                data = open(folder_path / file, 'rb')
                payload = mpwriter.append(data, headers)
                if payload.size is not None:
                    total_payloads_length += payload.size
        except FileNotFoundError:
            return web.Response(status=404, reason='File not found')
        mpwriter._headers['X-TOTAL-PAYLOADS-LENGTH'] = str(total_payloads_length)
        return web.Response(body=mpwriter, status=200)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
@check_api_params(
    t.Dict({
        t.Key('file'): t.String,
        t.Key('archive', default=False): t.Bool | t.Null,
    }))
async def download_single(request: web.Request, params: Any, row: VFolderRow) -> web.StreamResponse:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    fn = params['file']
    log.info('VFOLDER.DOWNLOAD_SINGLE (ak:{}, vf:{}, path:{})', access_key, folder_name, fn)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    try:
        file_path = (folder_path / fn).resolve()
        file_path.relative_to(folder_path)
        if not file_path.exists():
            raise FileNotFoundError
    except (ValueError, FileNotFoundError):
        raise GenericNotFound('The file is not found.')
    if not file_path.is_file():
        if params['archive']:
            # Download directory as an archive when archive param is set.
            return await download_directory_as_archive(request, file_path)
        else:
            raise InvalidAPIParameters('The file is not a regular file.')
    if request.method == 'HEAD':
        return web.Response(status=200, headers={
            hdrs.ACCEPT_RANGES: 'bytes',
            hdrs.CONTENT_LENGTH: str(file_path.stat().st_size),
        })
    ascii_filename = file_path.name.encode('ascii', errors='ignore').decode('ascii').replace('"', r'\"')
    encoded_filename = urllib.parse.quote(file_path.name, encoding='utf-8')
    return web.FileResponse(file_path, headers={
        hdrs.CONTENT_TYPE: "application/octet-stream",
        hdrs.CONTENT_DISPOSITION: " ".join([
            "attachment;"
            f"filename=\"{ascii_filename}\";",       # RFC-2616 sec2.2
            f"filename*=UTF-8''{encoded_filename}",  # RFC-5987
        ])
    })


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
@check_api_params(
    t.Dict({
        t.Key('file'): t.String,
        t.Key('archive', default=False): t.Bool | t.Null,
    }))
async def request_download(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    secret = request.app['config']['manager']['secret']
    p = {}
    p['file'] = params['file']
    p['host'] = row['host']
    p['id'] = row['id'].hex
    p['archive'] = params['archive']
    p['exp'] = datetime.utcnow() + timedelta(minutes=2)  # TODO: make it configurable
    token = jwt.encode(p, secret, algorithm='HS256').decode('UTF-8')
    resp = {
        'token': token,
    }
    log.info('VFOLDER.REQUEST_DOWNLOAD_TOKEN (ak:{}, vf:{}, path:{}): generated token:{}',
             request['keypair']['access_key'], row['name'], params['file'], token)
    return web.json_response(resp, status=200)


async def download_with_token(request) -> web.StreamResponse:
    try:
        secret = request.app['config']['manager']['secret']
        token = request.query.get('token', '')
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters('Could not validate the download token.')

    assert params.get('file'), 'no file(s) specified!'
    fn = params.get('file')
    log.info('VFOLDER.DOWNLOAD_WITH_TOKEN (token:{}, path:{})', token, fn)
    folder_path = (request.app['VFOLDER_MOUNT'] / params['host'] /
                   request.app['VFOLDER_FSPREFIX'] / params['id'])
    try:
        file_path = (folder_path / fn).resolve()
        file_path.relative_to(folder_path)
        if not file_path.exists():
            raise FileNotFoundError
    except (ValueError, FileNotFoundError):
        raise InvalidAPIParameters('The file is not found.')
    if not file_path.is_file():
        if params['archive']:
            # Download directory as an archive when archive param is set.
            return await download_directory_as_archive(request, file_path)
        else:
            raise InvalidAPIParameters('The file is not a regular file.')
    if request.method == 'HEAD':
        return web.Response(status=200, headers={
            hdrs.ACCEPT_RANGES: 'bytes',
            hdrs.CONTENT_LENGTH: str(file_path.stat().st_size),
        })
    ascii_filename = file_path.name.encode('ascii', errors='ignore').decode('ascii').replace('"', r'\"')
    encoded_filename = urllib.parse.quote(file_path.name, encoding='utf-8')
    return web.FileResponse(file_path, headers={
        hdrs.CONTENT_TYPE: "application/octet-stream",
        hdrs.CONTENT_DISPOSITION: " ".join([
            "attachment;"
            f"filename=\"{ascii_filename}\";",       # RFC-2616 sec2.2
            f"filename*=UTF-8''{encoded_filename}",  # RFC-5987
        ])
    })


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
@check_api_params(
    t.Dict({
        t.Key('path', default=''): t.String(allow_blank=True),
    }))
async def list_files(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_FILES (ak:{}, vf:{}, path:{})',
             access_key, folder_name, params['path'])
    base_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                 request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    try:
        folder_path = (base_path / params['path']).resolve()
        folder_path.relative_to(base_path)
    except ValueError:
        raise VFolderNotFound('No such file or directory.')
    if not folder_path.exists():
        raise VFolderNotFound('No such file or directory.')
    if not folder_path.is_dir():
        raise InvalidAPIParameters('The target path must be a directory.')
    files = []

    def _scan():
        for f in os.scandir(folder_path):
            fstat = f.stat()
            ctime = fstat.st_ctime  # TODO: way to get concrete create time?
            mtime = fstat.st_mtime
            atime = fstat.st_atime
            files.append({
                'mode': stat.filemode(fstat.st_mode),
                'size': fstat.st_size,
                'ctime': ctime,
                'mtime': mtime,
                'atime': atime,
                'filename': f.name,
            })

    loop = current_loop()
    await loop.run_in_executor(None, _scan)
    resp = {
        'files': json.dumps(files),
    }
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def list_sent_invitations(request: web.Request) -> web.Response:
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_SENT_INVITATIONS (ak:{})', access_key)
    async with dbpool.acquire() as conn:
        j = sa.join(vfolders, vfolder_invitations,
                    vfolders.c.id == vfolder_invitations.c.vfolder)
        query = (sa.select([vfolder_invitations, vfolders.c.name])
                   .select_from(j)
                   .where((vfolder_invitations.c.inviter == request['user']['email']) &
                          (vfolder_invitations.c.state == VFolderInvitationState.PENDING)))
        result = await conn.execute(query)
        invitations = await result.fetchall()
    invs_info = []
    for inv in invitations:
        invs_info.append({
            'id': str(inv.id),
            'inviter': inv.inviter,
            'invitee': inv.invitee,
            'perm': inv.permission,
            'state': inv.state.value,
            'created_at': str(inv.created_at),
            'vfolder_id': str(inv.vfolder),
            'vfolder_name': inv.name,
        })
    resp = {'invitations': invs_info}
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        tx.AliasedKey(['perm', 'permission']): VFolderPermissionValidator,
    })
)
async def update_invitation(request: web.Request, params: Any) -> web.Response:
    '''
    Update sent invitation's permission. Other fields are not allowed to be updated.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    inv_id = request.match_info['inv_id']
    perm = params['perm']
    log.info('VFOLDER.UPDATE_INVITATION (ak:{}, inv:{})', access_key, inv_id)
    async with dbpool.acquire() as conn:
        query = (sa.update(vfolder_invitations)
                   .values(permission=VFolderPermission(perm))
                   .where(vfolder_invitations.c.id == inv_id)
                   .where(vfolder_invitations.c.inviter == request['user']['email'])
                   .where(vfolder_invitations.c.state == VFolderInvitationState.PENDING))
        await conn.execute(query)
    resp = {'msg': f'vfolder invitation updated: {inv_id}.'}
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
async def invite(request: web.Request) -> web.Response:
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    user_uuid = request['user']['uuid']
    params = await request.json()
    perm = params.get('perm', VFolderPermission.READ_WRITE.value)
    perm = VFolderPermission(perm)
    user_ids = params.get('user_ids', [])
    assert len(user_ids) > 0, 'no user ids'
    log.info('VFOLDER.INVITE (ak:{}, vf:{}, inv.users:{})',
             access_key, folder_name, ','.join(user_ids))
    if folder_name.startswith('.'):
        raise GenericForbidden('Cannot share private dot-prefixed vfolders.')
    async with dbpool.acquire() as conn:
        # Get virtual folder.
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where((vfolders.c.user == user_uuid) &
                          (vfolders.c.name == folder_name)))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        vf = await result.first()
        if vf is None:
            raise VFolderNotFound()

        # Get invited user's keypairs except vfolder owner.
        query = (sa.select('*')
                   .select_from(keypairs)
                   .where(keypairs.c.user_id.in_(user_ids))
                   .where(keypairs.c.user_id != request['user']['id']))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        kps = await result.fetchall()

        # Create invitation.
        invitees = [kp.user_id for kp in kps]
        invited_ids = []
        for invitee in set(invitees):
            inviter = request['user']['id']
            # Do not create invitation if already exists.
            query = (sa.select('*')
                       .select_from(vfolder_invitations)
                       .where((vfolder_invitations.c.inviter == inviter) &
                              (vfolder_invitations.c.invitee == invitee) &
                              (vfolder_invitations.c.vfolder == vf.id) &
                              (vfolder_invitations.c.state == VFolderInvitationState.PENDING)))
            result = await conn.execute(query)
            if result.rowcount > 0:
                continue

            # TODO: insert multiple values with one query.
            #       insert().values([{}, {}, ...]) does not work:
            #       sqlalchemy.exc.CompileError: The 'default' dialect with current
            #       database version settings does not support in-place multirow
            #       inserts.
            query = (vfolder_invitations.insert().values({
                'id': uuid.uuid4().hex,
                'permission': perm,
                'vfolder': vf.id,
                'inviter': inviter,
                'invitee': invitee,
                'state': VFolderInvitationState.PENDING,
            }))
            try:
                await conn.execute(query)
                invited_ids.append(invitee)
            except psycopg2.DataError:
                pass
    resp = {'invited_ids': invited_ids}
    return web.json_response(resp, status=201)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def invitations(request: web.Request) -> web.Response:
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.INVITATIONS (ak:{})', access_key)
    async with dbpool.acquire() as conn:
        j = sa.join(vfolders, vfolder_invitations,
                    vfolders.c.id == vfolder_invitations.c.vfolder)
        query = (sa.select([vfolder_invitations, vfolders.c.name])
                   .select_from(j)
                   .where((vfolder_invitations.c.invitee == request['user']['id']) &
                          (vfolder_invitations.c.state == VFolderInvitationState.PENDING)))
        result = await conn.execute(query)
        invitations = await result.fetchall()
    invs_info = []
    for inv in invitations:
        invs_info.append({
            'id': str(inv.id),
            'inviter': inv.inviter,
            'invitee': inv.invitee,
            'perm': inv.permission,
            'state': inv.state,
            'created_at': str(inv.created_at),
            'vfolder_id': str(inv.vfolder),
            'vfolder_name': inv.name,
        })
    resp = {'invitations': invs_info}
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
@check_api_params(t.Dict({t.Key('inv_id'): t.String}))
async def accept_invitation(request: web.Request, params: Any) -> web.Response:
    '''Accept invitation by invitee.

    * `inv_ak` parameter is removed from 19.06 since virtual folder's ownership is
    moved from keypair to a user or a group.

    :params inv_id: ID of vfolder_invitations row.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    user_uuid = request['user']['uuid']
    inv_id = params['inv_id']
    log.info('VFOLDER.ACCEPT_INVITATION (ak:{}, inv:{})', access_key, inv_id)
    async with dbpool.acquire() as conn:
        # Get invitation.
        query = (sa.select([vfolder_invitations])
                   .select_from(vfolder_invitations)
                   .where((vfolder_invitations.c.id == inv_id) &
                          (vfolder_invitations.c.state == VFolderInvitationState.PENDING)))
        result = await conn.execute(query)
        invitation = await result.first()
        if invitation is None:
            raise GenericNotFound('No such vfolder invitation')

        # Get target virtual folder.
        query = (sa.select([vfolders.c.name])
                   .select_from(vfolders)
                   .where(vfolders.c.id == invitation.vfolder))
        result = await conn.execute(query)
        target_vfolder = await result.first()
        if target_vfolder is None:
            raise VFolderNotFound

        # Prevent accepting vfolder with duplicated name.
        j = sa.join(vfolders, vfolder_permissions,
                    vfolders.c.id == vfolder_permissions.c.vfolder, isouter=True)
        query = (sa.select([vfolders.c.id])
                   .select_from(j)
                   .where(((vfolders.c.user == user_uuid) |
                           (vfolder_permissions.c.user == user_uuid)) &
                          (vfolders.c.name == target_vfolder.name)))
        result = await conn.execute(query)
        if result.rowcount > 0:
            raise VFolderAlreadyExists

        # Create permission relation between the vfolder and the invitee.
        query = (vfolder_permissions.insert().values({
            'permission': VFolderPermission(invitation.permission),
            'vfolder': invitation.vfolder,
            'user': user_uuid,
        }))
        await conn.execute(query)

        # Clear used invitation.
        query = (vfolder_invitations.update()
                                    .where(vfolder_invitations.c.id == inv_id)
                                    .values(state=VFolderInvitationState.ACCEPTED))
        await conn.execute(query)
    return web.json_response({})


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('inv_id'): t.String,
    }))
async def delete_invitation(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    request_email = request['user']['email']
    inv_id = params['inv_id']
    log.info('VFOLDER.DELETE_INVITATION (ak:{}, inv:{})', access_key, inv_id)
    try:
        async with dbpool.acquire() as conn:
            query = (sa.select([vfolder_invitations.c.inviter,
                                vfolder_invitations.c.invitee])
                       .select_from(vfolder_invitations)
                       .where((vfolder_invitations.c.id == inv_id) &
                              (vfolder_invitations.c.state == VFolderInvitationState.PENDING)))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise GenericNotFound('No such vfolder invitation')
            if request_email == row.inviter:
                state = VFolderInvitationState.CANCELED
            elif request_email == row.invitee:
                state = VFolderInvitationState.REJECTED
            else:
                raise GenericForbidden('Cannot change other user\'s invitaiton')
            query = (vfolder_invitations
                     .update()
                     .where(vfolder_invitations.c.id == inv_id)
                     .values(state=state))
            await conn.execute(query)
    except (psycopg2.IntegrityError, sa.exc.IntegrityError) as e:
        raise InternalServerError(f'integrity error: {e}')
    except (asyncio.CancelledError, asyncio.TimeoutError):
        raise
    except Exception as e:
        raise InternalServerError(f'unexpected error: {e}')
    return web.json_response({})


@auth_required
@server_status_required(ALL_ALLOWED)
async def delete(request: web.Request) -> web.Response:
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    log.info('VFOLDER.DELETE (ak:{}, vf:{})', access_key, folder_name)
    async with dbpool.acquire() as conn, conn.begin():
        entries = await query_accessible_vfolders(
            conn, user_uuid,
            user_role=user_role, domain_name=domain_name,
            allowed_vfolder_types=allowed_vfolder_types)
        for entry in entries:
            if entry['name'] == folder_name:
                if not entry['is_owner']:
                    raise InvalidAPIParameters(
                        'Cannot delete the vfolder '
                        'that is not owned by me.')
                break
        else:
            raise InvalidAPIParameters('No such vfolder.')
        folder_host = entry['host']
        folder_id = entry['id']
        folder_hex = folder_id.hex
        folder_path = (request.app['VFOLDER_MOUNT'] / folder_host /
                       request.app['VFOLDER_FSPREFIX'] / folder_hex)
        query = (vfolders.delete().where(vfolders.c.id == folder_id))
        await conn.execute(query)
        try:
            loop = current_loop()
            await loop.run_in_executor(None, lambda: shutil.rmtree(folder_path))  # type: ignore
        except IOError:
            pass
    return web.Response(status=204)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def list_shared_vfolders(request: web.Request) -> web.Response:
    '''
    List shared vfolders.

    Not available for group vfolders.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    params = await request.json() if request.can_read_body else request.query
    target_vfid = params.get('vfolder_id', None)
    log.info('VFOLDER.LIST_SHARED_VFOLDERS (ak:{})', access_key)
    async with dbpool.acquire() as conn:
        j = (vfolder_permissions
             .join(vfolders, vfolders.c.id == vfolder_permissions.c.vfolder)
             .join(users, users.c.uuid == vfolder_permissions.c.user))
        query = (sa.select([vfolder_permissions,
                            vfolders.c.id, vfolders.c.name,
                            users.c.email])
                   .select_from(j)
                   .where((vfolders.c.user == request['user']['uuid'])))
        if target_vfid is not None:
            query = query.where(vfolders.c.id == target_vfid)
        result = await conn.execute(query)
        shared_list = await result.fetchall()
    shared_info = []
    for shared in shared_list:
        shared_info.append({
            'vfolder_id': str(shared.id),
            'vfolder_name': str(shared.name),
            'shared_by': request['user']['email'],
            'shared_to': {
                'uuid': str(shared.user),
                'email': shared.email,
            },
            'perm': shared.permission.value,
        })
    resp = {'shared': shared_info}
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('vfolder'): tx.UUID,
        t.Key('user'): tx.UUID,
        tx.AliasedKey(['perm', 'permission']): VFolderPermissionValidator | t.Null,
    })
)
async def update_shared_vfolder(request: web.Request, params: Any) -> web.Response:
    '''
    Update permission for shared vfolders.

    If params['perm'] is None, remove user's permission for the vfolder.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    vfolder_id = params['vfolder']
    user_uuid = params['user']
    perm = params['perm']
    log.info('VFOLDER.UPDATE_SHARED_VFOLDER(ak:{}, vfid:{}, uid:{}, perm:{})',
             access_key, vfolder_id, user_uuid, perm)
    async with dbpool.acquire() as conn:
        if perm is not None:
            query = (
                sa.update(vfolder_permissions)
                .values(permission=VFolderPermission(perm))
                .where(vfolder_permissions.c.vfolder == vfolder_id)
                .where(vfolder_permissions.c.user == user_uuid)
            )
        else:
            query = (
                vfolder_permissions
                .delete()
                .where(vfolder_permissions.c.vfolder == vfolder_id)
                .where(vfolder_permissions.c.user == user_uuid)
            )
        await conn.execute(query)
    resp = {'msg': 'shared vfolder permission updated'}
    return web.json_response(resp, status=200)


@superadmin_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('fstab_path', default=None): t.String | t.Null,
        t.Key('agent_id', default=None): t.String | t.Null,
    }),
)
async def get_fstab_contents(request: web.Request, params: Any) -> web.Response:
    '''
    Return the contents of `/etc/fstab` file.
    '''
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.GET_FSTAB_CONTENTS(ak:{}, ag:{})', access_key, params['agent_id'])
    if params['fstab_path'] is None:
        params['fstab_path'] = '/etc/fstab'
    if params['agent_id'] is not None:
        # Return specific agent's fstab.
        watcher_info = await get_watcher_info(request, params['agent_id'])
        try:
            client_timeout = aiohttp.ClientTimeout(total=10.0)
            async with aiohttp.ClientSession(timeout=client_timeout) as sess:
                headers = {'X-BackendAI-Watcher-Token': watcher_info['token']}
                url = watcher_info['addr'] / 'fstab'
                async with sess.get(url, headers=headers, params=params) as watcher_resp:
                    if watcher_resp.status == 200:
                        content = await watcher_resp.text()
                        resp = {
                            'content': content,
                            'node': 'agent',
                            'node_id': params['agent_id'],
                        }
                        return web.json_response(resp)
                    else:
                        message = await watcher_resp.text()
                        raise BackendAgentError(
                            'FAILURE', f'({watcher_resp.status}: {watcher_resp.reason}) {message}')
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.error('VFOLDER.GET_FSTAB_CONTENTS(u:{}): timeout from watcher (agent:{})',
                      access_key, params['agent_id'])
            raise BackendAgentError('TIMEOUT', 'Could not fetch fstab data from agent')
        except Exception:
            log.exception('VFOLDER.GET_FSTAB_CONTENTS(u:{}): '
                          'unexpected error while reading from watcher (agent:{})',
                          access_key, params['agent_id'])
            raise InternalServerError
    else:
        # Return manager's fstab.
        async with aiofiles.open(params['fstab_path'], mode='r') as fp:
            content = await fp.read()
            resp = {
                'content': content,
                'node': 'manager',
                'node_id': 'manager',
            }
            return web.json_response(resp)


@superadmin_required
@server_status_required(READ_ALLOWED)
async def list_mounts(request: web.Request) -> web.Response:
    '''
    List all mounted vfolder hosts in vfroot.

    All mounted hosts from connected (ALIVE) agents are also gathered.
    Generally, agents should be configured to have same hosts structure,
    but newly introduced one may not.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_MOUNTS(ak:{})', access_key)
    config = request.app['config_server']
    mount_prefix = await config.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'

    # Scan mounted vfolder hosts in manager machine.
    mounts = set()
    for p in Path(mount_prefix).iterdir():
        # TODO: Change os.path.ismount to p.is_mount if Python 3.7 is supported.
        if p.is_dir() and os.path.ismount(str(p)):
            mounts.add(str(p))
    resp: MutableMapping[str, Any] = {
        'manager': {
            'success': True,
            'mounts': sorted(mounts),
            'message': '',
        },
        'agents': {},
    }

    # Scan mounted vfolder hosts for connected agents.
    async def _fetch_mounts(sess: aiohttp.ClientSession, agent_id: str) -> Tuple[str, Mapping]:
        watcher_info = await get_watcher_info(request, agent_id)
        headers = {'X-BackendAI-Watcher-Token': watcher_info['token']}
        url = watcher_info['addr'] / 'mounts'
        try:
            async with sess.get(url, headers=headers) as watcher_resp:
                if watcher_resp.status == 200:
                    data = {
                        'success': True,
                        'mounts': await watcher_resp.json(),
                        'message': '',
                    }
                else:
                    data = {
                        'success': False,
                        'mounts': [],
                        'message': await watcher_resp.text(),
                    }
                return (agent_id, data,)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.error('VFOLDER.LIST_MOUNTS(u:{}): timeout from watcher (agent:{})',
                      access_key, agent_id)
            raise
        except Exception:
            log.exception('VFOLDER.LIST_MOUNTS(u:{}): '
                          'unexpected error while reading from watcher (agent:{})',
                          access_key, agent_id)
            raise

    async with dbpool.acquire() as conn:
        query = (sa.select([agents.c.id])
                   .select_from(agents)
                   .where(agents.c.status == AgentStatus.ALIVE))
        result = await conn.execute(query)
        rows = await result.fetchall()

    client_timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(timeout=client_timeout) as sess:
        scheduler = await aiojobs.create_scheduler(limit=8)
        try:
            jobs = await asyncio.gather(*[
                scheduler.spawn(_fetch_mounts(sess, row.id)) for row in rows
            ])
            mounts = await asyncio.gather(*[job.wait() for job in jobs],
                                          return_exceptions=True)
            for mount in mounts:
                if isinstance(mount, Exception):
                    # exceptions are already logged.
                    continue
                resp['agents'][mount[0]] = mount[1]
        finally:
            await scheduler.close()

    return web.json_response(resp, status=200)


@superadmin_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('fs_location'): t.String,
        t.Key('name'): t.String,
        t.Key('fs_type', default='nfs'): t.String,
        t.Key('options', default=None): t.String | t.Null,
        t.Key('scaling_group', default=None): t.String | t.Null,
        t.Key('fstab_path', default=None): t.String | t.Null,
        t.Key('edit_fstab', default=False): t.Bool | t.StrBool,
    }),
)
async def mount_host(request: web.Request, params: Any) -> web.Response:
    '''
    Mount device into vfolder host.

    Mount a device (eg: nfs) located at `fs_location` into `<vfroot>/name` in the
    host machines (manager and all agents). `fs_type` can be specified by requester,
    which fallbaks to 'nfs'.

    If `scaling_group` is specified, try to mount for agents in the scaling group.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log_fmt = 'VFOLDER.MOUNT_HOST(ak:{}, name:{}, fs:{}, sg:{})'
    log_args = (access_key, params['name'], params['fs_location'], params['scaling_group'])
    log.info(log_fmt, *log_args)
    config = request.app['config_server']
    mount_prefix = await config.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'

    # Mount on manager.
    mountpoint = Path(mount_prefix) / params['name']
    mountpoint.mkdir(exist_ok=True)
    if params.get('options', None):
        cmd = ['sudo', 'mount', '-t', params['fs_type'], '-o', params['options'],
               params['fs_location'], str(mountpoint)]
    else:
        cmd = ['sudo', 'mount', '-t', params['fs_type'],
               params['fs_location'], str(mountpoint)]
    proc = await asyncio.create_subprocess_exec(*cmd,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    raw_out, raw_err = await proc.communicate()
    out = raw_out.decode('utf8')
    err = raw_err.decode('utf8')
    await proc.wait()
    resp: MutableMapping[str, Any] = {
        'manager': {
            'success': True if not err else False,
            'message': out if not err else err,
        },
        'agents': {},
    }
    if params['edit_fstab'] and resp['manager']['success']:
        fstab_path = params['fstab_path'] if params['fstab_path'] else '/etc/fstab'
        async with aiofiles.open(fstab_path, mode='r+') as fp:
            fstab = Fstab(fp)
            await fstab.add(params['fs_location'], str(mountpoint),
                            params['fs_type'], params['options'])

    # Mount on running agents.
    async with dbpool.acquire() as conn:
        query = (sa.select([agents.c.id])
                   .select_from(agents)
                   .where(agents.c.status == AgentStatus.ALIVE))
        if params['scaling_group'] is not None:
            query = query.where(agents.c.scaling == params['scaling_group'])
        result = await conn.execute(query)
        rows = await result.fetchall()

    async def _mount(sess: aiohttp.ClientSession, agent_id: str) -> Tuple[str, Mapping]:
        watcher_info = await get_watcher_info(request, agent_id)
        try:
            headers = {'X-BackendAI-Watcher-Token': watcher_info['token']}
            url = watcher_info['addr'] / 'mounts'
            async with sess.post(url, json=params, headers=headers) as resp:
                if resp.status == 200:
                    data = {
                        'success': True,
                        'message': await resp.text(),
                    }
                else:
                    data = {
                        'success': False,
                        'message': await resp.text(),
                    }
                return (agent_id, data,)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.error(log_fmt + ': timeout from watcher (ag:{})',
                      *log_args, agent_id)
            raise
        except Exception:
            log.exception(log_fmt + ': unexpected error while reading from watcher (ag:{})',
                          *log_args, agent_id)
            raise

    client_timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=client_timeout) as sess:
        scheduler = await aiojobs.create_scheduler(limit=8)
        try:
            jobs = await asyncio.gather(*[
                scheduler.spawn(_mount(sess, row.id)) for row in rows
            ])
            results = await asyncio.gather(*[job.wait() for job in jobs],
                                           return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    # exceptions are already logged.
                    continue
                resp['agents'][result[0]] = result[1]
        finally:
            await scheduler.close()

    return web.json_response(resp, status=200)


@superadmin_required
@server_status_required(ALL_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('name'): t.String,
        t.Key('scaling_group', default=None): t.String | t.Null,
        t.Key('fstab_path', default=None): t.String | t.Null,
        t.Key('edit_fstab', default=False): t.Bool | t.StrBool,
    }),
)
async def umount_host(request: web.Request, params: Any) -> web.Response:
    '''
    Unmount device from vfolder host.

    Unmount a device (eg: nfs) located at `<vfroot>/name` from the host machines
    (manager and all agents).

    If `scaling_group` is specified, try to unmount for agents in the scaling group.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log_fmt = 'VFOLDER.UMOUNT_HOST(ak:{}, name:{}, sg:{})'
    log_args = (access_key, params['name'], params['scaling_group'])
    log.info(log_fmt, *log_args)
    config = request.app['config_server']
    mount_prefix = await config.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'
    mountpoint = Path(mount_prefix) / params['name']
    assert Path(mount_prefix) != mountpoint

    async with dbpool.acquire() as conn, conn.begin():
        # Prevent unmount if target host is mounted to running kernels.
        query = (sa.select([kernels.c.mounts])
                   .select_from(kernels)
                   .where(kernels.c.status != KernelStatus.TERMINATED))
        result = await conn.execute(query)
        _kernels = await result.fetchall()
        _mounted = set()
        for kern in _kernels:
            if kern.mounts:
                _mounted.update([m[1] for m in kern.mounts])
        if params['name'] in _mounted:
            return web.json_response({
                'title': 'Target host is used in sessions',
                'message': 'Target host is used in sessions',
            }, status=409)

        query = (sa.select([agents.c.id])
                   .select_from(agents)
                   .where(agents.c.status == AgentStatus.ALIVE))
        if params['scaling_group'] is not None:
            query = query.where(agents.c.scaling == params['scaling_group'])
        result = await conn.execute(query)
        _agents = await result.fetchall()

    # Unmount from manager.
    proc = await asyncio.create_subprocess_exec(*[
        'sudo', 'umount', str(mountpoint)
    ], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    raw_out, raw_err = await proc.communicate()
    out = raw_out.decode('utf8')
    err = raw_err.decode('utf8')
    await proc.wait()
    resp: MutableMapping[str, Any] = {
        'manager': {
            'success': True if not err else False,
            'message': out if not err else err,
        },
        'agents': {},
    }
    if resp['manager']['success']:
        try:
            mountpoint.rmdir()  # delete directory if empty
        except OSError:
            pass
    if params['edit_fstab'] and resp['manager']['success']:
        fstab_path = params['fstab_path'] if params['fstab_path'] else '/etc/fstab'
        async with aiofiles.open(fstab_path, mode='r+') as fp:
            fstab = Fstab(fp)
            await fstab.remove_by_mountpoint(str(mountpoint))

    # Unmount from running agents.
    async def _umount(sess: aiohttp.ClientSession, agent_id: str) -> Tuple[str, Mapping]:
        watcher_info = await get_watcher_info(request, agent_id)
        try:
            headers = {'X-BackendAI-Watcher-Token': watcher_info['token']}
            url = watcher_info['addr'] / 'mounts'
            async with sess.delete(url, json=params, headers=headers) as resp:
                if resp.status == 200:
                    data = {
                        'success': True,
                        'message': await resp.text(),
                    }
                else:
                    data = {
                        'success': False,
                        'message': await resp.text(),
                    }
                return (agent_id, data,)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.error(log_fmt + ': timeout from watcher (agent:{})',
                      *log_args, agent_id)
            raise
        except Exception:
            log.exception(log_fmt + ': unexpected error while reading from watcher (agent:{})',
                          *log_args, agent_id)
            raise

    client_timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(timeout=client_timeout) as sess:
        scheduler = await aiojobs.create_scheduler(limit=8)
        try:
            jobs = await asyncio.gather(*[
                scheduler.spawn(_umount(sess, _agent.id)) for _agent in _agents
            ])
            results = await asyncio.gather(*[job.wait() for job in jobs],
                                           return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    # exceptions are already logged.
                    continue
                resp['agents'][result[0]] = result[1]
        finally:
            await scheduler.close()

    return web.json_response(resp, status=200)


async def init(app):
    mount_prefix = await app['config_server'].get('volumes/_mount')
    fs_prefix = await app['config_server'].get('volumes/_fsprefix')
    app['VFOLDER_MOUNT'] = Path(mount_prefix)
    app['VFOLDER_FSPREFIX'] = Path(fs_prefix.lstrip('/'))


async def shutdown(app):
    pass


def create_app(default_cors_options):
    app = web.Application()
    app['prefix'] = 'folders'
    app['api_versions'] = (2, 3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    root_resource = cors.add(app.router.add_resource(r''))
    cors.add(root_resource.add_route('POST', create))
    cors.add(root_resource.add_route('GET',  list_folders))
    cors.add(root_resource.add_route('DELETE',  delete_by_id))
    vfolder_resource = cors.add(app.router.add_resource(r'/{name}'))
    cors.add(vfolder_resource.add_route('GET',    get_info))
    cors.add(vfolder_resource.add_route('DELETE', delete))
    cors.add(add_route('GET',    r'/_/hosts', list_hosts))
    cors.add(add_route('GET',    r'/_/all_hosts', list_all_hosts))
    cors.add(add_route('GET',    r'/_/allowed_types', list_allowed_types))
    cors.add(add_route('GET',    r'/_/download_with_token', download_with_token))
    cors.add(add_route('HEAD',   r'/_/download_with_token', download_with_token))
    cors.add(add_route('POST',   r'/{name}/rename', rename))
    cors.add(add_route('POST',   r'/{name}/mkdir', mkdir))
    cors.add(add_route('POST',   r'/{name}/upload', upload))
    cors.add(add_route('POST',   r'/{name}/create_upload_session', create_tus_upload_session))
    cors.add(add_route('POST',   r'/{name}/rename_file', rename_file))
    cors.add(add_route('DELETE', r'/{name}/delete_files', delete_files))
    cors.add(add_route('GET',    r'/{name}/download', download))
    cors.add(add_route('GET',    r'/{name}/download_single', download_single))
    cors.add(add_route('HEAD',   r'/{name}/download_single', download_single))
    cors.add(add_route('POST',   r'/{name}/request_download', request_download))
    cors.add(add_route('GET',    r'/{name}/files', list_files))
    cors.add(add_route('POST',   r'/{name}/invite', invite))
    cors.add(add_route('GET',    r'/invitations/list_sent', list_sent_invitations))
    cors.add(add_route('POST',   r'/invitations/update/{inv_id}', update_invitation))
    cors.add(add_route('GET',    r'/invitations/list', invitations))
    cors.add(add_route('POST',   r'/invitations/accept', accept_invitation))
    cors.add(add_route('DELETE', r'/invitations/delete', delete_invitation))
    cors.add(add_route('GET',    r'/_/shared', list_shared_vfolders))
    cors.add(add_route('POST',   r'/_/shared', update_shared_vfolder))
    cors.add(add_route('GET',    r'/_/fstab', get_fstab_contents))
    cors.add(add_route('GET',    r'/_/mounts', list_mounts))
    cors.add(add_route('POST',   r'/_/mounts', mount_host))
    cors.add(add_route('DELETE', r'/_/mounts', umount_host))
    add_route('OPTIONS', r'/_/tus/upload/{session}', tus_options)
    add_route('HEAD',    r'/_/tus/upload/{session}', tus_check_session)
    add_route('PATCH',   r'/_/tus/upload/{session}', tus_upload_part)
    return app, []
