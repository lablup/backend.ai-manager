import functools
import json
import logging
import os
from pathlib import Path
import shutil
import stat
from typing import Any, Callable, Mapping
import uuid
import jwt
from datetime import datetime, timedelta

import aiohttp
from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import aiotools
import sqlalchemy as sa
import psycopg2
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .exceptions import (
    VFolderCreationFailed, VFolderNotFound, VFolderAlreadyExists,
    GenericForbidden, InvalidAPIParameters, ServerMisconfiguredError)
from .manager import (
    READ_ALLOWED, ALL_ALLOWED,
    server_status_required)
from .utils import check_api_params
from ..manager.models import (
    users, groups, keypairs, vfolders, vfolder_invitations, vfolder_permissions,
    VFolderPermission, VFolderPermissionValidator, query_accessible_vfolders,
    get_allowed_vfolder_hosts_by_group, get_allowed_vfolder_hosts_by_user,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.vfolder'))

VFolderRow = Mapping[str, Any]


def vfolder_permission_required(perm: VFolderPermission):
    '''
    Checks if the target vfolder exists and is either:
    - owned by the current access key, or
    - allowed accesses by the access key under the specified permission.

    The decorated handler should accept an extra argument
    which contains a dict object describing the matched VirtualFolder table row.
    '''

    def _wrapper(handler: Callable[[web.Request, VFolderRow], web.Response]):

        @functools.wraps(handler)
        async def _wrapped(request: web.Request, *args, **kwargs) -> web.Response:
            dbpool = request.app['dbpool']
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
                    allowed_vfolder_types=allowed_vfolder_types,
                    extra_vf_conds=(vfolders.c.name == folder_name),
                    extra_vfperm_conds=perm_cond)
                if len(entries) == 0:
                    raise VFolderNotFound(
                        'Your operation may be permission denied.')
                return await handler(request, entries[0], *args, **kwargs)

        return _wrapped

    return _wrapper


def vfolder_check_exists(handler: Callable[[web.Request, VFolderRow], web.Response]):
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
        t.Key('host', default=None) >> 'folder_host': t.Or(t.String, t.Null),
        tx.AliasedKey(['group', 'groupId', 'group_id'], default=None): t.Or(tx.UUID, t.Null),
    }),
)
async def create(request: web.Request, params: Any) -> web.Response:
    resp = {}
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    user_uuid = request['user']['uuid']
    resource_policy = request['keypair']['resource_policy']
    domain_name = request['user']['domain_name']
    group_id = params['group']
    log.info('VFOLDER.CREATE (u:{0})', access_key)
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

    async with dbpool.acquire() as conn:
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
            allowed_vfolder_types=allowed_vfolder_types,
            extra_vf_conds=(sa.and_(vfolders.c.name == params['name'],
                                    vfolders.c.host == folder_host))
        )
        if len(entries) > 0:
            raise VFolderAlreadyExists

        # Check if group exists.
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
            folder_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise VFolderCreationFailed
        user_uuid = str(user_uuid) if group_id is None else None
        group_uuid = str(group_id) if group_id is not None else None
        query = (vfolders.insert().values({
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
            'last_used': None,
            'user': user_uuid,
            'group': group_uuid,
        }))
        resp = {
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
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
async def list_folders(request: web.Request) -> web.Response:
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    user_uuid = request['user']['uuid']
    log.info('VFOLDER.LIST (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        params = await request.json() if request.can_read_body else request.query
        allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
        if request['is_superadmin'] and params.get('all', False):
            # List all folders for superadmin if all is specified
            query = sa.select([vfolders]).select_from(vfolders)
            result = await conn.execute(query)
            entries = []
            async for row in result:
                is_owner = True if row.user == user_uuid else False
                permission = VFolderPermission.OWNER_PERM if is_owner \
                        else VFolderPermission.READ_ONLY
                entries.append({
                    'name': row.name,
                    'id': row.id,
                    'host': row.host,
                    'created_at': row.created_at,
                    'is_owner': is_owner,
                    'permission': permission,
                    'user': str(row.user) if row.user else None,
                    'group': str(row.group) if row.group else None,
                    'type': 'user' if row['user'] is not None else 'group',
                })
        else:
            entries = await query_accessible_vfolders(
                conn, user_uuid, allowed_vfolder_types=allowed_vfolder_types)
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
                'type': 'user' if entry['user'] is not None else 'group',
            })
    return web.json_response(resp, status=200)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
async def list_hosts(request: web.Request) -> web.Response:
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST_HOSTS (u:{0})', access_key)
    config = request.app['config_server']
    dbpool = request.app['dbpool']
    domain_name = request['user']['domain_name']
    resource_policy = request['keypair']['resource_policy']
    async with dbpool.acquire() as conn:
        allowed_hosts = await get_allowed_vfolder_hosts_by_user(conn, resource_policy,
                                                                domain_name, request['user']['uuid'])
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
@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def get_info(request: web.Request, row: VFolderRow) -> web.Response:
    resp = {}
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.GETINFO (u:{0}, f:{1})', access_key, folder_name)
    if row['permission'] is None:
        is_owner = True
        permission = VFolderPermission.OWNER_PERM
    else:
        is_owner = row['is_owner']
        permission = row['permission']
    # TODO: handle nested directory structure
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    num_files = len(list(folder_path.iterdir()))
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
    user_uuid = request['user']['uuid']
    new_name = params['new_name']
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    log.info('VFOLDER.RENAME (u:{0}, f:{1} -> {2})',
             access_key, old_name, new_name)
    async with dbpool.acquire() as conn:
        entries = await query_accessible_vfolders(
            conn, user_uuid, allowed_vfolder_types=allowed_vfolder_types)
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
    log.info('VFOLDER.MKDIR (u:{0}, f:{1})', access_key, folder_name)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    assert not path.is_absolute(), 'path must be relative.'
    try:
        (folder_path / path).mkdir(parents=True, exist_ok=True)
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
    log.info('VFOLDER.UPLOAD (u:{0}, f:{1})', access_key, folder_name)
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
            file_dir.mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            raise InvalidAPIParameters(
                'Failed to create parent directories. '
                f'"{e.filename}" already exists and is not a directory.')
        with open(file_path, 'wb') as f:
            while not file.at_eof():
                chunk = await file.read_chunk(size=8192)
                f.write(file.decode(chunk))
    return web.Response(status=201)


@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.RW_DELETE)
@check_api_params(
    t.Dict({
        t.Key('files'): t.List[t.String],
        t.Key('recursive', default=False): t.Bool,
    }))
async def delete_files(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    recursive = params['recursive']
    log.info('VFOLDER.DELETE_FILES (u:{0}, f:{1})', access_key, folder_name)
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
    for op in ops:
        op()
    resp = {}
    return web.json_response(resp, status=200)


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
    log.info('VFOLDER.DOWNLOAD (u:{0}, f:{1})', access_key, folder_name)
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
        headers = {'Content-Encoding': 'gzip'}
        try:
            for file in files:
                data = open(folder_path / file, 'rb')
                payload = mpwriter.append(data, headers)
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
    }))
async def download_single(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    fn = params['file']
    log.info('VFOLDER.DOWNLOAD (u:{0}, f:{1})', access_key, folder_name)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    try:
        file_path = (folder_path / fn).resolve()
        file_path.relative_to(folder_path)
        if not file_path.exists():
            raise FileNotFoundError
    except (ValueError, FileNotFoundError):
        raise InvalidAPIParameters('The file is not found.')
    if not file_path.is_file():
        raise InvalidAPIParameters('The file is not a regular file.')

    return web.FileResponse(file_path)


@atomic
@auth_required
@server_status_required(READ_ALLOWED)
@vfolder_permission_required(VFolderPermission.READ_ONLY)
@check_api_params(
    t.Dict({
        t.Key('file'): t.String,
    }))
async def request_download(request: web.Request, params: Any, row: VFolderRow) -> web.Response:
    secret = request.app['config']['manager']['secret']
    p = {}
    p['file'] = params['file']
    p['host'] = row['host']
    p['id'] = row['id'].hex
    p['exp'] = datetime.utcnow() + timedelta(minutes=2)
    resp = {
        'token': jwt.encode(p, secret, algorithm='HS256').decode('UTF-8')
    }
    return web.json_response(resp, status=200)


async def download_with_token(request):
    try:
        secret = request.app['config']['manager']['secret']
        token = request.query.get('token', '')
        params = jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.PyJWTError:
        log.exception('jwt error while parsing "{}"', token)
        raise InvalidAPIParameters('Could not validate the download token.')

    assert params.get('file'), 'no file(s) specified!'
    fn = params.get('file')
    log.info('VFOLDER.DOWNLOAD_WITH_TOKEN (id:{0}, name:{1})', params['id'], fn)
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
        raise InvalidAPIParameters('The file is not a regular file.')

    return web.FileResponse(file_path)


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
    log.info('VFOLDER.LIST_FILES (u:{0}, f:{1})', access_key, folder_name)
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
    log.info('VFOLDER.LIST_SENT_INVITATIONS (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        j = sa.join(vfolders, vfolder_invitations,
                    vfolders.c.id == vfolder_invitations.c.vfolder)
        query = (sa.select([vfolder_invitations, vfolders.c.name])
                   .select_from(j)
                   .where((vfolder_invitations.c.inviter == request['user']['email']) &
                          (vfolder_invitations.c.state == 'pending')))
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
    log.info('VFOLDER.UPDATE_INVITATION (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        query = (sa.update(vfolder_invitations)
                   .values(permission=VFolderPermission(perm))
                   .where(vfolder_invitations.c.id == inv_id)
                   .where(vfolder_invitations.c.inviter == request['user']['email'])
                   .where(vfolder_invitations.c.state == 'pending'))
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
    log.info('VFOLDER.INVITE (u:{0}, f:{1})', access_key, folder_name)
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
                              (vfolder_invitations.c.state == 'pending')))
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
                'state': 'pending',
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
    log.info('VFOLDER.INVITATIONS (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        j = sa.join(vfolders, vfolder_invitations,
                    vfolders.c.id == vfolder_invitations.c.vfolder)
        query = (sa.select([vfolder_invitations, vfolders.c.name])
                   .select_from(j)
                   .where((vfolder_invitations.c.invitee == request['user']['id']) &
                          (vfolder_invitations.c.state == 'pending')))
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
    log.info('VFOLDER.ACCEPT_INVITATION (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        # Get invitation.
        query = (sa.select([vfolder_invitations])
                   .select_from(vfolder_invitations)
                   .where((vfolder_invitations.c.id == inv_id) &
                          (vfolder_invitations.c.state == 'pending')))
        result = await conn.execute(query)
        invitation = await result.first()
        if invitation is None:
            resp = {'msg': 'No such invitation found.'}
            return web.json_response(resp, status=404)

        # Get target virtual folder.
        query = (sa.select([vfolders.c.name])
                   .select_from(vfolders)
                   .where(vfolders.c.id == invitation.vfolder))
        result = await conn.execute(query)
        target_vfolder = await result.first()
        if target_vfolder is None:
            resp = {'msg': 'No such virtual folder found.'}
            return web.json_response(resp, status=404)

        # Prevent accepting vfolder with duplicated name.
        j = sa.join(vfolders, vfolder_permissions,
                    vfolders.c.id == vfolder_permissions.c.vfolder, isouter=True)
        query = (sa.select('*')
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
                                    .values(state='accepted'))
        await conn.execute(query)
    msg = (f'User {invitation.invitee} now can access '
           f'vfolder {invitation.vfolder}.')
    return web.json_response({'msg': msg}, status=201)


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
    inv_id = params['inv_id']
    log.info('VFOLDER.DELETE_INVITATION (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(vfolder_invitations)
                   .where((vfolder_invitations.c.id == inv_id) &
                          (vfolder_invitations.c.state == 'pending')))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            resp = {'msg': 'No such invitation found.'}
            return web.json_response(resp, status=404)
        query = (vfolder_invitations.update()
                                    .where(vfolder_invitations.c.id == inv_id)
                                    .values(state='rejected'))
        await conn.execute(query)
    resp = {'msg': f'Vfolder invitation is rejected: {inv_id}.'}
    return web.json_response(resp, status=200)


@auth_required
@server_status_required(ALL_ALLOWED)
async def delete(request: web.Request) -> web.Response:
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    user_uuid = request['user']['uuid']
    allowed_vfolder_types = await request.app['config_server'].get_vfolder_types()
    log.info('VFOLDER.DELETE (u:{0}, f:{1})', access_key, folder_name)
    async with dbpool.acquire() as conn, conn.begin():
        entries = await query_accessible_vfolders(
            conn, user_uuid,  allowed_vfolder_types=allowed_vfolder_types)
        for entry in entries:
            if entry['name'] == folder_name:
                if not entry['is_owner']:
                    raise InvalidAPIParameters(
                        'Cannot delete the vfolder '
                        'that is not owned by me.')
                break
        else:
            raise InvalidAPIParameters('No such group.')
        folder_path = (request.app['VFOLDER_MOUNT'] / entry['host'] /
                       request.app['VFOLDER_FSPREFIX'] / entry['id'].hex)
        try:
            shutil.rmtree(folder_path)
        except IOError:
            pass
        # TODO: mark it deleted instead of really deleting
        query = (vfolders.delete()
                         .where(vfolders.c.id == entry['id']))
        await conn.execute(query)
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
    log.info('VFOLDER.LIST_SHARED_VFOLDERS (u:{0})', access_key)
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
        tx.AliasedKey(['perm', 'permission']): VFolderPermissionValidator,
    })
)
async def update_shared_vfolder(request: web.Request, params: Any) -> web.Response:
    '''
    Update permission for shared vfolders.
    '''
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    vfolder_id = params['vfolder']
    user_uuid = params['user']
    perm = params['perm']
    log.info('VFOLDER.UPDATE_SHARED_VFOLDER(u:{0})', access_key)
    async with dbpool.acquire() as conn:
        query = (sa.update(vfolder_permissions)
                   .values(permission=VFolderPermission(perm))
                   .where(vfolder_permissions.c.vfolder == vfolder_id)
                   .where(vfolder_permissions.c.user == user_uuid))
        await conn.execute(query)
    resp = {'msg': f'shared vfolder permission updated'}
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
    vfolder_resource = cors.add(app.router.add_resource(r'/{name}'))
    cors.add(vfolder_resource.add_route('GET',    get_info))
    cors.add(vfolder_resource.add_route('DELETE', delete))
    cors.add(add_route('GET',    r'/_/hosts', list_hosts))
    cors.add(add_route('GET',    r'/_/download_with_token', download_with_token))
    cors.add(add_route('POST',   r'/{name}/rename', rename))
    cors.add(add_route('POST',   r'/{name}/mkdir', mkdir))
    cors.add(add_route('POST',   r'/{name}/upload', upload))
    cors.add(add_route('DELETE', r'/{name}/delete_files', delete_files))
    cors.add(add_route('GET',    r'/{name}/download', download))
    cors.add(add_route('GET',    r'/{name}/download_single', download_single))
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
    return app, []
