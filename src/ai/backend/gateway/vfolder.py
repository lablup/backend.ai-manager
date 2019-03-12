import functools
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import uuid

import aiohttp
from aiohttp import web
import aiohttp_cors
import aiotools
import sqlalchemy as sa
import psycopg2

from ai.backend.common.logging import BraceStyleAdapter

from .auth import auth_required
from .exceptions import (
    VFolderCreationFailed, VFolderNotFound, VFolderAlreadyExists,
    InvalidAPIParameters)
from .manager import (
    READ_ALLOWED, ALL_ALLOWED,
    server_status_required)
from ..manager.models import (
    keypairs, vfolders, vfolder_invitations, vfolder_permissions,
    VFolderPermission, query_accessible_vfolders)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.vfolder'))

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')


def vfolder_permission_required(perm: VFolderPermission):
    '''
    Checks if the target vfolder exists and is either:
    - owned by the current access key, or
    - allowed accesses by the access key under the specified permission.

    The decorated handler should accept an extra argument
    which contains a dict object describing the matched VirtualFolder table row.
    '''

    def _wrapper(handler):

        @functools.wraps(handler)
        async def _wrapped(request):
            dbpool = request.app['dbpool']
            access_key = request['keypair']['access_key']
            folder_name = request.match_info['name']
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
                    conn, access_key,
                    extra_vf_conds=(vfolders.c.name == folder_name),
                    extra_vfperm_conds=perm_cond)
                if len(entries) == 0:
                    raise VFolderNotFound(
                        'Your operation may be permission denied.')
                return await handler(request, row=entries[0])

        return _wrapped

    return _wrapper


def vfolder_check_exists(handler):
    '''
    Checks if the target vfolder exists and is owned
    by the current access key.

    The decorated handler should accept an extra "row" argument
    which contains the matched VirtualFolder table row.
    '''

    @functools.wraps(handler)
    async def _wrapped(request):
        dbpool = request.app['dbpool']
        access_key = request['keypair']['access_key']
        folder_name = request.match_info['name']
        async with dbpool.acquire() as conn:
            j = sa.join(
                vfolders, vfolder_permissions,
                vfolders.c.id == vfolder_permissions.c.vfolder, isouter=True)
            query = (
                sa.select('*')
                .select_from(j)
                .where(((vfolders.c.belongs_to == access_key) |
                        (vfolder_permissions.c.access_key == access_key)) &
                       (vfolders.c.name == folder_name)))
            try:
                result = await conn.execute(query)
            except psycopg2.DataError:
                raise InvalidAPIParameters
            row = await result.first()
            if row is None:
                raise VFolderNotFound()
            return await handler(request, row=row)

    return _wrapped


@server_status_required(ALL_ALLOWED)
@auth_required
async def create(request):
    resp = {}
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    resource_policy = request['keypair']['resource_policy']
    params = await request.json()
    log.info('VFOLDER.CREATE (u:{0})', access_key)
    assert _rx_slug.search(params.get('name', '')) is not None, 'invalid name format'
    # Resolve host for the new virtual folder.
    folder_host = params.get('host', None)
    if not folder_host:
        folder_host = \
            await request.app['config_server'].etcd.get('volumes/_default_host')
        if not folder_host:
            raise InvalidAPIParameters(
                'You must specify the vfolder host '
                'because the default host is not configured.')
    # Check resource policy's allowed_vfolder_hosts
    if folder_host not in resource_policy['allowed_vfolder_hosts']:
        raise InvalidAPIParameters('You are not allowed to use this vfolder host.')
    vfroot = (request.app['VFOLDER_MOUNT'] / folder_host /
              request.app['VFOLDER_FSPREFIX'])
    if not vfroot.is_dir():
        raise InvalidAPIParameters(f'Invalid vfolder host: {folder_host}')

    async with dbpool.acquire() as conn:
        # Check resource policy's max_vfolder_count
        if resource_policy['max_vfolder_count'] > 0:
            query = (sa.select([sa.func.count()])
                       .where(vfolders.c.belongs_to == access_key))
            result = await conn.scalar(query)
            if result >= resource_policy['max_vfolder_count']:
                raise InvalidAPIParameters('You cannot create more vfolders.')

        # Prevent creation of vfolder with duplicated name.
        entries = await query_accessible_vfolders(
            conn, access_key,
            extra_vf_conds=(vfolders.c.name == params['name']))
        if len(entries) > 0:
            raise VFolderAlreadyExists

        try:
            folder_id = uuid.uuid4().hex
            folder_path = (request.app['VFOLDER_MOUNT'] / folder_host /
                           request.app['VFOLDER_FSPREFIX'] / folder_id)
            folder_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise VFolderCreationFailed
        query = (vfolders.insert().values({
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
            'last_used': None,
            'belongs_to': access_key,
        }))
        resp = {
            'id': folder_id,
            'name': params['name'],
            'host': folder_host,
        }
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        assert result.rowcount == 1
    return web.json_response(resp, status=201)


@server_status_required(READ_ALLOWED)
@auth_required
async def list_folders(request):
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.LIST (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        entries = await query_accessible_vfolders(conn, access_key)
        for entry in entries:
            resp.append({
                'name': entry['name'],
                'id': entry['id'].hex,
                'host': entry['host'],
                'is_owner': entry['is_owner'],
                'permission': entry['permission'].value,
            })
    return web.json_response(resp, status=200)


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def get_info(request, row):
    resp = {}
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.GETINFO (u:{0}, f:{1})', access_key, folder_name)
    if row['permission'] is None:
        is_owner = True
        permission = VFolderPermission.OWNER_PERM
    else:
        is_owner = False
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
        'created': str(row['created_at']),
        'is_owner': is_owner,
        'permission': permission,
    }
    return web.json_response(resp, status=200)


@server_status_required(ALL_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.OWNER_PERM)
async def rename(request, row):
    dbpool = request.app['dbpool']
    old_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    params = await request.json()
    new_name = params.get('new_name', '')
    assert _rx_slug.search(new_name) is not None, 'invalid name format'
    log.info('VFOLDER.RENAME (u:{0}, f:{1} -> {2})',
             access_key, old_name, new_name)
    async with dbpool.acquire() as conn:
        entries = await query_accessible_vfolders(conn, access_key)
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


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_WRITE)
async def mkdir(request, row):
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    params = await request.json()
    path = params.get('path')
    assert path, 'path not specified!'
    path = Path(path)
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


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_WRITE)
async def upload(request, row):
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
        file_path = folder_path / file.filename
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


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.RW_DELETE)
async def delete_files(request, row):
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    params = await request.json()
    files = params.get('files')
    assert files, 'no file(s) specified!'
    recursive = params.get('recursive', False)
    log.info('VFOLDER.DELETE_FILES (u:{0}, f:{1})', access_key, folder_name)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    ops = []
    for file in files:
        file_path = folder_path / file
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


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def download(request, row):
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    params = await request.json()
    assert params.get('files'), 'no file(s) specified!'
    files = params.get('files')
    log.info('VFOLDER.DOWNLOAD (u:{0}, f:{1})', access_key, folder_name)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    for file in files:
        if not (folder_path / file).is_file():
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


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def download_single(request, row):
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    if request.can_read_body:
        params = await request.json()
    else:
        params = request.query
    assert params.get('file'), 'no file(s) specified!'
    fn = params.get('file')
    log.info('VFOLDER.DOWNLOAD (u:{0}, f:{1})', access_key, folder_name)
    folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                   request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    path = folder_path / fn
    if not (path).is_file():
        raise InvalidAPIParameters(
            f'You cannot download "{fn}" because it is not a regular file.')

    data = open(folder_path / fn, 'rb')
    return web.Response(body=data, status=200)


@server_status_required(READ_ALLOWED)
@auth_required
@vfolder_permission_required(VFolderPermission.READ_ONLY)
async def list_files(request, row):
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    if request.can_read_body:
        params = await request.json()
    else:
        params = request.query

    log.info('VFOLDER.LIST_FILES (u:{0}, f:{1})', access_key, folder_name)
    base_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                 request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
    folder_path = base_path / params['path'] if 'path' in params else base_path
    if not str(folder_path).startswith(str(base_path)):
        resp = {'error_msg': 'No such file or directory'}
        return web.json_response(resp, status=404)
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


@server_status_required(ALL_ALLOWED)
@auth_required
async def invite(request):
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
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
                   .where((vfolders.c.belongs_to == access_key) &
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


@server_status_required(READ_ALLOWED)
@auth_required
async def invitations(request):
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.INVITATION (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(vfolder_invitations)
                   .where((vfolder_invitations.c.invitee == request['user']['id']) &
                          (vfolder_invitations.c.state == 'pending')))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        invitations = await result.fetchall()
    invs_info = []
    for inv in invitations:
        invs_info.append({
            'id': str(inv.id),
            'inviter': inv.inviter,
            'perm': inv.permission,
            'state': inv.state,
            'created_at': str(inv.created_at),
            'vfolder_id': str(inv.vfolder),
        })
    resp = {'invitations': invs_info}
    return web.json_response(resp, status=200)


@server_status_required(ALL_ALLOWED)
@auth_required
async def accept_invitation(request):
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    params = await request.json()
    inv_id = params['inv_id']
    inv_ak = params['inv_ak']
    log.info('VFOLDER.ACCEPT_INVITATION (u:{0})', access_key)
    async with dbpool.acquire() as conn:
        # Get invitation.
        query = (sa.select('*')
                   .select_from(vfolder_invitations)
                   .where((vfolder_invitations.c.id == inv_id) &
                          (vfolder_invitations.c.state == 'pending')))
        result = await conn.execute(query)
        invitation = await result.first()

        # Get target virtual folder.
        query = (sa.select('*')
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
                   .where(((vfolders.c.belongs_to == inv_ak) |
                           (vfolder_permissions.c.access_key == inv_ak)) &
                          (vfolders.c.name == target_vfolder.name)))
        result = await conn.execute(query)
        if result.rowcount > 0:
            raise VFolderAlreadyExists

        if invitation is None:
            resp = {'msg': 'No such invitation found.'}
            return web.json_response(resp, status=404)

        # Check the access_key is valid by comparing with invitation info.
        query = (sa.select('*')
                   .select_from(keypairs)
                   .where(keypairs.c.access_key == inv_ak))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            invitation = {'msg': 'Invalid invitee access_key'}
            return web.json_response(resp, status=404)
        assert row['user_id'] == invitation.invitee

        # Create permission relation between the vfolder and the invitee.
        query = (vfolder_permissions.insert().values({
            'permission': VFolderPermission(invitation.permission),
            'vfolder': invitation.vfolder,
            'access_key': inv_ak,
        }))
        await conn.execute(query)

        # Clear used invitation.
        query = (vfolder_invitations.update()
                                    .where(vfolder_invitations.c.id == inv_id)
                                    .values(state='accepted'))
        await conn.execute(query)
    msg = (f'Access key ({inv_ak} by {invitation.invitee}) now can access '
           f'vfolder {invitation.vfolder}.')
    return web.json_response({'msg': msg}, status=201)


@server_status_required(ALL_ALLOWED)
@auth_required
async def delete_invitation(request):
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    params = await request.json()
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


@server_status_required(ALL_ALLOWED)
@auth_required
async def delete(request):
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info('VFOLDER.DELETE (u:{0}, f:{1})', access_key, folder_name)
    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select('*', for_update=True)
                   .select_from(vfolders)
                   .where((vfolders.c.belongs_to == access_key) &
                          (vfolders.c.name == folder_name)))
        try:
            result = await conn.execute(query)
        except psycopg2.DataError:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            raise VFolderNotFound()
        folder_path = (request.app['VFOLDER_MOUNT'] / row['host'] /
                       request.app['VFOLDER_FSPREFIX'] / row['id'].hex)
        try:
            shutil.rmtree(folder_path)
        except IOError:
            pass
        # TODO: mark it deleted instead of really deleting
        query = (vfolders.delete()
                         .where(vfolders.c.id == row['id']))
        result = await conn.execute(query)
    return web.Response(status=204)


async def init(app):
    mount_prefix = await app['config_server'].etcd.get('volumes/_mount')
    if mount_prefix is None:
        mount_prefix = '/mnt'
    fs_prefix = await app['config_server'].etcd.get('volumes/_fsprefix')
    if fs_prefix is None:
        fs_prefix = '/'
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
    cors.add(add_route('POST',   r'/{name}/rename', rename))
    cors.add(add_route('POST',   r'/{name}/mkdir', mkdir))
    cors.add(add_route('POST',   r'/{name}/upload', upload))
    cors.add(add_route('DELETE', r'/{name}/delete_files', delete_files))
    cors.add(add_route('GET',    r'/{name}/download', download))
    cors.add(add_route('GET',    r'/{name}/download_single', download_single))
    cors.add(add_route('GET',    r'/{name}/files', list_files))
    cors.add(add_route('POST',   r'/{name}/invite', invite))
    cors.add(add_route('GET',    r'/invitations/list', invitations))
    cors.add(add_route('POST',   r'/invitations/accept', accept_invitation))
    cors.add(add_route('DELETE', r'/invitations/delete', delete_invitation))
    return app, []
