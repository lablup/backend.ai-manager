import logging
import os
from pathlib import Path
import re
import shutil
import uuid

from aiohttp import web
import sqlalchemy as sa

from .auth import auth_required
from .exceptions import FolderNotFound
from ..manager.models import vfolders

log = logging.getLogger('ai.backend.gateway.vfolder')

VF_ROOT = None

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')


@auth_required
async def create(request):
    resp = {}
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    params = await request.json()
    log.info(f"VFOLDER.CREATE (u:{access_key})")
    assert _rx_slug.search(params['name']) is not None
    async with dbpool.acquire() as conn:
        folder_id = uuid.uuid4().hex
        os.mkdir(VF_ROOT / folder_id)
        query = (vfolders.insert().values({
            'id': folder_id,
            'name': params['name'],
            'host': 'azure-shard01',
            'last_used': None,
            'belongs_to': access_key,
        }))
        resp = {
            'id': folder_id,
            'name': params['name'],
        }
        # TODO: check for duplicate name
        result = await conn.execute(query)
        assert result.rowcount == 1
    return web.json_response(resp, status=201)


@auth_required
async def list_folders(request):
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    log.info(f"VFOLDER.LIST (u:{access_key})")
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where(vfolders.c.belongs_to == access_key))
        result = await conn.execute(query)
        async for row in result:
            resp.append({
                'name': row.name,
                'id': row.id.hex,
            })
    return web.json_response(resp, status=200)


@auth_required
async def get_info(request):
    resp = {}
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info(f"VFOLDER.GETINFO (u:{access_key}, f:{folder_name})")
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where((vfolders.c.belongs_to == access_key) &
                          (vfolders.c.name == folder_name)))
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            raise FolderNotFound()
        # TODO: handle nested directory structure
        folder_path = (VF_ROOT / row.id.hex)
        num_files = len(list(folder_path.iterdir()))
        resp = {
            'name': row.name,
            'id': row.id.hex,
            'numFiles': num_files,
            'created': str(row.created_at),
        }
    return web.json_response(resp, status=200)


@auth_required
async def upload(request):
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info(f"VFOLDER.UPLOAD (u:{access_key}, f:{folder_name})")
    async with dbpool.acquire() as conn:
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where((vfolders.c.belongs_to == access_key) &
                          (vfolders.c.name == folder_name)))
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            log.error('why here')
            raise FolderNotFound()
        folder_path = (VF_ROOT / row.id.hex)
        reader = await request.multipart()
        file_count = 0
        while True:
            file = await reader.next()
            if file is None:
                break
            # TODO: impose limits on file size and count
            file_count += 1
            with open(folder_path / file.filename, 'wb') as f:
                while not file.at_eof():
                    chunk = await file.read_chunk(size=8192)
                    f.write(file.decode(chunk))
    return web.Response(status=201)


@auth_required
async def delete(request):
    dbpool = request.app['dbpool']
    folder_name = request.match_info['name']
    access_key = request['keypair']['access_key']
    log.info(f"VFOLDER.DELETE (u:{access_key}, f:{folder_name})")
    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select('*', for_update=True)
                   .select_from(vfolders)
                   .where((vfolders.c.belongs_to == access_key) &
                          (vfolders.c.name == folder_name)))
        result = await conn.execute(query)
        row = await result.first()
        if row is None:
            raise FolderNotFound()
        folder_id = row['id'].hex
        try:
            shutil.rmtree(VF_ROOT / folder_id)
        except IOError:
            pass
        # TODO: mark it deleted instead of really deleting
        query = (vfolders.delete()
                         .where(vfolders.c.id == row['id']))
        result = await conn.execute(query)
    return web.Response(status=204)


async def init(app):
    global VF_ROOT
    root_name = await app['config_server'].etcd.get('volumes/_vfroot')
    VF_ROOT = Path('/mnt') / root_name
    rt = app.router.add_route
    rt('POST',   r'/v{version:\d+}/folders/', create)
    rt('GET',    r'/v{version:\d+}/folders/', list_folders)
    rt('GET',    r'/v{version:\d+}/folders/{name}', get_info)
    rt('DELETE', r'/v{version:\d+}/folders/{name}', delete)
    rt('POST',   r'/v{version:\d+}/folders/{name}/upload', upload)


async def shutdown(app):
    pass
