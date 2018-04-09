import logging
from pathlib import Path
import re
import shutil
import uuid

from aiohttp import web
import aiotools
import sqlalchemy as sa
import psycopg2

from .auth import auth_required
from .exceptions import FolderNotFound, InvalidAPIParameters
from ..manager.models import vfolders

log = logging.getLogger('ai.backend.gateway.vfolder')

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
        # TODO: make this configurable
        folder_host = 'azure-shard01'
        folder_path = (request.app['VFOLDER_MOUNT'] / folder_host / folder_id)
        folder_path.mkdir(parents=True, exist_ok=True)
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
        }
        try:
            result = await conn.execute(query)
        except psycopg2.DataError as e:
            raise InvalidAPIParameters
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
        try:
            result = await conn.execute(query)
        except psycopg2.DataError as e:
            raise InvalidAPIParameters
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
        try:
            result = await conn.execute(query)
        except psycopg2.DataError as e:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            raise FolderNotFound()
        # TODO: handle nested directory structure
        folder_path = (request.app['VFOLDER_MOUNT'] / row.host / row.id.hex)
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
        try:
            result = await conn.execute(query)
        except psycopg2.DataError as e:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            log.error('why here')
            raise FolderNotFound()
        folder_path = (request.app['VFOLDER_MOUNT'] / row.host / row.id.hex)
        reader = await request.multipart()
        file_count = 0
        async for file in aiotools.aiter(reader.next, None):
            # TODO: impose limits on file size and count
            file_count += 1
            file_dir = (folder_path / file.filename).parent
            file_dir.mkdir(parents=True, exist_ok=True)
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
        try:
            result = await conn.execute(query)
        except psycopg2.DataError as e:
            raise InvalidAPIParameters
        row = await result.first()
        if row is None:
            raise FolderNotFound()
        folder_path = (request.app['VFOLDER_MOUNT'] / row.host / row.id.hex)
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
    app['VFOLDER_MOUNT'] = Path(mount_prefix)


async def shutdown(app):
    pass


def create_app():
    app = web.Application()
    app['prefix'] = 'folders'
    app['api_versions'] = (2, 3)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app.router.add_route('POST',   r'', create)
    app.router.add_route('GET',    r'', list_folders)
    app.router.add_route('GET',    r'/{name}', get_info)
    app.router.add_route('DELETE', r'/{name}', delete)
    app.router.add_route('POST',   r'/{name}/upload', upload)
    return app, []
