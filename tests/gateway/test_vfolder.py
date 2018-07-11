import json
import shutil
import uuid
import zlib

import aiohttp
import pytest
import sqlalchemy as sa

from ai.backend.manager.models import vfolder_invitations, vfolder_permissions


@pytest.fixture
def folder_host():
    # FIXME: generalize this
    return 'azure-shard01'


@pytest.fixture
async def prepare_vfolder(event_loop, request, create_app_and_client, get_headers,
                          folder_mount):
    app, client = await create_app_and_client(modules=['etcd', 'auth', 'vfolder'])

    folder_name = 'test-folder'
    folder_id = None

    folder_mount.mkdir(parents=True, exist_ok=True)

    def _remove_folder_mount():
        shutil.rmtree(folder_mount)

    request.addfinalizer(_remove_folder_mount)

    async def create_vfolder():
        nonlocal folder_id

        url = '/v3/folders/'
        req_bytes = json.dumps({'name': folder_name}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status == 201
        rsp_json = await ret.json()
        folder_id = rsp_json['id']
        assert rsp_json['name'] == folder_name

        return rsp_json

    return app, client, create_vfolder


@pytest.mark.asyncio
async def test_create_vfolder(prepare_vfolder, get_headers):
    app, client, create_vfolder = prepare_vfolder
    folder_info = await create_vfolder()

    assert 'id' in folder_info
    assert 'name' in folder_info


@pytest.mark.asyncio
async def test_list_vfolders(prepare_vfolder, get_headers):
    app, client, create_vfolder = prepare_vfolder

    # Ensure there's no test folders
    url = '/v3/folders/'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    assert len(await ret.json()) == 0

    # Create a vfolder
    folder_info = await create_vfolder()

    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert len(rsp_json) == 1
    assert rsp_json[0]['id'] == folder_info['id']
    assert rsp_json[0]['name'] == folder_info['name']
    assert rsp_json[0]['is_owner']


@pytest.mark.asyncio
async def test_cannot_list_other_users_vfolders(prepare_vfolder, get_headers,
                                                user_keypair):
    # Create admin's vfolder
    app, client, create_vfolder = prepare_vfolder
    await create_vfolder()

    # Ensure other user's vfolder is not listed.
    url = '/v3/folders/'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
    ret = await client.get(url, data=req_bytes, headers=headers)
    rsp_json = await ret.json()

    assert ret.status == 200
    assert len(rsp_json) == 0


@pytest.mark.asyncio
async def test_get_info(prepare_vfolder, get_headers):
    app, client, create_vfolder = prepare_vfolder
    folder_info = await create_vfolder()

    url = f'/v3/folders/{folder_info["name"]}'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 200
    rsp_json = await ret.json()
    assert rsp_json['id'] == folder_info['id']
    assert rsp_json['name'] == folder_info['name']
    assert rsp_json['numFiles'] == 0
    assert rsp_json['is_owner']


@pytest.mark.asyncio
async def test_cannot_get_info_other_users_vfolders(prepare_vfolder, get_headers,
                                                    user_keypair):
    # Create admin's vfolder.
    app, client, create_vfolder = prepare_vfolder
    folder_info = await create_vfolder()

    url = f'/v3/folders/{folder_info["name"]}'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
    ret = await client.get(url, data=req_bytes, headers=headers)

    assert ret.status == 404


@pytest.mark.asyncio
async def test_delete_vfolder(prepare_vfolder, get_headers,
                              folder_mount, folder_host):
    app, client, create_vfolder = prepare_vfolder
    folder_info = await create_vfolder()
    folder_path = (folder_mount / folder_host / folder_info['id'])

    assert folder_path.exists()

    url = f'/v3/folders/{folder_info["name"]}'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('DELETE', url, req_bytes)
    ret = await client.delete(url, data=req_bytes, headers=headers)

    assert ret.status == 204
    assert not folder_path.exists()


@pytest.mark.asyncio
async def test_cannot_delete_others_vfolder(prepare_vfolder, get_headers,
                                            folder_mount, folder_host, user_keypair):
    app, client, create_vfolder = prepare_vfolder
    folder_info = await create_vfolder()
    folder_path = (folder_mount / folder_host / folder_info['id'])

    assert folder_path.exists()

    url = f'/v3/folders/{folder_info["name"]}'
    req_bytes = json.dumps({}).encode()
    headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
    ret = await client.delete(url, data=req_bytes, headers=headers)

    assert ret.status == 404


class TestFiles:

    @pytest.mark.asyncio
    async def test_upload_file(self, prepare_vfolder, get_headers, tmpdir,
                               folder_mount, folder_host):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        # Create files
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')
        p2 = tmpdir.join('test2.txt')
        p2.write('35711')

        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))
        data.add_field('file', open(p2, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data')
        ret = await client.post(url, data=data, headers=headers)

        # Get paths for files uploaded to virtual folder
        vf_fname1 = p1.strpath.split('/')[-1]
        vf_fname2 = p2.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host / folder_info['id'])

        assert ret.status == 201
        assert 2 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()
        assert (folder_path / vf_fname2).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_other_users_vfolder(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        # Create file
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')

        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=user_keypair)
        ret = await client.post(url, data=data, headers=headers)

        vf_fname1 = p1.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host / folder_info['id'])
        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_download(self, prepare_vfolder, get_headers, folder_mount,
                            folder_host):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)

        # Decode multipart response. Here, there's only one part.
        reader = aiohttp.MultipartReader.from_response(ret)
        part = await reader.next()
        encoding = part.headers['Content-Encoding']
        zlib_mode = (16 + zlib.MAX_WBITS
                         if encoding == 'gzip'
                         else -zlib.MAX_WBITS)
        decompressor = zlib.decompressobj(wbits=zlib_mode)
        content = decompressor.decompress(await part.read())

        assert content == b'hello vfolder!'
        assert ret.status == 200

    @pytest.mark.asyncio
    async def test_cannot_download_from_other_users_vfolder(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_list_files(self, prepare_vfolder, get_headers, folder_mount,
                              folder_host):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()
        files = json.loads(rsp_json['files'])

        assert files[0]['filename'] == 'hello.txt'
        assert rsp_json['folder_path'] == str(folder_path)

    @pytest.mark.asyncio
    async def test_cannot_list_other_vfolder_files(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_delete_files(self, prepare_vfolder, get_headers, folder_mount,
                                folder_host):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        assert not (folder_path / 'hello.txt').exists()

    @pytest.mark.asyncio
    async def test_cannot_delete_other_vfolder_files(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 404
        assert (folder_path / 'hello.txt').exists()


class TestInvitation:

    @pytest.mark.asyncio
    async def test_invite(self, prepare_vfolder, get_headers, folder_mount,
                          folder_host):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        url = f'/v3/folders/{folder_info["name"]}/invite'
        req_bytes = json.dumps({'perm': 'rw',
                                'user_ids': ['user@lablup.com']}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        async with app['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitation = await result.first()

        assert invitation.permission == 'rw'
        assert invitation.inviter == 'admin@lablup.com'
        assert rsp_json['invited_ids'][0] == 'user@lablup.com'

    @pytest.mark.asyncio
    async def test_invitations(self, prepare_vfolder, get_headers, folder_mount,
                               folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': uuid.uuid4().hex,
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/list'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['invitations']) == 1
        assert rsp_json['invitations'][0]['inviter'] == 'admin@lablup.com'

    @pytest.mark.asyncio
    async def test_accept_invitation(self, prepare_vfolder, get_headers,
                                     folder_mount, folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        inv_id = uuid.uuid4().hex
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': inv_id,
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/accept'
        req_bytes = json.dumps({'inv_id': inv_id,
                                'inv_ak': user_keypair['access_key']}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(vfolder_permissions)
                       .where(vfolder_permissions.c.access_key ==
                              user_keypair['access_key']))
            result = await conn.execute(query)
            perm = await result.first()

            query = (sa.select('*')
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitations = await result.fetchall()

        assert ret.status == 201
        assert perm.permission == 'ro'
        assert len(invitations) == 0

    @pytest.mark.asyncio
    async def test_delete_invitation(self, prepare_vfolder, get_headers,
                                     folder_mount, folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        inv_id = uuid.uuid4().hex
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': inv_id,
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/delete'
        req_bytes = json.dumps({'inv_id': inv_id}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            query = (sa.select('*')
                       .select_from(vfolder_permissions)
                       .where(vfolder_permissions.c.access_key ==
                              user_keypair['access_key']))
            result = await conn.execute(query)
            perms = await result.fetchall()

            query = (sa.select('*')
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitations = await result.fetchall()

        assert ret.status == 200
        assert len(perms) == 0
        assert len(invitations) == 0


class TestJoinedVfolderManipulations:

    @pytest.mark.asyncio
    async def test_list_vfolders(self, prepare_vfolder, get_headers, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        # Create admin's vfolder.
        folder_info = await create_vfolder()

        # Create vfolder_permissions.
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        url = '/v3/folders/'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert len(rsp_json) == 1
        assert rsp_json[0]['id'] == folder_info['id']
        assert rsp_json[0]['name'] == folder_info['name']
        assert not rsp_json[0]['is_owner']

    @pytest.mark.asyncio
    async def test_get_info(self, prepare_vfolder, get_headers, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['id'] == folder_info['id']
        assert rsp_json['name'] == folder_info['name']
        assert not rsp_json['is_owner']
        assert rsp_json['numFiles'] == 0

    @pytest.mark.asyncio
    async def test_upload_file(self, prepare_vfolder, get_headers, tmpdir,
                               folder_mount, folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'rw',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        # Prepare file to be uploaded.
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=user_keypair)
        ret = await client.post(url, data=data, headers=headers)

        # Get paths for files uploaded to virtual folder
        vf_fname1 = p1.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host / folder_info['id'])

        assert ret.status == 201
        assert 1 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_file_to_read_only_vfolders(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        # Prepare file to be uploaded.
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=user_keypair)
        ret = await client.post(url, data=data, headers=headers)

        # Get paths for files uploaded to virtual folder
        vf_fname1 = p1.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host / folder_info['id'])

        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_download(self, prepare_vfolder, get_headers, folder_mount,
                            folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        # Decode multipart response. Here, there's only one part.
        reader = aiohttp.MultipartReader.from_response(ret)
        part = await reader.next()
        encoding = part.headers['Content-Encoding']
        zlib_mode = (16 + zlib.MAX_WBITS
                         if encoding == 'gzip'
                         else -zlib.MAX_WBITS)
        decompressor = zlib.decompressobj(wbits=zlib_mode)
        content = decompressor.decompress(await part.read())

        assert content == b'hello vfolder!'
        assert ret.status == 200

    @pytest.mark.asyncio
    async def test_list_files(self, prepare_vfolder, get_headers, folder_mount,
                              folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()
        files = json.loads(rsp_json['files'])

        assert files[0]['filename'] == 'hello.txt'
        assert rsp_json['folder_path'] == str(folder_path)

    @pytest.mark.asyncio
    async def test_delete_files(self, prepare_vfolder, get_headers, folder_mount,
                                folder_host, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'rw',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        assert not (folder_path / 'hello.txt').exists()

    @pytest.mark.asyncio
    async def test_cannot_delete_readonly_vfolder_files(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_permissions.insert().values({
                'permission': 'ro',
                'vfolder': folder_info['id'],
                'access_key': user_keypair['access_key']
            }))
            await conn.execute(query)

        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 404
        assert (folder_path / 'hello.txt').exists()
