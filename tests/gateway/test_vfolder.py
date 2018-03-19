import json
import shutil

import pytest
import aiohttp


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


@pytest.mark.asyncio
async def test_upload_file(prepare_vfolder, get_headers, tmpdir,
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
