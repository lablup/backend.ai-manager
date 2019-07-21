import json
import shutil
import uuid
import zlib

import aiohttp
import pytest
import sqlalchemy as sa
import subprocess

from ai.backend.manager.models import (
    groups, keypairs, keypair_resource_policies, users,
    vfolders, vfolder_invitations, vfolder_permissions,
    VFolderPermission
)
from ai.backend.manager.models.user import UserRole
from tests.model_factory import (
    get_random_string,
    AssociationGroupsUsersFactory, DomainFactory, GroupFactory, UserFactory,
    VFolderFactory, VFolderInvitationFactory, VFolderPermissionFactory
)


@pytest.fixture
async def prepare_vfolder(event_loop, request, create_app_and_client, get_headers,
                          folder_mount, folder_host, folder_fsprefix):
    app, client = await create_app_and_client(
        modules=['etcd', 'auth', 'vfolder', 'manager'])

    folder_name = 'test-folder'
    folder_id = None

    base_path = folder_mount / folder_host / folder_fsprefix
    base_path.mkdir(parents=True, exist_ok=True)

    def _remove_folder_mount():
        shutil.rmtree(folder_mount)

    request.addfinalizer(_remove_folder_mount)

    async def create_vfolder(name=None, host=None, group=None, keypair=None):
        nonlocal folder_name, folder_id
        folder_name = name if name is not None else folder_name

        url = '/v3/folders/'
        req_bytes = json.dumps({
            'name': folder_name,
            'host': host,
            'group': group,
        }).encode()
        if keypair is not None:
            headers = get_headers('POST', url, req_bytes, keypair=keypair)
        else:
            headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 201
        folder_id = rsp_json['id']
        assert rsp_json['name'] == folder_name

        return rsp_json

    return app, client, create_vfolder


@pytest.fixture
async def other_host(folder_mount, folder_fsprefix):
    other_host = 'other-host'
    other_host_path = folder_mount / other_host / folder_fsprefix
    other_host_path.mkdir(parents=True, exist_ok=True)
    return other_host


@pytest.mark.asyncio
class TestVFolder:

    async def test_create_vfolder_in_default_host(self, prepare_vfolder, folder_host):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        assert 'id' in folder_info
        assert 'name' in folder_info
        assert folder_info.get('host', None) == folder_host

    async def test_create_vfolder_in_other_host(
            self, prepare_vfolder, folder_mount, other_host):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder(host=other_host)
        assert 'id' in folder_info
        assert 'name' in folder_info
        assert folder_info.get('host', None) == other_host

    async def test_create_vfolder_with_same_name_in_other_host(
            self, prepare_vfolder, other_host):
        app, client, create_vfolder = prepare_vfolder
        await create_vfolder()
        folder_info = await create_vfolder(host=other_host)

        assert 'id' in folder_info
        assert 'name' in folder_info
        assert folder_info.get('host', None) == other_host

    async def test_cannot_create_vfolder_in_not_existing_host(self, prepare_vfolder,
                                                              get_headers):
        app, client, create_vfolder = prepare_vfolder

        url = '/v3/folders/'
        req_bytes = json.dumps({
            'name': 'test-vfolder',
            'host': 'not-existing-host',
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status == 400

    async def test_cannot_create_vfolder_with_duplicated_name(self, prepare_vfolder,
                                                              get_headers):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        url = '/v3/folders/'
        req_bytes = json.dumps({'name': folder_info['name']}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            query = (sa.select('*').select_from(vfolders))
            result = await conn.execute(query)

        assert ret.status == 400
        assert result.rowcount == 1

    async def test_list_vfolders(self, prepare_vfolder, get_headers, other_host):
        app, client, create_vfolder = prepare_vfolder

        # Ensure there's no test folders
        url = '/v3/folders/'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        assert len(await ret.json()) == 0

        # Create a vfolder
        folder_info1 = await create_vfolder()
        folder_info2 = await create_vfolder(other_host)

        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert len(rsp_json) == 2
        rsp_info_pairs = []
        if rsp_json[0]['id'] == folder_info1['id']:
            rsp_info_pairs.append((rsp_json[0], folder_info1))
            rsp_info_pairs.append((rsp_json[1], folder_info2))
        else:
            rsp_info_pairs.append((rsp_json[0], folder_info2))
            rsp_info_pairs.append((rsp_json[1], folder_info1))

        for rsp_json, folder_info in rsp_info_pairs:
            assert rsp_json['id'] == folder_info['id']
            assert rsp_json['name'] == folder_info['name']
            assert rsp_json['is_owner']
            assert rsp_json['permission'] == VFolderPermission.OWNER_PERM.value
            assert rsp_json['host'] == folder_info['host']

    async def test_cannot_list_other_users_vfolders(self, prepare_vfolder, get_headers,
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

    async def test_get_info(self, prepare_vfolder, get_headers, other_host):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder(other_host)

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['id'] == folder_info['id']
        assert rsp_json['name'] == folder_info['name']
        assert rsp_json['host'] == folder_info['host']
        assert rsp_json['numFiles'] == 0
        assert rsp_json['is_owner']
        assert rsp_json['permission'] == VFolderPermission.OWNER_PERM.value

    async def test_cannot_get_info_other_users_vfolders(self, prepare_vfolder, get_headers,
                                                        user_keypair):
        # Create admin's vfolder.
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    async def test_rename_vfolder(self, prepare_vfolder, get_headers):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        url = f'/v3/folders/{folder_info["name"]}/rename'
        req_bytes = json.dumps({'new_name': 'renamed-test-default-vf'}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status == 201
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([vfolders])
                       .select_from(vfolders)
                       .where(vfolders.c.id == folder_info['id']))
            result = await conn.execute(query)
            vfolder = await result.first()
        assert vfolder.name == 'renamed-test-default-vf'

    async def test_cannot_rename_other_users_vfolder(self, prepare_vfolder, get_headers,
                                                     user_keypair):
        # Create admin's vfolder.
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        url = f'/v3/folders/{folder_info["name"]}/rename'
        req_bytes = json.dumps({'new_name': 'renamed-test-default-vf'}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status != 201
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([vfolders])
                       .select_from(vfolders)
                       .where(vfolders.c.id == folder_info['id']))
            result = await conn.execute(query)
            vfolder = await result.first()
        assert vfolder.name != 'renamed-test-default-vf'

    async def test_delete_vfolder(self, prepare_vfolder, get_headers,
                                  folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host / folder_fsprefix / folder_info['id'])

        assert folder_path.exists()

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('DELETE', url, req_bytes)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 204
        assert not folder_path.exists()

    async def test_cannot_delete_others_vfolder(
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert folder_path.exists()

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 400


@pytest.mark.asyncio
class TestGroupVFolder:
    @pytest.fixture(autouse=True)
    def set_vfolder_type_to_group(self):
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                         'config/volumes/_type', 'group'])
        yield
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'delete',
                         'config/volumes/_type'])

    async def create_group_vfolder(self, app, create_vfolder, name=None, host=None,
                                   keypair=None):
        # Create a group vfolder with group_id of the default group.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == 'default')
                       .where(groups.c.name == 'default'))
            group_id = str(await conn.scalar(query))
        folder_info = await create_vfolder(group=group_id, name=name, host=host,
                                           keypair=keypair)
        return folder_info

    async def test_domain_admin_create_gvfolder_in_default_host(
            self, folder_host, prepare_vfolder, get_headers, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        group = await GroupFactory(app).get(domain_name='default', name='default')

        url = '/v3/folders/'
        folder_name = 'domain-admin-created-gvfolder-in-default-host'
        req_bytes = json.dumps({
            'name': folder_name,
            'host': folder_host,
            'group': str(group.id),
        }).encode()
        headers = get_headers('POST', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        vf = await VFolderFactory(app).get(name=folder_name, group=str(group.id))
        assert ret.status == 201
        assert vf is not None
        assert rsp_json['id'] == vf.id.hex
        assert rsp_json['name'] == folder_name

    async def test_superadmin_cannot_create_gvfolder(
            self, folder_host, prepare_vfolder, get_headers):
        # Virtual folders should belong to each domain admin.
        app, client, create_vfolder = prepare_vfolder
        group = await GroupFactory(app).get(domain_name='default', name='default')

        url = '/v3/folders/'
        folder_name = 'domain-superadmin-created-gvfolder-in-default-host'
        req_bytes = json.dumps({
            'name': folder_name,
            'host': folder_host,
            'group': str(group.id),
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vf = await VFolderFactory(app).get(name=folder_name, group=str(group.id))
        assert ret.status != 201
        assert vf is None

    async def test_cannot_create_gvfolder_in_not_existing_host(
            self, prepare_vfolder, get_headers, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        group = await GroupFactory(app).get(domain_name='default', name='default')

        url = '/v3/folders/'
        folder_name = 'domain-admin-created-gvfolder-in-non-existing-host'
        req_bytes = json.dumps({
            'name': folder_name,
            'host': 'non-existing-host',
            'group': str(group.id),
        }).encode()
        headers = get_headers('POST', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        assert ret.status == 400

    async def test_cannot_create_gvfolder_with_duplicated_name(
            self, folder_host, prepare_vfolder, get_headers, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        grp_factory = GroupFactory(app)
        group = await grp_factory.get(domain_name='default', name='default')

        # Create a vfolder.
        req_data = {
            'name': 'domain-admin-created-gvfolder-in-non-existing-host',
            'host': folder_host,
            'group': str(group.id),
        }
        await VFolderFactory(app).create(**req_data)
        groups = await grp_factory.list(domain_name='default', name='default')
        assert len(groups) == 1

        # Try to create vfolder with the same name, group and host.
        url = '/v3/folders/'
        req_bytes = json.dumps(req_data).encode()
        headers = get_headers('POST', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)
        assert ret.status == 400
        groups = await grp_factory.list(domain_name='default', name='default')
        assert len(groups) == 1

    async def test_list_gvfolders(self, prepare_vfolder, get_headers, other_host,
                                  default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder

        # Ensure there's no test folders
        url = '/v3/folders/'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        assert len(await ret.json()) == 0

        # Create a vfolder
        folder_info1 = await self.create_group_vfolder(app, create_vfolder,
                                                       keypair=default_domain_keypair)
        folder_info2 = await self.create_group_vfolder(
            app, create_vfolder, name='list-gvfolders-2', host=other_host,
            keypair=default_domain_keypair)

        headers = get_headers('GET', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json) == 2
        rsp_info_pairs = []
        if rsp_json[0]['id'] == folder_info1['id']:
            rsp_info_pairs.append((rsp_json[0], folder_info1))
            rsp_info_pairs.append((rsp_json[1], folder_info2))
        else:
            rsp_info_pairs.append((rsp_json[0], folder_info2))
            rsp_info_pairs.append((rsp_json[1], folder_info1))

        for rsp_json, folder_info in rsp_info_pairs:
            assert rsp_json['id'] == folder_info['id']
            assert rsp_json['name'] == folder_info['name']
            assert rsp_json['is_owner']
            assert rsp_json['permission'] == VFolderPermission.OWNER_PERM.value
            assert rsp_json['host'] == folder_info['host']

    async def test_list_gvfolder_by_group_member(self, prepare_vfolder, get_headers,
                                                 default_domain_keypair, user_keypair):
        # Create admin's vfolder
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Ensure other user's vfolder is not listed.
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
        assert rsp_json[0]['permission'] == VFolderPermission.READ_WRITE.value

    async def test_cannot_list_gvfolder_in_other_group(self, prepare_vfolder, get_headers,
                                                       default_domain_keypair):
        # Create group vfolder in default group.
        app, client, create_vfolder = prepare_vfolder
        await self.create_group_vfolder(app, create_vfolder, keypair=default_domain_keypair)

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        # Ensure user in other group cannot see group vfolder in the default group.
        url = '/v3/folders/'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert len(rsp_json) == 0

    async def test_cannot_list_gvfolder_in_other_domain_by_domain_admin(
            self, prepare_vfolder, get_headers, default_domain_keypair):
        # Create group vfolder in default group.
        app, client, create_vfolder = prepare_vfolder
        await self.create_group_vfolder(app, create_vfolder, keypair=default_domain_keypair)

        # Create other domain and domain admin for it.
        other_domain = await DomainFactory(app).create()
        other_group = await GroupFactory(app).create(domain_name=other_domain['name'])
        admin_in_other_domain = await UserFactory(app).create(role=UserRole.ADMIN)
        await AssociationGroupsUsersFactory(app).create(
            user_id=admin_in_other_domain['uuid'],
            group_id=other_group['id'])

        # Ensure other domain's admin cannot see group vfolder in the default group.
        url = '/v3/folders/'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=admin_in_other_domain['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert len(rsp_json) == 0

    async def test_get_gvfolder_info(self, prepare_vfolder, get_headers, other_host,
                                     default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['id'] == folder_info['id']
        assert rsp_json['name'] == folder_info['name']
        assert rsp_json['host'] == folder_info['host']
        assert rsp_json['numFiles'] == 0
        assert rsp_json['is_owner']
        assert rsp_json['permission'] == VFolderPermission.OWNER_PERM.value

    async def test_cannot_get_info_in_other_group(self, prepare_vfolder, get_headers,
                                                  default_domain_keypair, user_keypair):
        # Create admin's vfolder in default group.
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        # Ensure user in other group cannot get group vfolder info in the default group.
        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    async def test_rename_gvfolder_by_domain_admin(self, prepare_vfolder, get_headers,
                                                   default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        url = f'/v3/folders/{folder_info["name"]}/rename'
        req_bytes = json.dumps({'new_name': 'renamed-test-default-gvf'}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vf = await VFolderFactory(app).get(id=folder_info['id'])
        assert ret.status == 201
        assert vf.name == 'renamed-test-default-gvf'

    async def test_cannot_rename_gvfolder_by_superadmin(self, prepare_vfolder, get_headers,
                                                        default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        url = f'/v3/folders/{folder_info["name"]}/rename'
        req_bytes = json.dumps({'new_name': 'renamed-test-default-gvf'}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vf = await VFolderFactory(app).get(id=folder_info['id'])
        assert ret.status != 201
        assert vf.name != 'renamed-test-default-gvf'

    async def test_cannot_rename_gvfolder_by_member(self, prepare_vfolder, get_headers,
                                                    default_domain_keypair, user_keypair):
        # Create admin's vfolder.
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        url = f'/v3/folders/{folder_info["name"]}/rename'
        req_bytes = json.dumps({'new_name': 'renamed-test-default-gvf'}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vf = await VFolderFactory(app).get(id=folder_info['id'])
        assert ret.status != 201
        assert vf.name != 'renamed-test-default-gvf'

    async def test_delete_gvfolder_by_domain_admin(self, prepare_vfolder, get_headers,
                                                   folder_mount, folder_host, folder_fsprefix,
                                                   default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)
        folder_path = (folder_mount / folder_host / folder_fsprefix / folder_info['id'])

        assert folder_path.exists()

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({'group': folder_info['group']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=default_domain_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 204
        assert not folder_path.exists()

    async def test_superadmin_cannot_delete_gvfolder(
            self, folder_host, prepare_vfolder, get_headers):
        app, client, create_vfolder = prepare_vfolder
        group = await GroupFactory(app).get(domain_name='default', name='default')

        # Create a vfolder.
        req_data = {
            'name': 'gvfolder-in-default-host-for-deletion-test-by-superadmin',
            'host': folder_host,
            'group': str(group.id),
        }
        vf_factory = VFolderFactory(app)
        await vf_factory.create(**req_data)
        vf = await vf_factory.get(name=req_data['name'], group=str(group.id))
        assert vf is not None

        url = f'/v3/folders/{req_data["name"]}'
        req_bytes = json.dumps({'group': str(group.id)}).encode()
        headers = get_headers('DELETE', url, req_bytes)
        await client.delete(url, data=req_bytes, headers=headers)

        vf = await vf_factory.get(name=req_data['name'], group=str(group.id))
        assert vf is not None

    async def test_cannot_delete_gvfolder_by_member(
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            default_domain_keypair, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)
        folder_path = (folder_mount / folder_host / folder_fsprefix / folder_info['id'])

        assert folder_path.exists()

        url = f'/v3/folders/{folder_info["name"]}'
        req_bytes = json.dumps({'group': folder_info['group']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 400
        assert folder_path.exists()


class TestFiles:

    @pytest.mark.asyncio
    async def test_upload_file(self, prepare_vfolder, get_headers, tmpdir,
                               folder_mount, folder_host, folder_fsprefix):
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
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert ret.status == 201
        assert 2 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()
        assert (folder_path / vf_fname2).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_other_users_vfolder(
            self, prepare_vfolder, get_headers, tmpdir,
            folder_mount, folder_host, folder_fsprefix,
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
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_download(self, prepare_vfolder, get_headers,
                            folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
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
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_list_files(self, prepare_vfolder, get_headers,
                              folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
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

    @pytest.mark.asyncio
    async def test_cannot_list_other_vfolder_files(
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_delete_files(self, prepare_vfolder, get_headers,
                                folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
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
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 404
        assert (folder_path / 'hello.txt').exists()


class TestFilesInGroupVFolder:

    @pytest.fixture(autouse=True)
    def set_vfolder_type_to_group(self):
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'put',
                         'config/volumes/_type', 'group'])
        yield
        subprocess.call(['python', '-m', 'ai.backend.manager.cli', 'etcd', 'delete',
                         'config/volumes/_type'])

    async def create_group_vfolder(self, app, create_vfolder, name=None,
                                   host=None, keypair=None):
        # Create a group vfolder with group_id of the default group.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == 'default')
                       .where(groups.c.name == 'default'))
            group_id = str(await conn.scalar(query))
        folder_info = await create_vfolder(group=group_id, name=name, host=host,
                                           keypair=keypair)
        return folder_info

    @pytest.mark.asyncio
    async def test_upload_file_by_domain_admin(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Create files and prepare form data
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')
        p2 = tmpdir.join('test2.txt')
        p2.write('35711')
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))
        data.add_field('file', open(p2, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=default_domain_keypair)
        ret = await client.post(url, data=data, headers=headers)

        # Get paths for files uploaded to virtual folder
        vf_fname1 = p1.strpath.split('/')[-1]
        vf_fname2 = p2.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert ret.status == 201
        assert 2 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()
        assert (folder_path / vf_fname2).exists()

    @pytest.mark.asyncio
    async def test_group_member_upload_to_gvfolder(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Create a file and prepare form data
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
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert ret.status == 201
        assert 1 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_to_gvfolder_in_other_group(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount,
            folder_host, folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        # Create file
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')

        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=user_in_other_group['keypair'])
        ret = await client.post(url, data=data, headers=headers)

        vf_fname1 = p1.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_to_gvfolder_in_other_domain_by_domain_admin(
            self, prepare_vfolder, get_headers, tmpdir, folder_mount,
            folder_host, folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        # Create domain admin in other domain's group.
        other_domain = await DomainFactory(app).create()
        other_group = await GroupFactory(app).create(domain_name=other_domain['name'])
        user_in_other_group = await UserFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        # Create file
        p1 = tmpdir.join('test1.txt')
        p1.write('1357')

        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', open(p1, 'rb'))

        # Upload the file
        url = f'/v3/folders/{folder_info["name"]}/upload'
        headers = get_headers('POST', url, b'', ctype='multipart/form-data',
                              keypair=user_in_other_group['keypair'])
        ret = await client.post(url, data=data, headers=headers)

        vf_fname1 = p1.strpath.split('/')[-1]
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_download_by_member(self, prepare_vfolder, get_headers, folder_mount,
                                      folder_host, folder_fsprefix,
                                      default_domain_keypair, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
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
    async def test_cannot_download_from_other_groups_vfolder(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_cannot_download_from_other_domain_admin(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        # Create domain admin in other domain's group.
        other_domain = await DomainFactory(app).create()
        other_group = await GroupFactory(app).create(domain_name=other_domain['name'])
        user_in_other_group = await UserFactory(app).create(role=UserRole.ADMIN)
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        url = f'/v3/folders/{folder_info["name"]}/download'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)

        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_list_gvfolder_files_by_member(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()
        files = json.loads(rsp_json['files'])

        assert files[0]['filename'] == 'hello.txt'

    @pytest.mark.asyncio
    async def test_cannot_list_other_groups_vfolder_files_by_member(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.get(url, data=req_bytes, headers=headers)
        assert ret.status == 404

    @pytest.mark.asyncio
    async def test_delete_files_by_member(self, prepare_vfolder, get_headers, folder_mount,
                                          folder_host, folder_fsprefix, default_domain_keypair,
                                          user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 200
        assert not (folder_path / 'hello.txt').exists()

    @pytest.mark.asyncio
    async def test_cannot_delete_other_groups_vfolder_files_by_member(
            self, prepare_vfolder, get_headers, folder_mount, folder_host,
            folder_fsprefix, default_domain_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await self.create_group_vfolder(app, create_vfolder,
                                                      keypair=default_domain_keypair)

        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        assert (folder_path / 'hello.txt').exists()

        # Create user in other group in default domain.
        user_in_other_group = await UserFactory(app).create()
        other_group = await GroupFactory(app).create()
        await AssociationGroupsUsersFactory(app).create(
            user_id=user_in_other_group['uuid'],
            group_id=other_group['id'])

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_in_other_group['keypair'])
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 404
        assert (folder_path / 'hello.txt').exists()


class TestInvitation:

    @pytest.mark.asyncio
    async def test_invite(self, prepare_vfolder, get_headers,
                          folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        url = f'/v3/folders/{folder_info["name"]}/invite'
        req_bytes = json.dumps({'perm': VFolderPermission.READ_WRITE,
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

        assert invitation.permission == VFolderPermission.READ_WRITE
        assert invitation.inviter == 'admin@lablup.com'
        assert invitation.state == 'pending'
        assert rsp_json['invited_ids'][0] == 'user@lablup.com'

    @pytest.mark.asyncio
    async def test_invitations(self, prepare_vfolder, get_headers,
                               folder_mount, folder_host, folder_fsprefix,
                               user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': uuid.uuid4().hex,
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
                'state': 'pending',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/list'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['invitations']) == 1
        assert rsp_json['invitations'][0]['inviter'] == 'admin@lablup.com'
        assert rsp_json['invitations'][0]['state'] == 'pending'

    @pytest.mark.asyncio
    async def test_list_sent_invitations(self, prepare_vfolder, get_headers,
                                         folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        inviter = 'admin@lablup.com'
        invitee = 'user@lablup.com'
        vfinv = await VFolderInvitationFactory(app).create(
            inviter=inviter, invitee=invitee,
            vfolder=folder_info['id'])

        url = f'/v3/folders/invitations/list_sent'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['invitations']) == 1
        assert rsp_json['invitations'][0]['vfolder_name'] == folder_info['name']
        assert rsp_json['invitations'][0]['inviter'] == inviter
        assert rsp_json['invitations'][0]['invitee'] == invitee
        assert uuid.UUID(rsp_json['invitations'][0]['vfolder_id']).hex == folder_info['id']

    @pytest.mark.asyncio
    async def test_update_invitation(self, prepare_vfolder, get_headers,
                                     folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        inviter = 'admin@lablup.com'
        invitee = 'user@lablup.com'
        vfinv = await VFolderInvitationFactory(app).create(
            inviter=inviter, invitee=invitee,
            permission=VFolderPermission('ro'),
            vfolder=folder_info['id'])

        url = f'/v3/folders/invitations/update/{vfinv["id"]}'
        req_bytes = json.dumps({'perm': 'rw'}).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vfinv = await VFolderInvitationFactory(app).get(id=vfinv['id'])
        assert ret.status == 200
        assert vfinv['permission'] == 'rw'

    @pytest.mark.asyncio
    async def test_list_received_invitations(self, prepare_vfolder, get_headers,
                                             folder_mount, folder_host, folder_fsprefix,
                                             user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        inviter = 'admin@lablup.com'
        invitee = 'user@lablup.com'
        vfinv = await VFolderInvitationFactory(app).create(
            inviter=inviter, invitee=invitee,
            vfolder=folder_info['id'])

        url = f'/v3/folders/invitations/list'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['invitations']) == 1
        assert rsp_json['invitations'][0]['vfolder_name'] == folder_info['name']
        assert rsp_json['invitations'][0]['inviter'] == inviter
        assert rsp_json['invitations'][0]['invitee'] == invitee
        assert uuid.UUID(rsp_json['invitations'][0]['vfolder_id']).hex == folder_info['id']

    @pytest.mark.asyncio
    async def test_not_list_finished_invitations(
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': uuid.uuid4().hex,
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
                'state': 'rejected',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/list'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['invitations']) == 0

    @pytest.mark.asyncio
    async def test_accept_invitation(self, prepare_vfolder, get_headers,
                                     folder_mount, folder_host, folder_fsprefix,
                                     user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        inv_id = uuid.uuid4().hex
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': inv_id,
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
                'state': 'pending',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/accept'
        req_bytes = json.dumps({'inv_id': inv_id}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            j = sa.join(vfolder_permissions, users,
                        vfolder_permissions.c.user == users.c.uuid)
            query = (sa.select([vfolder_permissions])
                       .select_from(j)
                       .where(users.c.email == 'user@lablup.com'))
            result = await conn.execute(query)
            perm = await result.first()

            query = (sa.select([vfolder_invitations])
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitations = await result.fetchall()

        assert ret.status == 201
        assert perm.permission == VFolderPermission.READ_ONLY
        assert len(invitations) == 1
        assert invitations[0]['state'] == 'accepted'

    @pytest.mark.asyncio
    async def test_cannot_accept_invitation_with_duplicated_vfolder_name(
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        inv_id = uuid.uuid4().hex
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': inv_id,
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
                'state': 'pending',
            }))
            await conn.execute(query)

            # Invitee already has vfolder with the same name as invitation.
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            row = await result.fetchone()
            query = (vfolders.insert().values({
                'id': uuid.uuid4().hex,
                'name': folder_info['name'],
                'host': 'local',
                'last_used': None,
                'user': row.user,
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/accept'
        req_bytes = json.dumps({'inv_id': inv_id}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            j = sa.join(vfolder_permissions, users,
                        vfolder_permissions.c.user == users.c.uuid)
            query = (sa.select([vfolder_permissions])
                       .select_from(j)
                       .where(users.c.email == 'user@lablup.com'))
            result = await conn.execute(query)
            perm = await result.first()

            query = (sa.select([vfolder_invitations])
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitations = await result.fetchall()

        assert ret.status == 400
        assert perm is None
        assert len(invitations) == 1
        assert invitations[0]['state'] == 'pending'

    @pytest.mark.asyncio
    async def test_delete_invitation(self, prepare_vfolder, get_headers,
                                     folder_mount, folder_host, folder_fsprefix,
                                     user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()

        inv_id = uuid.uuid4().hex
        async with app['dbpool'].acquire() as conn:
            query = (vfolder_invitations.insert().values({
                'id': inv_id,
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'inviter': 'admin@lablup.com',
                'invitee': 'user@lablup.com',
                'state': 'pending',
            }))
            await conn.execute(query)

        url = f'/v3/folders/invitations/delete'
        req_bytes = json.dumps({'inv_id': inv_id}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            j = sa.join(vfolder_permissions, users,
                        vfolder_permissions.c.user == users.c.uuid)
            query = (sa.select([vfolder_permissions])
                       .select_from(j)
                       .where(users.c.email == 'user@lablup.com'))
            result = await conn.execute(query)
            perms = await result.fetchall()

            query = (sa.select([vfolder_invitations])
                       .select_from(vfolder_invitations)
                       .where(vfolder_invitations.c.invitee == 'user@lablup.com'))
            result = await conn.execute(query)
            invitations = await result.fetchall()

        assert ret.status == 200
        assert len(perms) == 0
        assert len(invitations) == 1
        assert invitations[0]['state'] == 'rejected'

    @pytest.mark.asyncio
    async def test_list_shared_vfolders(self, prepare_vfolder, get_headers,
                                        folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        shared_to = await UserFactory(app).get(email='user@lablup.com')
        vfinv = await VFolderPermissionFactory(app).create(
            vfolder=folder_info['id'], user=shared_to['uuid'],
            permission=VFolderPermission('rw'))

        url = f'/v3/folders/_/shared'
        req_bytes = json.dumps({}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['shared']) == 1
        assert uuid.UUID(rsp_json['shared'][0]['vfolder_id']).hex == folder_info['id']
        assert rsp_json['shared'][0]['vfolder_name'] == folder_info['name']
        assert rsp_json['shared'][0]['shared_by'] == 'admin@lablup.com'
        assert rsp_json['shared'][0]['shared_to']['email'] == 'user@lablup.com'
        assert rsp_json['shared'][0]['perm'] == 'rw'

        # Request with target vfolder_id.
        url = f'/v3/folders/_/shared'
        req_bytes = json.dumps({'vfolder_id': folder_info['id']}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['shared']) == 1
        assert uuid.UUID(rsp_json['shared'][0]['vfolder_id']).hex == folder_info['id']
        assert rsp_json['shared'][0]['vfolder_name'] == folder_info['name']
        assert rsp_json['shared'][0]['shared_by'] == 'admin@lablup.com'
        assert rsp_json['shared'][0]['shared_to']['email'] == 'user@lablup.com'
        assert rsp_json['shared'][0]['perm'] == 'rw'

        # Request with invalid target vfolder_id.
        url = f'/v3/folders/_/shared'
        req_bytes = json.dumps({'vfolder_id': str(uuid.uuid4())}).encode()
        headers = get_headers('GET', url, req_bytes)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()

        assert len(rsp_json['shared']) == 0

    @pytest.mark.asyncio
    async def test_update_shared_vfolder(self, prepare_vfolder, get_headers,
                                         folder_mount, folder_host, folder_fsprefix):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()

        shared_to = await UserFactory(app).get(email='user@lablup.com')
        vfinv = await VFolderPermissionFactory(app).create(
            vfolder=folder_info['id'], user=shared_to['uuid'],
            permission=VFolderPermission('ro'))

        url = f'/v3/folders/_/shared'
        req_bytes = json.dumps({
            'vfolder': folder_info['id'],
            'user': str(shared_to['uuid']),
            'perm': 'rw',
        }).encode()
        headers = get_headers('POST', url, req_bytes)
        ret = await client.post(url, data=req_bytes, headers=headers)

        vfperm = await VFolderPermissionFactory(app).get(vfolder=folder_info['id'],
                                                         user=shared_to['uuid'])
        assert ret.status == 200
        assert vfperm['permission'] == 'rw'



class TestJoinedVfolderManipulations:

    @pytest.mark.asyncio
    async def test_cannot_create_vfolder_when_joined_vfolder_has_duplicated_name(
            self, prepare_vfolder, get_headers, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        # Create admin's vfolder.
        folder_info = await create_vfolder()

        # Create vfolder_permission.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
            }))
            await conn.execute(query)

        url = '/v3/folders/'
        req_bytes = json.dumps({'name': folder_info['name']}).encode()
        headers = get_headers('POST', url, req_bytes, keypair=user_keypair)
        ret = await client.post(url, data=req_bytes, headers=headers)

        async with app['dbpool'].acquire() as conn:
            query = (sa.select('*').select_from(vfolders))
            result = await conn.execute(query)

        assert ret.status == 400
        assert result.rowcount == 1

    @pytest.mark.asyncio
    async def test_list_vfolders(self, prepare_vfolder, get_headers, user_keypair):
        app, client, create_vfolder = prepare_vfolder

        # Create admin's vfolder.
        folder_info = await create_vfolder()

        # Create vfolder_permission.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
        assert rsp_json[0]['permission'] == VFolderPermission.READ_ONLY.value

    @pytest.mark.asyncio
    async def test_get_info(self, prepare_vfolder, get_headers, user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
        assert rsp_json['permission'] == VFolderPermission.READ_ONLY.value
        assert rsp_json['numFiles'] == 0

    @pytest.mark.asyncio
    async def test_upload_file(self, prepare_vfolder, get_headers, tmpdir,
                               folder_mount, folder_host, folder_fsprefix,
                               user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_WRITE,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert ret.status == 201
        assert 1 == len(list(folder_path.glob('**/*.txt')))
        assert (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_cannot_upload_file_to_read_only_vfolders(
            self, prepare_vfolder, get_headers, tmpdir,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])

        assert ret.status == 404
        assert not (folder_path / vf_fname1).exists()

    @pytest.mark.asyncio
    async def test_download(self, prepare_vfolder, get_headers,
                            folder_mount, folder_host, folder_fsprefix,
                            user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
    async def test_list_files(self, prepare_vfolder, get_headers,
                              folder_mount, folder_host, folder_fsprefix,
                              user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
            }))
            await conn.execute(query)

        url = f'/v3/folders/{folder_info["name"]}/files'
        req_bytes = json.dumps({'path': '.'}).encode()
        headers = get_headers('GET', url, req_bytes, keypair=user_keypair)
        ret = await client.get(url, data=req_bytes, headers=headers)
        rsp_json = await ret.json()
        files = json.loads(rsp_json['files'])

        assert files[0]['filename'] == 'hello.txt'

    @pytest.mark.asyncio
    async def test_delete_files(self, prepare_vfolder, get_headers,
                                folder_mount, folder_host, folder_fsprefix,
                                user_keypair):
        app, client, create_vfolder = prepare_vfolder
        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.RW_DELETE,
                'vfolder': folder_info['id'],
                'user': user_uuid,
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
            self, prepare_vfolder, get_headers,
            folder_mount, folder_host, folder_fsprefix,
            user_keypair):
        app, client, create_vfolder = prepare_vfolder

        folder_info = await create_vfolder()
        folder_path = (folder_mount / folder_host /
                       folder_fsprefix / folder_info['id'])
        with open(folder_path / 'hello.txt', 'w') as f:
            f.write('hello vfolder!')
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([keypairs.c.user])
                       .select_from(keypairs)
                       .where(keypairs.c.access_key == user_keypair['access_key']))
            result = await conn.execute(query)
            user_uuid = (await result.fetchone()).user
            query = (vfolder_permissions.insert().values({
                'permission': VFolderPermission.READ_ONLY,
                'vfolder': folder_info['id'],
                'user': user_uuid,
            }))
            await conn.execute(query)

        assert (folder_path / 'hello.txt').exists()

        url = f'/v3/folders/{folder_info["name"]}/delete_files'
        req_bytes = json.dumps({'files': ['hello.txt']}).encode()
        headers = get_headers('DELETE', url, req_bytes, keypair=user_keypair)
        ret = await client.delete(url, data=req_bytes, headers=headers)

        assert ret.status == 404
        assert (folder_path / 'hello.txt').exists()
