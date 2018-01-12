import json

import pytest

from ai.backend.gateway.admin import init as admin_init

USER_KEYPAIR = {
    'access_key': 'AKIANOTADMIN7EXAMPLE',
    'secret_key': 'C8qnIo29EZvXkPK_MXcuAakYTy4NYrxwmCEyNPlf',
}


@pytest.fixture
async def admin_server(pre_app):
    server = admin_init(pre_app)
    yield server


class TestAdminQuery:
    url = '/v3/admin/graphql'

    @pytest.mark.asyncio
    async def test_query_keypair(self, create_app_and_client, get_headers,
                                 default_keypair):
        app, client = await create_app_and_client(extras=['auth', 'admin'])

        query = '{ keypair { access_key secret_key is_active is_admin } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypair']['access_key'] == default_keypair['access_key']
        assert rsp_json['keypair']['secret_key'] == default_keypair['secret_key']
        assert rsp_json['keypair']['is_active']
        assert rsp_json['keypair']['is_admin']

    @pytest.mark.asyncio
    async def test_query_other_keypair(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(extras=['auth', 'admin'])

        query = '''{ keypair(access_key: "AKIANOTADMIN7EXAMPLE") {
    access_key secret_key is_active is_admin
} }'''
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypair']['access_key'] == USER_KEYPAIR['access_key']
        assert rsp_json['keypair']['secret_key'] == USER_KEYPAIR['secret_key']
        assert rsp_json['keypair']['is_active']
        assert not rsp_json['keypair']['is_admin']
