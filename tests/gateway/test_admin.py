import json

import pytest

from ai.backend.manager.models import agents, AgentStatus

USER_KEYPAIR = {
    'access_key': 'AKIANOTADMIN7EXAMPLE',
    'secret_key': 'C8qnIo29EZvXkPK_MXcuAakYTy4NYrxwmCEyNPlf',
}


class TestAdminQuery:
    url = '/v3/admin/graphql'

    @pytest.mark.asyncio
    async def test_query_agent(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(extras=['auth', 'admin'])

        # Add test agent info to db
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'mem_slots': 1,
                'cpu_slots': 1,
                'gpu_slots': 0,
                'used_mem_slots': 0,
                'used_cpu_slots': 0,
                'used_gpu_slots': 0,
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        query = '{ agent(agent_id: "test-agent-id") { status region addr } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['agent']['status'] == 'ALIVE'
        assert rsp_json['agent']['region'] == 'local'
        assert rsp_json['agent']['addr'] == '127.0.0.1'

    @pytest.mark.asyncio
    async def test_query_agents(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(extras=['auth', 'admin'])

        # Add test agents info to db (all ALIVE)
        async with app['dbpool'].acquire() as conn:
            for i in range(2):
                query = agents.insert().values({
                    'id': f'test-agent-id-{i}',
                    'status': AgentStatus.ALIVE,
                    'region': 'local',
                    'mem_slots': 1,
                    'cpu_slots': 1,
                    'gpu_slots': 0,
                    'used_mem_slots': 0,
                    'used_cpu_slots': 0,
                    'used_gpu_slots': 0,
                    'addr': '127.0.0.1',
                    'lost_at': None,
                })
                await conn.execute(query)

        query = '{ agents { status region addr } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['agents'][0]['status'] == 'ALIVE'
        assert rsp_json['agents'][0]['region'] == 'local'
        assert rsp_json['agents'][0]['addr'] == '127.0.0.1'
        assert rsp_json['agents'][1]['status'] == 'ALIVE'
        assert rsp_json['agents'][1]['region'] == 'local'
        assert rsp_json['agents'][1]['addr'] == '127.0.0.1'

        # query with status
        query = '{ agents(status: "LOST") { status region addr } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['agents']) == 0

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

    @pytest.mark.asyncio
    async def test_query_keypairs(self, create_app_and_client, get_headers,
                                  default_keypair):
        app, client = await create_app_and_client(extras=['auth', 'admin'])

        query = '{ keypairs(user_id: 2) { access_key secret_key } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypairs'][0]['access_key'] == default_keypair['access_key']
        assert rsp_json['keypairs'][0]['secret_key'] == default_keypair['secret_key']

        query = '{ keypairs(user_id: 3) { access_key secret_key } }'
        req_bytes = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, req_bytes)
        ret = await client.post(self.url, data=req_bytes, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypairs'][0]['access_key'] == USER_KEYPAIR['access_key']
        assert rsp_json['keypairs'][0]['secret_key'] == USER_KEYPAIR['secret_key']
