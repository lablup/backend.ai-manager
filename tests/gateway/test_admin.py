import json
import uuid

import pytest

from ai.backend.manager.models import (
    agents, kernels, vfolders, AgentStatus, KernelStatus
)
from ai.backend.common.types import ResourceSlot


class TestAdminQuery:
    url = '/v3/admin/graphql'

    @pytest.mark.asyncio
    async def test_query_agent(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agent info to db
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'available_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '0',
                }),
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '0',
                }),
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        query = '{ agent(agent_id: "test-agent-id") { status region addr } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['agent']['status'] == 'ALIVE'
        assert rsp_json['agent']['region'] == 'local'
        assert rsp_json['agent']['addr'] == '127.0.0.1'

    @pytest.mark.asyncio
    async def test_query_agents(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agents info to db (all ALIVE)
        async with app['dbpool'].acquire() as conn:
            for i in range(2):
                query = agents.insert().values({
                    'id': f'test-agent-id-{i}',
                    'status': AgentStatus.ALIVE,
                    'region': 'local',
                    'available_slots': ResourceSlot({
                        'cpu': '1',
                        'mem': '1073741824',
                    }),
                    'occupied_slots': ResourceSlot({
                        'cpu': '0',
                        'mem': '0',
                    }),
                    'addr': '127.0.0.1',
                    'lost_at': None,
                })
                await conn.execute(query)

        query = '{ agents { status region addr } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

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
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['agents']) == 0

    @pytest.mark.asyncio
    async def test_query_vfolders(self, create_app_and_client, get_headers,
                                  default_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test vfolder info to db
        async with app['dbpool'].acquire() as conn:
            query = vfolders.insert().values({
                'host': 'test-local-host',
                'name': 'test-vfolder',
                'belongs_to': default_keypair['access_key'],
            })
            await conn.execute(query)

        query = '{ vfolders { host name max_files max_size } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['vfolders'][0]['host'] == 'test-local-host'
        assert rsp_json['vfolders'][0]['name'] == 'test-vfolder'
        assert rsp_json['vfolders'][0]['max_files'] == 1000
        assert rsp_json['vfolders'][0]['max_size'] == 1048576

    @pytest.mark.asyncio
    async def test_query_compute_sessions(self, create_app_and_client, get_headers,
                                          default_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agent info to db (needed for foreign key for kernel)
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'available_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'occupied_slots': ResourceSlot({
                    'cpu': '0',
                    'mem': '0',
                }),
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        # Add test kernel info to db
        async with app['dbpool'].acquire() as conn:
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.RUNNING,
                'sess_id': 'test-sess-id',
                'role': 'master',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:5.3-alpine3.8',
                'registry': 'index.docker.io',
                'tag': 'test-tag',
                'access_key': default_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)

        query = '{ compute_sessions { sess_id role agent image tag } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['compute_sessions'][0]['sess_id'] == 'test-sess-id'
        assert rsp_json['compute_sessions'][0]['role'] == 'master'
        assert rsp_json['compute_sessions'][0]['agent'] == 'test-agent-id'
        assert rsp_json['compute_sessions'][0]['image'] == 'lablup/lua:5.3-alpine3.8'
        assert rsp_json['compute_sessions'][0]['tag'] == 'test-tag'

    @pytest.mark.asyncio
    async def test_query_compute_worker(self, create_app_and_client, get_headers,
                                        default_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agent info to db (needed for foreign key for kernel)
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'available_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'occupied_slots': ResourceSlot({
                    'cpu': '0',
                    'mem': '0',
                }),
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        async with app['dbpool'].acquire() as conn:
            # Add master kernel info to db
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.PREPARING,
                'sess_id': 'test-sess-id',
                'role': 'master',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:latest',
                'registry': 'index.docker.io',
                'tag': 'test-tag',
                'access_key': default_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)
            # Add worker
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.PREPARING,
                'sess_id': 'test-sess-id',
                'role': 'worker',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:latest',
                'registry': 'index.docker.io',
                'access_key': default_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)

        query = '{ compute_workers(sess_id: "test-sess-id") { role agent image } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json) == 1
        assert rsp_json['compute_workers'][0]['role'] == 'worker'


class TestUserQuery:
    url = '/v3/admin/graphql'

    @pytest.mark.asyncio
    async def test_query_vfolders(self, create_app_and_client, get_headers,
                                  user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test vfolder info to db
        async with app['dbpool'].acquire() as conn:
            query = vfolders.insert().values({
                'host': 'test-local-host',
                'name': 'test-vfolder',
                'belongs_to': user_keypair['access_key'],
            })
            await conn.execute(query)

        query = '{ vfolders { host name max_files max_size } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['vfolders'][0]['host'] == 'test-local-host'
        assert rsp_json['vfolders'][0]['name'] == 'test-vfolder'
        assert rsp_json['vfolders'][0]['max_files'] == 1000
        assert rsp_json['vfolders'][0]['max_size'] == 1048576
        # assert rsp_json['vfolders'][0]['belongs_to'] ==

    @pytest.mark.asyncio
    async def test_query_compute_sessions(self, create_app_and_client, get_headers,
                                          user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agent info to db (needed for foreign key for kernel)
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'available_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'occupied_slots': ResourceSlot({
                    'cpu': '0',
                    'mem': '0',
                }),
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        # Add test kernel info to db
        async with app['dbpool'].acquire() as conn:
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.PREPARING,
                'sess_id': 'test-sess-id',
                'role': 'master',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:latest',
                'registry': 'index.docker.io',
                'tag': 'test-tag',
                'access_key': user_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)

        query = '{ compute_sessions { sess_id role agent image tag } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['compute_sessions'][0]['sess_id'] == 'test-sess-id'
        assert rsp_json['compute_sessions'][0]['role'] == 'master'
        assert rsp_json['compute_sessions'][0]['agent'] == 'test-agent-id'
        assert rsp_json['compute_sessions'][0]['image'] == 'lablup/lua:latest'
        assert rsp_json['compute_sessions'][0]['tag'] == 'test-tag'

    @pytest.mark.asyncio
    async def test_query_compute_worker(self, create_app_and_client, get_headers,
                                        user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # Add test agent info to db (needed for foreign key for kernel)
        async with app['dbpool'].acquire() as conn:
            query = agents.insert().values({
                'id': 'test-agent-id',
                'status': AgentStatus.ALIVE,
                'region': 'local',
                'available_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'occupied_slots': ResourceSlot({
                    'cpu': '0',
                    'mem': '0',
                }),
                'addr': '127.0.0.1',
                'lost_at': None,
            })
            await conn.execute(query)

        async with app['dbpool'].acquire() as conn:
            # Add master kernel info to db
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.PREPARING,
                'sess_id': 'test-sess-id',
                'role': 'master',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:latest',
                'tag': 'test-tag',
                'access_key': user_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)
            # Add worker
            query = kernels.insert().values({
                'id': uuid.uuid4(),
                'status': KernelStatus.PREPARING,
                'sess_id': 'test-sess-id',
                'role': 'worker',
                'agent': 'test-agent-id',
                'agent_addr': '127.0.0.1:5002',
                'image': 'lablup/lua:latest',
                'access_key': user_keypair['access_key'],
                'occupied_slots': ResourceSlot({
                    'cpu': '1',
                    'mem': '1073741824',
                }),
                'environ': [],
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            })
            await conn.execute(query)

        query = '{ compute_workers(sess_id: "test-sess-id") { role agent image } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json) == 1
        assert rsp_json['compute_workers'][0]['role'] == 'worker'
