import json
import textwrap

import pytest


@pytest.mark.asyncio
class TestDomainAdminQuery:
    url = '/v3/admin/graphql'

    async def test_query_domain(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            domain(name: "default") {
                name is_active total_resource_slots
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['domain']['name'] == 'default'
        assert rsp_json['domain']['is_active']
        assert rsp_json['domain']['total_resource_slots'] == '{}'

    @pytest.mark.asyncio
    async def test_query_domains(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = '{ domains { name is_active total_resource_slots } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' in [item['name'] for item in rsp_json['domains']]

        # With is_active flag
        query = textwrap.dedent('''\
        query($is_active: Boolean) {
            domains(is_active: $is_active) {
                name is_active total_resource_slots
            }
        }''')
        variables = {'is_active': False}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' not in [item['name'] for item in rsp_json['domains']]

    async def test_mutate_domain(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain.
        domain_name = 'new-domain'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'description': 'desc',
                'is_active': True,
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['create_domain']['domain']['name'] == 'new-domain'
        assert rsp_json['create_domain']['domain']['description'] == 'desc'
        assert rsp_json['create_domain']['domain']['is_active']
        assert rsp_json['create_domain']['domain']['total_resource_slots'] == '{"cpu": "0", "mem": "0"}'

        # Update the domain.
        query = textwrap.dedent('''\
        mutation($name: String!, $input: ModifyDomainInput!) {
            modify_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'name': 'new-domain-mod',
                'description': 'New domain-mod',
                'is_active': False,
                'total_resource_slots': '{"cpu": "1", "mem": "1"}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['modify_domain']['domain']['name'] == 'new-domain-mod'
        assert rsp_json['modify_domain']['domain']['description'] == 'New domain-mod'
        assert not rsp_json['modify_domain']['domain']['is_active']
        assert rsp_json['modify_domain']['domain']['total_resource_slots'] == '{"cpu": "1", "mem": "1"}'

        # Delete the domain.
        domain_name = rsp_json['modify_domain']['domain']['name']
        query = textwrap.dedent('''\
        mutation($name: String!) {
            delete_domain(name: $name) { ok msg }
        }''')
        variables = {'name': domain_name}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200

    async def test_name_should_be_slugged(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Try to create a domain with space in name.
        domain_name = 'new domain'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'description': 'desc',
                'is_active': True,
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status != 200


@pytest.mark.asyncio
class TestUserUserQuery:
    url = '/v3/admin/graphql'
    # No User Queries currently.
