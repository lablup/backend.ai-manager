import json
import textwrap

import pytest


@pytest.mark.asyncio
class TestKeyPairAdminQuery:
    url = '/v3/admin/graphql'

    async def test_query_keypair(self, create_app_and_client, get_headers,
                                 default_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        query = '{ keypair { access_key secret_key is_active is_admin } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypair']['access_key'] == default_keypair['access_key']
        assert rsp_json['keypair']['secret_key'] == default_keypair['secret_key']
        assert rsp_json['keypair']['is_active']
        assert rsp_json['keypair']['is_admin']

    async def test_query_other_keypair(self, create_app_and_client, get_headers,
                                       user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            keypair(access_key: "AKIANABBDUSEREXAMPLE") {
                access_key secret_key is_active is_admin
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypair']['access_key'] == user_keypair['access_key']
        assert rsp_json['keypair']['secret_key'] == user_keypair['secret_key']
        assert rsp_json['keypair']['is_active']
        assert not rsp_json['keypair']['is_admin']

    @pytest.mark.asyncio
    async def test_query_keypairs(self, create_app_and_client, get_headers,
                                  default_keypair, user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # directly embed value inside the query
        query = '{ keypairs(user_id: "admin@lablup.com") { access_key secret_key } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['keypairs']) == 1
        assert rsp_json['keypairs'][0]['access_key'] == default_keypair['access_key']
        assert rsp_json['keypairs'][0]['secret_key'] == default_keypair['secret_key']

        # use a parametrized query.
        query = 'query($uid: String!) {\n' \
                '  keypairs(user_id: $uid) { access_key secret_key }\n' \
                '}'
        variables = {'uid': 'user@lablup.com'}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['keypairs']) == 1
        assert rsp_json['keypairs'][0]['access_key'] == user_keypair['access_key']
        assert rsp_json['keypairs'][0]['secret_key'] == user_keypair['secret_key']

    async def test_query_keypairs_list_other_keypair_as_well(self, create_app_and_client, get_headers,
                                                             default_keypair, user_keypair,
                                                             monitor_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # List all keypairs
        query = '{ keypairs { access_key secret_key } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert default_keypair['access_key'] in [item['access_key'] for item in rsp_json['keypairs']]
        assert user_keypair['access_key'] in [item['access_key'] for item in rsp_json['keypairs']]
        assert monitor_keypair['access_key'] in [item['access_key'] for item in rsp_json['keypairs']]

    async def test_mutate_keypair(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a keypair.
        email = 'user@lablup.com'
        query = textwrap.dedent('''\
        mutation($user_id: String!, $input: KeyPairInput!) {
            create_keypair(user_id: $user_id, props: $input) {
                ok msg keypair { user_id access_key is_active is_admin rate_limit user }
            }
        }''')
        variables = {
            'user_id': email,
            'input': {
                'is_active': True,
                'is_admin': False,
                'resource_policy': 'default',
                'rate_limit': 500,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        access_key = rsp_json['create_keypair']['keypair']['access_key']

        assert ret.status == 200
        assert rsp_json['create_keypair']['keypair']['user_id'] == email
        assert rsp_json['create_keypair']['keypair']['is_active']
        assert not rsp_json['create_keypair']['keypair']['is_admin']
        assert rsp_json['create_keypair']['keypair']['rate_limit'] == 500

        # Update the keypair.
        query = textwrap.dedent('''\
        mutation($access_key: String!, $input: ModifyKeyPairInput!) {
            modify_keypair(access_key: $access_key, props: $input) {
                ok msg
            }
        }''')
        variables = {
            'access_key': access_key,
            'input': {
                'is_active': False,
                'is_admin': True,
                'rate_limit': 1000,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200

        # Check if updated.
        query = textwrap.dedent('''\
        {
            keypair(access_key: "%s") {
                access_key is_active is_admin rate_limit
            }
        }''' % (access_key))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert not rsp_json['keypair']['is_active']
        assert rsp_json['keypair']['is_admin']
        assert rsp_json['keypair']['rate_limit'] == 1000

        # Delete the keypair.
        query = textwrap.dedent('''\
        mutation($access_key: String!) {
            delete_keypair(access_key: $access_key) { ok msg }
        }''')
        variables = {'access_key': access_key}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200


@pytest.mark.asyncio
class TestKeyPairUserQuery:
    url = '/v3/admin/graphql'

    @pytest.mark.asyncio
    async def test_query_keypair(self, create_app_and_client, get_headers,
                                 user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        query = '{ keypair { access_key secret_key is_active is_admin } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['keypair']['access_key'] == user_keypair['access_key']
        assert rsp_json['keypair']['secret_key'] == user_keypair['secret_key']
        assert rsp_json['keypair']['is_active']
        assert not rsp_json['keypair']['is_admin']

    async def test_cannot_query_other_keypair(self, create_app_and_client, get_headers,
                                              user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            keypair(access_key: "AKIAIOSFODNN7EXAMPLE") {
                access_key secret_key is_active is_admin
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 400

    @pytest.mark.asyncio
    async def test_query_keypairs(self, create_app_and_client, get_headers,
                                  user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # User cannot query admin's keypairs
        query = '{ keypairs(user_id: "admin@lablup.com") { access_key secret_key } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 400

        # User can query his/her own keypairs
        query = 'query($uid: String!) {\n' \
                '  keypairs(user_id: $uid) { access_key secret_key }\n' \
                '}'
        variables = {'uid': 'user@lablup.com'}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['keypairs']) == 1
        assert rsp_json['keypairs'][0]['access_key'] == user_keypair['access_key']
        assert rsp_json['keypairs'][0]['secret_key'] == user_keypair['secret_key']

    async def test_query_keypairs_do_not_list_other_keypair(
            self, create_app_and_client, get_headers, user_keypair):
        app, client = await create_app_and_client(
            modules=['auth', 'admin', 'manager'])

        # List all keypairs returns user's keypairs only
        query = '{ keypairs { access_key secret_key } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert len(rsp_json['keypairs']) == 1
        assert user_keypair['access_key'] == rsp_json['keypairs'][0]['access_key']
