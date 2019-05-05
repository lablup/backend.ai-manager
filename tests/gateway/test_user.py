import json
import textwrap

import pytest


@pytest.mark.asyncio
class TestUserAdminQuery:
    url = '/v3/admin/graphql'

    async def test_query_user(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = '{ user { username email password need_password_change is_active role } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['user']['email'] == 'admin@lablup.com'
        assert rsp_json['user']['password'] is None  # password should not be delivered
        assert not rsp_json['user']['need_password_change']
        assert rsp_json['user']['is_active']
        assert rsp_json['user']['role'] == 'admin'

    async def test_query_other_user(self, create_app_and_client, get_headers, user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            user(email: "user@lablup.com") {
                username email password need_password_change is_active role
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['user']['email'] == 'user@lablup.com'
        assert rsp_json['user']['password'] is None  # password should not be delivered
        assert not rsp_json['user']['need_password_change']
        assert rsp_json['user']['is_active']
        assert rsp_json['user']['role'] == 'user'

    @pytest.mark.asyncio
    async def test_query_users(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = '{ users { username email password need_password_change is_active role} }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert 'admin@lablup.com' in [item['email'] for item in rsp_json['users']]
        assert 'user@lablup.com' in [item['email'] for item in rsp_json['users']]

        # With is_active flag
        query = textwrap.dedent('''\
        query($is_active: Boolean) {
            users(is_active: $is_active) {
                username email password need_password_change is_active role
            }
        }''')
        variables = {'is_active': False}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert 'admin@lablup.com' not in [item['email'] for item in rsp_json['users']]
        assert 'user@lablup.com' not in [item['email'] for item in rsp_json['users']]

    async def test_mutate_user(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a user.
        email = 'newuser@lablup.com'
        query = textwrap.dedent('''\
        mutation($email: String!, $input: UserInput!) {
            create_user(email: $email, props: $input) {
                ok msg user { username email password full_name description is_active role }
            }
        }''')
        variables = {
            'email': email,
            'input': {
                'username': 'newuser',
                'password': 'new-password',
                'need_password_change': False,
                'first_name': 'First',
                'last_name': 'Last',
                'description': 'New user',
                'is_active': True,
                'role': 'monitor',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['create_user']['user']['username'] == 'newuser'
        assert rsp_json['create_user']['user']['email'] == email
        assert rsp_json['create_user']['user']['password'] is None
        assert rsp_json['create_user']['user']['full_name'] == 'First Last'
        assert rsp_json['create_user']['user']['description'] == 'New user'
        assert rsp_json['create_user']['user']['is_active']
        assert rsp_json['create_user']['user']['role'] == 'monitor'

        # Update the user.
        query = textwrap.dedent('''\
        mutation($email: String!, $input: ModifyUserInput!) {
            modify_user(email: $email, props: $input) {
                ok msg user { username email password full_name description is_active role }
            }
        }''')
        variables = {
            'email': email,
            'input': {
                'username': 'newuser-mod',
                'password': 'new-password-mod',
                'need_password_change': True,
                'first_name': 'First-mod',
                'last_name': 'Last-mod',
                'description': 'New user-mod',
                'is_active': False,
                'role': 'user',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['modify_user']['user']['username'] == 'newuser-mod'
        assert rsp_json['modify_user']['user']['email'] == email
        assert rsp_json['modify_user']['user']['password'] is None
        assert rsp_json['modify_user']['user']['full_name'] == 'First-mod Last-mod'
        assert rsp_json['modify_user']['user']['description'] == 'New user-mod'
        assert not rsp_json['modify_user']['user']['is_active']
        assert rsp_json['modify_user']['user']['role'] == 'user'

        # Delete the user.
        query = textwrap.dedent('''\
        mutation($email: String!) {
            delete_user(email: $email) { ok msg }
        }''')
        variables = {'email': email}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200

    async def test_new_users_password_is_hashed(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a user.
        email = 'newuser@lablup.com'
        password = 'new-password'
        query = textwrap.dedent('''\
        mutation($email: String!, $input: UserInput!) {
            create_user(email: $email, props: $input) {
                ok msg user { password }
            }
        }''')
        variables = {
            'email': email,
            'input': {
                'username': 'newuser',
                'password': password,
                'need_password_change': False,
                'first_name': 'First',
                'last_name': 'Last',
                'description': 'New user',
                'is_active': True,
                'role': 'monitor',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['create_user']['user']['password'] is None
        async with app['dbpool'].acquire() as conn, conn.begin():
            from ai.backend.manager.models.user import users  # noqa
            query = users.select(users.c.email == email)
            result = await conn.execute(query)
            obj = await result.first()
        assert obj['password'] != password

    async def test_check_password(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Check pasword with incorrect password
        query = textwrap.dedent('''\
        {
            user_check_password(email: "admin@lablup.com", password: "1") {
                password_correct
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert not rsp_json['user_check_password']['password_correct']

        # Check pasword with correct password
        query = textwrap.dedent('''\
        {
            user_check_password(email: "admin@lablup.com", password: "0") {
                password_correct
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert rsp_json['user_check_password']['password_correct']


@pytest.mark.asyncio
class TestUserUserQuery:
    url = '/v3/admin/graphql'

    async def test_query_user(self, create_app_and_client, get_headers, user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = '{ user { username email password need_password_change is_active role } }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['user']['email'] == 'user@lablup.com'
        assert rsp_json['user']['password'] is None  # password should not be delivered
        assert not rsp_json['user']['need_password_change']
        assert rsp_json['user']['is_active']
        assert rsp_json['user']['role'] == 'user'

    async def test_cannot_query_other_user(self, create_app_and_client, get_headers, user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            user(email: "admin@lablup.com") {
                username email password need_password_change is_active role
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 400

    async def test_check_password(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Check pasword with incorrect password
        query = textwrap.dedent('''\
        {
            user_check_password(email: "user@lablup.com", password: "1") {
                password_correct
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert not rsp_json['user_check_password']['password_correct']

        # Check pasword with correct password
        query = textwrap.dedent('''\
        {
            user_check_password(email: "user@lablup.com", password: "0") {
                password_correct
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert rsp_json['user_check_password']['password_correct']
