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
        assert rsp_json['user']['role'] == 'superadmin'

    async def test_query_other_user(self, create_app_and_client, get_headers):
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

    async def test_domain_admin_cannot_query_user_in_other_domain(
            self, create_app_and_client, get_headers, default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create new domain and user in the domain.
        async with app['dbpool'].acquire() as conn, conn.begin():
            from ai.backend.manager.models.domain import domains  # noqa
            from ai.backend.manager.models.user import users  # noqa
            domain_data = {
                'name': 'domain-for-testing-user-query-in-other-domain',
                'total_resource_slots': {},
            }
            user_data = {
                'email': 'test-user-in-other-domain-for-user-query@test.com',
                'domain_name': domain_data['name'],
            }
            query = domains.insert().values(domain_data)
            await conn.execute(query)
            query = users.insert().values(user_data)
            await conn.execute(query)

        query = textwrap.dedent('''\
        {
            user(email: "%s") {
                username email password need_password_change is_active role
            }
        }''' % user_data['email'])
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert rsp_json['user'] is None

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

    @pytest.mark.asyncio
    async def test_domain_admin_cannot_query_users_in_other_domain(
            self, create_app_and_client, get_headers, default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create new domain and user in the domain.
        async with app['dbpool'].acquire() as conn, conn.begin():
            from ai.backend.manager.models.domain import domains  # noqa
            from ai.backend.manager.models.user import users  # noqa
            domain_data = {
                'name': 'domain-for-testing-users-query-in-other-domain',
                'total_resource_slots': {},
            }
            user_data = {
                'email': 'test-user-in-other-domain-for-users-query@test.com',
                'domain_name': domain_data['name'],
            }
            query = domains.insert().values(domain_data)
            await conn.execute(query)
            query = users.insert().values(user_data)
            await conn.execute(query)

        # Query users by domain admin.
        query = '{ users { username email password need_password_change is_active role} }'
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200
        rsp_json = await ret.json()
        assert user_data['email'] not in [item['email'] for item in rsp_json['users']]
        assert 'user@lablup.com' in [item['email'] for item in rsp_json['users']]

    async def test_mutate_user(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a user.
        email = 'newuser@lablup.com'
        query = textwrap.dedent('''\
        mutation($email: String!, $input: UserInput!) {
            create_user(email: $email, props: $input) {
                ok msg user { username email password full_name description is_active domain_name role }
            }
        }''')
        variables = {
            'email': email,
            'input': {
                'username': 'newuser',
                'password': 'new-password',
                'need_password_change': False,
                'full_name': 'First Last',
                'description': 'New user',
                'is_active': True,
                'domain_name': 'default',
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
        assert rsp_json['create_user']['user']['domain_name'] == 'default'
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
                'full_name': 'First Last-mod',
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
        assert rsp_json['modify_user']['user']['full_name'] == 'First Last-mod'
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

    async def test_domain_admin_cannot_mutate_user(
            self, create_app_and_client, get_headers, default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a user.
        email = 'newuser-by-domain-admin@lablup.com'
        query = textwrap.dedent('''\
        mutation($email: String!, $input: UserInput!) {
            create_user(email: $email, props: $input) {
                ok msg user { username email password full_name description is_active domain_name role }
            }
        }''')
        variables = {
            'email': email,
            'input': {
                'username': 'newuser',
                'password': 'new-password',
                'need_password_change': False,
                'full_name': 'First Last',
                'description': 'New user',
                'is_active': True,
                'domain_name': 'default',
                'role': 'monitor',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status != 200

    async def test_new_users_password_is_hashed(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a user.
        email = 'newuser2@lablup.com'
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
                'username': 'newuser2',
                'password': password,
                'need_password_change': False,
                'full_name': 'First Last',
                'description': 'New user',
                'is_active': True,
                'domain_name': 'default',
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
