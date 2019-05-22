import json
import textwrap

import pytest
import sqlalchemy as sa

from ai.backend.manager.models import association_groups_users, groups


@pytest.mark.asyncio
class TestGroupAdminQuery:
    url = '/v3/admin/graphql'

    async def test_query_group_by_id(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        async with app['dbpool'].acquire() as conn:
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.name == 'default'))
            result = await conn.execute(query)
            row = await result.fetchone()
            gid = row.id

        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group']['name'] == 'default'
        assert rsp_json['group']['is_active']
        assert rsp_json['group']['domain_name'] == 'default'

    async def test_cannot_query_group_in_other_domain(self, create_app_and_client, get_headers,
                                                      default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain and where requester does not belong to.
        domain_name = 'other-domain-to-test-query-group-by-id'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Create a group in other domain.
        group_name = 'group-in-other-domain-to-test-query-group-by-id'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'domain_name': domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        gid = rsp_json['create_group']['group']['id']

        # Check if query the group by global admin
        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group'] is not None

        # Check if query other group by id in other domain.
        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group'] is None

    async def test_query_groups_with_domain_name(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            groups(domain_name: "default") {
                name description is_active domain_name
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' in [item['name'] for item in rsp_json['groups']]

        # With is_active flag
        query = textwrap.dedent('''\
        {
            groups(domain_name: "default", is_active=False) {
                name description is_active domain_name
            }
        }''')
        query = textwrap.dedent('''\
        query($domain_name: String!, $is_active: Boolean) {
            groups(domain_name: $domain_name, is_active: $is_active) {
                name description is_active domain_name
            }
        }''')
        variables = {
            'domain_name': 'default',
            'is_active': False,
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' not in [item['name'] for item in rsp_json['groups']]

    async def test_cannot_query_groups_in_other_domain(self, create_app_and_client, get_headers,
                                                       default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain and where requester does not belong to.
        new_domain_name = 'other-domain-to-test-query-groups'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': new_domain_name,
            'input': {
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Create a group in other domain.
        new_group_name = 'group-in-other-domain-to-test-query-groups'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': new_group_name,
            'input': {
                'domain_name': new_domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        new_gid = rsp_json['create_group']['group']['id']

        query = textwrap.dedent('''\
        {
            groups(domain_name: "default") {
                id name description is_active domain_name
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert new_gid not in [item['id'] for item in rsp_json['groups']]

    async def test_mutate_group_by_domain_admin(self, create_app_and_client, get_headers,
                                                default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a group.
        group_name = 'new-group-mutate-test'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'description': 'desc',
                'is_active': True,
                'domain_name': 'default',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['create_group']['group']['name'] == 'new-group-mutate-test'
        assert rsp_json['create_group']['group']['description'] == 'desc'
        assert rsp_json['create_group']['group']['is_active']
        assert rsp_json['create_group']['group']['domain_name'] == 'default'

        # Update the group.
        gid = rsp_json['create_group']['group']['id']
        query = textwrap.dedent('''\
        mutation($gid: String!, $input: ModifyGroupInput!) {
            modify_group(gid: $gid, props: $input) {
                ok msg group { name description is_active domain_name }
            }
        }''')
        variables = {
            'gid': gid,
            'input': {
                'name': 'new-group-mutate-test-mod',
                'description': 'New group-mod',
                'is_active': False,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['modify_group']['group']['name'] == 'new-group-mutate-test-mod'
        assert rsp_json['modify_group']['group']['description'] == 'New group-mod'
        assert not rsp_json['modify_group']['group']['is_active']

        # Delete the group.
        query = textwrap.dedent('''\
        mutation($gid: String!) {
            delete_group(gid: $gid) { ok msg }
        }''')
        variables = {'gid': gid}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 200

    async def test_cannot_mutate_group_in_other_domain(self, create_app_and_client, get_headers,
                                                       default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain and where requester does not belong to.
        domain_name = 'other-domain-to-test-mutate-group'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Try to create a group in other domain.
        group_name = 'new-group-in-other-domain-mutate-test'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'is_active': True,
                'domain_name': domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 400

        # Create a group in other domain by global admin.
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'is_active': True,
                'domain_name': domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        # Try to update the group.
        gid = rsp_json['create_group']['group']['id']
        query = textwrap.dedent('''\
        mutation($gid: String!, $input: ModifyGroupInput!) {
            modify_group(gid: $gid, props: $input) {
                ok msg group { name description is_active domain_name }
            }
        }''')
        variables = {
            'gid': gid,
            'input': {
                'name': 'new-group-mutate-test-mod',
                'description': 'New group-mod',
                'is_active': False,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 400

        # Try to delete the group.
        query = textwrap.dedent('''\
        mutation($gid: String!) {
            delete_group(gid: $gid) { ok msg }
        }''')
        variables = {'gid': gid}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status == 400
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([groups])
                       .select_from(groups)
                       .where(groups.c.id == gid))
            result = await conn.execute(query)
            row = await result.fetchone()
        assert row is not None

    async def test_name_should_be_slugged(self, create_app_and_client, get_headers):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Try to create a group with space in name.
        group_name = 'new group'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'description': 'desc',
                'is_active': True,
                'domain_name': 'default',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)

        assert ret.status != 200

    async def test_mutate_group_with_users(self, create_app_and_client, get_headers,
                                           default_domain_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a group w/o any users.
        group_name = 'new-group-mutate-with-users'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'description': 'desc',
                'is_active': True,
                'domain_name': 'default',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        gid = rsp_json['create_group']['group']['id']

        assert ret.status == 200

        # Add users to the group.
        async with app['dbpool'].acquire() as conn:
            from ai.backend.manager.models import users
            query = (sa.select([users.c.uuid])
                       .select_from(users)
                       .where((users.c.email == 'user@lablup.com') |
                              (users.c.email == 'monitor@lablup.com')))
            result = await conn.execute(query)
            rows = await result.fetchall()
            user_uuids = [str(row.uuid) for row in rows]
        query = textwrap.dedent('''\
        mutation($gid: String!, $input: ModifyGroupInput!) {
            modify_group(gid: $gid, props: $input) {
                ok msg group { name description is_active domain_name }
            }
        }''')
        variables = {
            'gid': gid,
            'input': {
                'user_update_mode': 'add',
                'user_uuids': user_uuids,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Check association between the group and the users are created.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([association_groups_users.c.user_id])
                       .select_from(association_groups_users)
                       .where(association_groups_users.c.group_id == gid))
            result = await conn.execute(query)
            rows = await result.fetchall()
        assert set(user_uuids) == set([str(item[0]) for item in rows])

        # Remove one user from the group.
        query = textwrap.dedent('''\
        mutation($gid: String!, $input: ModifyGroupInput!) {
            modify_group(gid: $gid, props: $input) {
                ok msg group { name description is_active domain_name }
            }
        }''')
        variables = {
            'gid': gid,
            'input': {
                'user_update_mode': 'remove',
                'user_uuids': [user_uuids[0]],
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Check association is removed.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([association_groups_users.c.user_id])
                       .select_from(association_groups_users)
                       .where(association_groups_users.c.group_id == gid))
            result = await conn.execute(query)
            rows = await result.fetchall()
        assert set([user_uuids[1]]) == set([str(item[0]) for item in rows])

        # Delete the group.
        query = textwrap.dedent('''\
        mutation($gid: String!) {
            delete_group(gid: $gid) { ok msg }
        }''')
        variables = {'gid': gid}
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=default_domain_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Check remaining association is removed.
        async with app['dbpool'].acquire() as conn:
            query = (sa.select([association_groups_users.c.user_id])
                       .select_from(association_groups_users)
                       .where(association_groups_users.c.group_id == gid))
            result = await conn.execute(query)
            rows = await result.fetchall()
        assert len(rows) == 0


@pytest.mark.asyncio
class TestGroupUserQuery:
    url = '/v3/admin/graphql'

    async def test_query_group_by_id(self, create_app_and_client, get_headers, user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        async with app['dbpool'].acquire() as conn:
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.name == 'default'))
            result = await conn.execute(query)
            row = await result.fetchone()
            gid = row.id

        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group']['name'] == 'default'
        assert rsp_json['group']['is_active']
        assert rsp_json['group']['domain_name'] == 'default'

    async def test_cannot_query_group_in_other_domain(self, create_app_and_client, get_headers,
                                                      user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain and where requester does not belong to.
        domain_name = 'other-domain-to-test-user-query-group-by-id'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': domain_name,
            'input': {
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Create a group in other domain.
        group_name = 'group-in-other-domain-to-test-user-query-group-by-id'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': group_name,
            'input': {
                'domain_name': domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        gid = rsp_json['create_group']['group']['id']

        # Check if query the group by global admin
        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group'] is not None

        # Check if query other group by id in other domain.
        query = textwrap.dedent('''\
        {
            group(id: "%s") {
                name description is_active domain_name
            }
        }''' % (gid))
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert rsp_json['group'] is None

    async def test_query_groups_with_domain_name(self, create_app_and_client, get_headers,
                                                 user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        query = textwrap.dedent('''\
        {
            groups(domain_name: "default") {
                name description is_active domain_name
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' in [item['name'] for item in rsp_json['groups']]

        # With is_active flag
        query = textwrap.dedent('''\
        {
            groups(domain_name: "default", is_active=False) {
                name description is_active domain_name
            }
        }''')
        query = textwrap.dedent('''\
        query($domain_name: String!, $is_active: Boolean) {
            groups(domain_name: $domain_name, is_active: $is_active) {
                name description is_active domain_name
            }
        }''')
        variables = {
            'domain_name': 'default',
            'is_active': False,
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert 'default' not in [item['name'] for item in rsp_json['groups']]

    async def test_cannot_query_groups_in_other_domain(self, create_app_and_client, get_headers,
                                                       user_keypair):
        app, client = await create_app_and_client(modules=['auth', 'admin', 'manager'])

        # Create a domain and where requester does not belong to.
        new_domain_name = 'other-domain-to-test-user-query-groups'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: DomainInput!) {
            create_domain(name: $name, props: $input) {
                ok msg domain { name description is_active total_resource_slots }
            }
        }''')
        variables = {
            'name': new_domain_name,
            'input': {
                'total_resource_slots': '{}',
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        assert ret.status == 200

        # Create a group in other domain.
        new_group_name = 'group-in-other-domain-to-test-user-query-groups'
        query = textwrap.dedent('''\
        mutation($name: String!, $input: GroupInput!) {
            create_group(name: $name, props: $input) {
                ok msg group { id name description is_active domain_name }
            }
        }''')
        variables = {
            'name': new_group_name,
            'input': {
                'domain_name': new_domain_name,
            }
        }
        payload = json.dumps({'query': query, 'variables': variables}).encode()
        headers = get_headers('POST', self.url, payload)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()
        new_gid = rsp_json['create_group']['group']['id']

        query = textwrap.dedent('''\
        {
            groups(domain_name: "default") {
                id name description is_active domain_name
            }
        }''')
        payload = json.dumps({'query': query}).encode()
        headers = get_headers('POST', self.url, payload, keypair=user_keypair)
        ret = await client.post(self.url, data=payload, headers=headers)
        rsp_json = await ret.json()

        assert ret.status == 200
        assert new_gid not in [item['id'] for item in rsp_json['groups']]
