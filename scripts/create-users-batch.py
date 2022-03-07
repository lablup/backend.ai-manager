'''
This scripts generates a bunch of users
by creating fixtures and populating them
using the manager CLI.
'''

import json
import re
import secrets
import subprocess
import sys
import tempfile
from typing import (
    Any,
    Dict, Mapping,
    List,
)
import uuid

from ai.backend.manager.models.keypair import generate_keypair

import click


@click.command()
@click.argument('username_pattern')
@click.argument('num_users', type=int)
@click.option('-r', '--resource-policy', type=str, default='default',
              help='Set the resource policy of the users.')
@click.option('-g', '--group-uuid', type=str, default=None,
              help='Set the group of the users (as UUID).')
@click.option('-d', '--domain', type=str, default='default',
              help='Set the domain of the users (as name).')
@click.option('--rate-limit', type=int, default=30_000,
              help='Set the API rate limit for the generated keypairs.')
@click.option('--require-password-change', is_flag=True,
              help='Enforce users to change passwords after first login.')
@click.option('--dry-run', is_flag=True,
              help='Generate fixture and credentials only without population.')
def main(username_pattern: str, num_users: int,
         resource_policy: str, domain: str, group_uuid: str, rate_limit: int,
         require_password_change: bool,
         dry_run: bool) -> None:
    '''
    Generate NUM_USERS users with their email/names prefixed with USERNAME_PREFIX.
    '''
    run_id = secrets.token_hex(4)
    fixture: Mapping[str, List[Dict[str, Any]]] = {
        'users': [],
        'keypairs': [],
        'keypair_resource_usages': [],
        'association_groups_users': [],
    }

    if group_uuid is None:
        print('You must set the group UUID (-g/--group-uuid).', file=sys.stderr)
        sys.exit(1)

    m = re.search(r'X+', username_pattern)
    if not m:
        print('The username pattern must include one or more subsequent alphabet "X"s'
              'to indicate the place for zero-filled user indices.', file=sys.stderr)
        sys.exit(1)
    username_format = username_pattern.replace(m.group(0), f'{{0:0{len(m.group(0))}d}}')

    m = re.search(r'[^@]+@[^@]+', username_pattern)
    if not m:
        print('The username pattern must be an email format.')
        sys.exit(1)

    for idx in range(1, num_users + 1):
        ak, sk = generate_keypair()
        email = username_format.format(idx)
        user_uuid = str(uuid.uuid4())
        u = {
            'uuid': user_uuid,
            'username': email,
            'email': email,
            'password': secrets.token_urlsafe(4),
            'need_password_change': True,
            'full_name': email.split('@')[0],
            'description': 'Auto-generated user account',
            'is_active': require_password_change,
            'domain_name': domain,
            'role': 'user',
        }
        fixture['users'].append(u)
        kp = {
            'user_id': email,
            'user': user_uuid,
            'access_key': ak,
            'secret_key': sk,
            'is_active': True,
            'is_admin': False,
            'resource_policy': resource_policy,
            'rate_limit': rate_limit,
            'num_queries': 0,
        }
        fixture['keypairs'].append(kp)
        kp_conc = {
            'access_key': ak,
            'concurrency_used': 0,
        }
        fixture['keypair_resource_usages'].append(kp_conc)
        ug = {
            'user_id': user_uuid,
            'group_id': group_uuid,
        }
        fixture['association_groups_users'].append(ug)

    with tempfile.NamedTemporaryFile('w', prefix='backendai-batch-created-users',
                                     suffix='.json', encoding='utf-8') as ftmp:
        json.dump(fixture, ftmp, indent=4)
        ftmp.flush()
        if dry_run:
            fixture_path = f'generated-users-{run_id}-fixture.json'
            with open(fixture_path, 'w') as fout:
                json.dump(fixture, fout, indent=4)
            print(f'Generated user fixtures are saved at {fixture_path}')
        else:
            subprocess.run([
                'python', '-m', 'ai.backend.manager.cli',
                'fixture', 'populate', ftmp.name,
            ], check=True)

    creds_path = f'generated-users-{run_id}-creds.csv'
    with open(creds_path, 'w') as f:
        f.write('username,password,access_key,secret_key\n')
        for u, kp in zip(fixture['users'], fixture['keypairs']):
            f.write(f"{u['username']},{u['password']},{kp['access_key']},{kp['secret_key']}\n")

    print(f'Generated user credentials are saved at {creds_path}')


if __name__ == '__main__':
    main()
