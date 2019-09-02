'''
This scripts generates a bunch of users
by creating fixtures and populating them
using the manager CLI.
'''

import json
import secrets
import subprocess
import tempfile
import uuid

from ai.backend.manager.models.keypair import generate_keypair

import click


@click.command()
@click.argument('username_prefix')
@click.argument('num_users', type=int)
@click.option('-r', '--resource-policy', type=str, default='default', help='Set the resource policy of the users.')
@click.option('-d', '--domain', type=str, default='default', help='Set the domain of the users.')
@click.option('--rate-limit', type=int, default=30_000, help='Set the API rate limit for the generated keypairs.')
@click.option('--dry-run', is_flag=True, help='Generate fixture and credentials only without population.')
def main(username_prefix, num_users, resource_policy, domain, rate_limit, dry_run):
    '''
    Generate NUM_USERS users with their email/names prefixed with USERNAME_PREFIX.
    '''
    run_id = secrets.token_hex(4)
    fixture = {}
    fixture['users'] = []
    fixture['keypairs'] = []

    for idx in range(1, num_users + 1):
        ak, sk = generate_keypair()
        email = f'{username_prefix}{idx:03d}@managed.lablup.com'
        user_uuid = str(uuid.uuid4())
        u = {
            'uuid': user_uuid,
            'username': email,
            'email': email,
            'password': secrets.token_urlsafe(4),
            'need_password_change': True,
            'full_name': f'{username_prefix}{idx:03d}',
            'description': 'Auto-generated user account',
            'is_active': True,
            'domain_name': domain,
            'role': 'USER',
        }
        fixture['users'].append(u)
        kp = {
            'user_id': email,
            'access_key': ak,
            'secret_key': sk,
            'is_active': True,
            'is_admin': False,
            'resource_policy': resource_policy,
            'concurrency_used': 0,
            'rate_limit': rate_limit,
            'num_queries': 0,
            'user': user_uuid,
        }
        fixture['keypairs'].append(kp)

    with tempfile.NamedTemporaryFile('w', prefix=username_prefix,
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
                'scripts/run-with-halfstack.sh',
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
