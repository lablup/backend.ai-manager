from datetime import datetime, timedelta
import functools
import hashlib, hmac
import logging
import secrets
from typing import Any

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import click
from dateutil.tz import tzutc
from dateutil.parser import parse as dtparse
import sqlalchemy as sa
import trafaret as t

from ai.backend.common.logging import Logger, BraceStyleAdapter
from .exceptions import (
    GenericBadRequest, InternalServerError,
    InvalidAuthParameters, AuthorizationFailed,
    InvalidAPIParameters,
)
from ..manager.models import (
    keypairs, keypair_resource_policies, users,
)
from ..manager.models.user import UserRole, check_credential
from ..manager.models.keypair import generate_keypair as _gen_keypair
from ..manager.models.group import association_groups_users, groups
from .utils import check_api_params, set_handler_attr, get_handler_attr

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.auth'))


def _extract_auth_params(request):
    """
    HTTP Authorization header must be formatted as:
    "Authorization: BackendAI signMethod=HMAC-SHA256,
                    credential=<ACCESS_KEY>:<SIGNATURE>"
    """
    auth_hdr = request.headers.get('Authorization')
    if not auth_hdr:
        return None
    pieces = auth_hdr.split(' ', 1)
    if len(pieces) != 2:
        raise InvalidAuthParameters('Malformed authorization header')
    auth_type, auth_str = pieces
    if auth_type not in ('BackendAI', 'Sorna'):
        raise InvalidAuthParameters('Invalid authorization type name')

    raw_params = map(lambda s: s.strip(), auth_str.split(','))
    params = {}
    for param in raw_params:
        key, value = param.split('=', 1)
        params[key.strip()] = value.strip()

    try:
        access_key, signature = params['credential'].split(':', 1)
        ret = params['signMethod'], access_key, signature
        return ret
    except (KeyError, ValueError):
        raise InvalidAuthParameters('Missing or malformed authorization parameters')


def check_date(request) -> bool:
    raw_date = request.headers.get('Date')
    if not raw_date:
        raw_date = request.headers.get('X-BackendAI-Date',
                                       request.headers.get('X-Sorna-Date'))
    if not raw_date:
        return False
    try:
        # HTTP standard says "Date" header must be in GMT only.
        # However, dateutil.parser can recognize other commonly used
        # timezone names and offsets.
        date = dtparse(raw_date)
        if date.tzinfo is None:
            date = date.replace(tzinfo=tzutc())  # assume as UTC
        now = datetime.now(tzutc())
        min_date = now - timedelta(minutes=15)
        max_date = now + timedelta(minutes=15)
        request['date'] = date
        request['raw_date'] = raw_date
        if date < min_date or date > max_date:
            return False
    except ValueError:
        return False
    return True


async def sign_request(sign_method, request, secret_key) -> str:
    try:
        mac_type, hash_type = map(lambda s: s.lower(), sign_method.split('-'))
        assert mac_type == 'hmac', 'Unsupported request signing method (MAC type)'
        assert hash_type in hashlib.algorithms_guaranteed, \
               'Unsupported request signing method (hash type)'

        new_api_version = request.headers.get('X-BackendAI-Version')
        legacy_api_version = request.headers.get('X-Sorna-Version')
        api_version = new_api_version or legacy_api_version
        assert api_version is not None, 'API version missing in request headers'
        body = b''
        if api_version < 'v4.20181215':
            if (request.can_read_body and
                request.content_type != 'multipart/form-data'):
                # read the whole body if neither streaming nor bodyless
                body = await request.read()
        body_hash = hashlib.new(hash_type, body).hexdigest()

        sign_bytes = ('{0}\n{1}\n{2}\nhost:{3}\ncontent-type:{4}\n'
                      'x-{name}-version:{5}\n{6}').format(
            request.method, str(request.raw_path), request['raw_date'],
            request.host, request.content_type, api_version,
            body_hash,
            name='backendai' if new_api_version is not None else 'sorna'
        ).encode()
        sign_key = hmac.new(secret_key.encode(),
                            request['date'].strftime('%Y%m%d').encode(),
                            hash_type).digest()
        sign_key = hmac.new(sign_key, request.host.encode(), hash_type).digest()
        return hmac.new(sign_key, sign_bytes, hash_type).hexdigest()
    except ValueError:
        raise AuthorizationFailed('Invalid signature')
    except AssertionError as e:
        raise InvalidAuthParameters(e.args[0])


@web.middleware
async def auth_middleware(request, handler):
    '''
    Fetches user information and sets up keypair, uesr, and is_authorized
    attributes.
    '''
    # This is a global middleware: request.app is the root app.
    request['is_authorized'] = False
    request['is_admin'] = False
    request['is_superadmin'] = False
    request['keypair'] = None
    request['user'] = None
    if not get_handler_attr(request, 'auth_required', False):
        return (await handler(request))
    if not check_date(request):
        raise InvalidAuthParameters('Date/time sync error')
    params = _extract_auth_params(request)
    if params:
        sign_method, access_key, signature = params
        async with request.app['dbpool'].acquire() as conn, conn.begin():
            j = (keypairs.join(users, keypairs.c.user == users.c.uuid)
                         .join(keypair_resource_policies,
                               keypairs.c.resource_policy == keypair_resource_policies.c.name))
            query = (sa.select([users, keypairs, keypair_resource_policies], use_labels=True)
                       .select_from(j)
                       .where((keypairs.c.access_key == access_key) &
                              (keypairs.c.is_active.is_(True))))
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise AuthorizationFailed('Access key not found')
            my_signature = \
                await sign_request(sign_method, request, row['keypairs_secret_key'])
            if secrets.compare_digest(my_signature, signature):
                query = (keypairs.update()
                                 .values(last_used=datetime.now(tzutc()),
                                         num_queries=keypairs.c.num_queries + 1)
                                 .where(keypairs.c.access_key == access_key))
                await conn.execute(query)
                request['is_authorized'] = True
                request['keypair'] = {
                    col.name: row[f'keypairs_{col.name}']
                    for col in keypairs.c
                    if col.name != 'secret_key'
                }
                request['keypair']['resource_policy'] = {
                    col.name: row[f'keypair_resource_policies_{col.name}']
                    for col in keypair_resource_policies.c
                }
                request['user'] = {
                    col.name: row[f'users_{col.name}']
                    for col in users.c
                    if col.name not in ('password', 'description', 'created_at')
                }
                request['user']['id'] = row['keypairs_user_id']  # legacy
                # if request['role'] in ['admin', 'superadmin']:
                if row['keypairs_is_admin']:
                    request['is_admin'] = True
                if request['user']['role'] == 'superadmin':
                    request['is_superadmin'] = True

    # No matter if authenticated or not, pass-through to the handler.
    # (if it's required, auth_required decorator will handle the situation.)
    return (await handler(request))


def auth_required(handler):

    @functools.wraps(handler)
    async def wrapped(request):
        if request.get('is_authorized', False):
            return (await handler(request))
        raise AuthorizationFailed

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


def admin_required(handler):

    @functools.wraps(handler)
    async def wrapped(request):
        if request.get('is_authorized', False) and request.get('is_admin', False):
            return (await handler(request))
        raise AuthorizationFailed

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


def superadmin_required(handler):

    @functools.wraps(handler)
    async def wrapped(request):
        if request.get('is_authorized', False) and request.get('is_superadmin', False):
            return (await handler(request))
        raise AuthorizationFailed

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


@atomic
@auth_required
@check_api_params(
    t.Dict({
        t.Key('echo'): t.String,
    }))
async def test(request: web.Request, params: Any) -> web.Response:
    resp_data = {'authorized': 'yes'}
    if 'echo' in params:
        resp_data['echo'] = params['echo']
    return web.json_response(resp_data)


@atomic
@check_api_params(
    t.Dict({
        t.Key('type'): t.Enum('keypair', 'jwt'),
        t.Key('domain'): t.String,
        t.Key('username'): t.String,
        t.Key('password'): t.String,
    }))
async def authorize(request: web.Request, params: Any) -> web.Response:
    if params['type'] != 'keypair':
        # other types are not implemented yet.
        raise InvalidAPIParameters('Unsupported authorization type')
    dbpool = request.app['dbpool']
    user = await check_credential(
        dbpool,
        params['domain'], params['username'], params['password'])
    if user is None:
        raise AuthorizationFailed('User credential mismatch.')
    async with dbpool.acquire() as conn:
        query = (sa.select([keypairs.c.access_key, keypairs.c.secret_key])
                   .select_from(users)
                   .where((keypairs.c.user == user['uuid'])))
        result = await conn.execute(query)
        keypair = await result.first()
    return web.json_response({
        'data': {
            'access_key': keypair['access_key'],
            'secret_key': keypair['secret_key'],
            'role': user['role'],
        },
    })


@atomic
@check_api_params(
    t.Dict({
        t.Key('domain'): t.String,
        t.Key('email'): t.String,
        t.Key('password'): t.String,
    }))
async def signup(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    # TODO: assume only one hook (hanati) and bound method (very dirty, but no time now)
    # Check if email exists in hanati
    assert 'hanati_hook' in request.app
    check_user = request.app['hanati_hook'].get_handlers()[0][0][1]
    hana_user = await check_user(params['email'])  # exception will be raised if not found
    if isinstance(hana_user, dict) and not hana_user['success']:
        return web.json_response({'error_msg': 'no such cloudia user'}, status=404)

    async with dbpool.acquire() as conn:
        # Check if email already exists.
        query = (sa.select([users])
                   .select_from(users)
                   .where((users.c.email == params['email'])))
        result = await conn.execute(query)
        row = await result.first()
        if row is not None:
            raise GenericBadRequest('Email already exists')

        # Create a user.
        data = {
            'domain_name': params['domain'],
            'username': params['email'],
            'email': params['email'],
            'password': params['password'],
            'need_password_change': False,
            'full_name': hana_user.name,
            'description': f'Cloudia user in {hana_user.company.name}',
            'is_active': True,
            'role': UserRole.USER,
            'integration_id': hana_user.idx,
        }
        query = (users.insert().values(data))
        result = await conn.execute(query)
        if result.rowcount > 0:
            checkq = users.select().where(users.c.email == params['email'])
            result = await conn.execute(checkq)
            user = await result.first()
            # Create user's first access_key and secret_key.
            ak, sk = _gen_keypair()
            kp_data = {
                'user_id': params['email'],
                'access_key': ak,
                'secret_key': sk,
                'is_active': True,
                'is_admin': False,
                'resource_policy': 'default',
                'concurrency_used': 0,
                'rate_limit': 1000,
                'num_queries': 0,
                'user': user.uuid,
            }
            query = (keypairs.insert().values(kp_data))
            await conn.execute(query)

            # Add user to the default group.
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == params['domain'])
                       .where(groups.c.name == 'default'))
            result = await conn.execute(query)
            grp = await result.fetchone()
            values = [{'user_id': user.uuid, 'group_id': grp.id}]
            query = association_groups_users.insert().values(values)
            await conn.execute(query)
        else:
            raise InternalServerError('Error creating user account')
    return web.json_response({
        'data': {
            'access_key': ak,
            'secret_key': sk,
        },
    })


def create_app(default_cors_options):
    app = web.Application()
    app['prefix'] = 'auth'  # slashed to distinguish with "/vN/authorize"
    app['api_versions'] = (1, 2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    root_resource = cors.add(app.router.add_resource(r''))
    cors.add(root_resource.add_route('GET', test))
    cors.add(root_resource.add_route('POST', test))
    test_resource = cors.add(app.router.add_resource('/test'))
    cors.add(test_resource.add_route('GET', test))
    cors.add(test_resource.add_route('POST', test))
    cors.add(app.router.add_route('POST', '/authorize', authorize))
    cors.add(app.router.add_route('POST', '/signup', signup))
    return app, [auth_middleware]


@click.group()
def cli():
    pass


@cli.command()
def generate_keypair():
    logger = Logger({
        'level': 'INFO',
        'drivers': ['console'],
        'pkg-ns': {'ai.backend': 'INFO'},
        'console': {'colored': True, 'format': 'verbose'},
    })
    with logger:
        log.info('generating keypair...')
        ak, sk = _gen_keypair()
        print(f'Access Key: {ak} ({len(ak)} bytes)')
        print(f'Secret Key: {sk} ({len(sk)} bytes)')


if __name__ == '__main__':
    cli()
