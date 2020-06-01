import copy
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
from ai.backend.common import validators as tx
from .exceptions import (
    GenericBadRequest, GenericForbidden, GenericNotFound,
    InvalidAuthParameters, AuthorizationFailed,
    InvalidAPIParameters,
    InternalServerError,
)
from ..manager.models import (
    keypairs, keypair_resource_policies, users,
)
from ..manager.models.user import UserRole, check_credential
from ..manager.models.keypair import generate_keypair as _gen_keypair, generate_ssh_keypair
from ..manager.models.group import association_groups_users, groups
from ..manager.plugin import get_plugin_handlers_by_type
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
    async def wrapped(request, *args, **kwargs):
        if request.get('is_authorized', False):
            return (await handler(request, *args, **kwargs))
        raise AuthorizationFailed('Unauthorized access')

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


def admin_required(handler):

    @functools.wraps(handler)
    async def wrapped(request, *args, **kwargs):
        if request.get('is_authorized', False) and request.get('is_admin', False):
            return (await handler(request, *args, **kwargs))
        raise AuthorizationFailed('Unauthorized access')

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


def superadmin_required(handler):

    @functools.wraps(handler)
    async def wrapped(request, *args, **kwargs):
        if request.get('is_authorized', False) and request.get('is_superadmin', False):
            return (await handler(request, *args, **kwargs))
        raise AuthorizationFailed('Unauthorized access')

    set_handler_attr(wrapped, 'auth_required', True)
    return wrapped


@atomic
@auth_required
@check_api_params(
    t.Dict({
        t.Key('echo'): t.String,
    }))
async def test(request: web.Request, params: Any) -> web.Response:
    log.info('AUTH.TEST(ak:{})', request['keypair']['access_key'])
    resp_data = {'authorized': 'yes'}
    if 'echo' in params:
        resp_data['echo'] = params['echo']
    return web.json_response(resp_data)


@atomic
@auth_required
@check_api_params(
    t.Dict({
        t.Key('group', default=None): t.Null | tx.UUID,
    }))
async def get_role(request: web.Request, params: Any) -> web.Response:
    group_role = None
    log.info('AUTH.ROLES(ak:{}, d:{}, g:{})',
             request['keypair']['access_key'],
             request['user']['domain_name'],
             params['group'])
    if params['group'] is not None:
        query = (
            # TODO: per-group role is not yet implemented.
            sa.select([association_groups_users.c.group_id])
            .select_from(association_groups_users)
            .where(
                (association_groups_users.c.group_id == params['group']) &
                (association_groups_users.c.user_id == request['user']['uuid'])
            )
        )
        async with request.app['dbpool'].acquire() as conn:
            result = await conn.execute(query)
            row = await result.first()
            if row is None:
                raise GenericNotFound('No such user group or '
                                      'you are not the member of the group.')
        group_role = 'user'
    resp_data = {
        'global_role': 'superadmin' if request['is_superadmin'] else 'user',
        'domain_role': 'admin' if request['is_admin'] else 'user',
        'group_role': group_role,
    }
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
    log.info('AUTH.AUTHORIZE(d:{0[domain]}, u:{0[username]}, passwd:****, type:{0[type]})', params)
    dbpool = request.app['dbpool']
    user = await check_credential(
        dbpool,
        params['domain'], params['username'], params['password'])
    if user is None:
        raise AuthorizationFailed('User credential mismatch.')
    async with dbpool.acquire() as conn:
        query = (sa.select([keypairs.c.access_key, keypairs.c.secret_key])
                   .select_from(keypairs)
                   .where(
                       (keypairs.c.user == user['uuid']) &
                       (keypairs.c.is_active)
                   )
                   .order_by(sa.desc(keypairs.c.is_admin)))
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
    }).allow_extra('*'))
async def signup(request: web.Request, params: Any) -> web.Response:
    log_fmt = 'AUTH.SIGNUP(d:{}, email:{}, passwd:****)'
    log_args = (params['domain'], params['email'])
    log.info(log_fmt, *log_args)
    dbpool = request.app['dbpool']
    checked_user = None
    for _handler in get_plugin_handlers_by_type(request.app['plugins'], 'CHECK_USER'):
        # Execute all CHECK_USER plugin handlers,
        # which determine the user should be allowed to sign up or not per plugin bases.
        extra_params = copy.deepcopy(params)
        extra_params.pop('email')
        extra_params['config_server'] = request.app['config_server']
        checked_user = await _handler(params['email'], **extra_params)
        if not checked_user['success']:
            reason = checked_user.get('reason', 'signup not allowed')
            log.warning(reason)
            return web.json_response({'title': reason}, status=403)
    for _handler in get_plugin_handlers_by_type(request.app['plugins'], 'CHECK_PASSWORD'):
        # Execute all CHECK_PASSWORD plugin handlers, which checks the password validity.
        result = await _handler(params['password'])
        if not result['success']:
            reason = result.get('reason', 'invalid password')
            log.warning(reason)
            return web.json_response({'title': reason}, status=403)

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
            'username': params['username'] if 'username' in params else params['email'],
            'email': params['email'],
            'password': params['password'],
            'need_password_change': False,
            'full_name': params['full_name'] if 'full_name' in params else '',
            'description': params['description'] if 'description' in params else '',
            'is_active': True,
            'role': UserRole.USER,
            'integration_id': None,
        }
        if checked_user:
            # Override fields specified in check_user plugins.
            for key, val in checked_user.items():
                if key in data:
                    data[key] = val
        query = (users.insert().values(data))
        result = await conn.execute(query)
        if result.rowcount > 0:
            checkq = users.select().where(users.c.email == params['email'])
            result = await conn.execute(checkq)
            user = await result.first()
            # Create user's first access_key and secret_key.
            ak, sk = _gen_keypair()
            resource_policy = checked_user.get('resource_policy', 'default') \
                                  if checked_user is not None else 'default'
            kp_data = {
                'user_id': params['email'],
                'access_key': ak,
                'secret_key': sk,
                'is_active': data['is_active'] if 'is_active' in data else True,
                'is_admin': False,
                'resource_policy': resource_policy,
                'concurrency_used': 0,
                'rate_limit': 1000,
                'num_queries': 0,
                'user': user.uuid,
            }
            query = (keypairs.insert().values(kp_data))
            await conn.execute(query)

            # Add user to the default group.
            group_name = checked_user.get('group', 'default') \
                             if checked_user is not None else 'default'
            query = (sa.select([groups.c.id])
                       .select_from(groups)
                       .where(groups.c.domain_name == params['domain'])
                       .where(groups.c.name == group_name))
            result = await conn.execute(query)
            grp = await result.fetchone()
            if grp is not None:
                values = [{'user_id': user.uuid, 'group_id': grp.id}]
                query = association_groups_users.insert().values(values)
                await conn.execute(query)
        else:
            raise InternalServerError('Error creating user account')

    resp_data = {
        'access_key': ak,
        'secret_key': sk,
    }
    secret = request.app['config']['manager']['secret']
    for _handler in get_plugin_handlers_by_type(request.app['plugins'], 'POST_SIGNUP'):
        try:
            result = await _handler(
                params['email'],
                user_id=str(user.uuid),
                secret=secret,
                accept_language=request.headers.get('Accept-Language'),
            )
            if result.pop('success', False):
                resp_data.update(result)
        except (BaseException, Exception) as e:
            log.warning('plugin handler exception: ' + repr(e))
            pass  # ignore exceptions during post_signup hook since user is created anyway

    return web.json_response(resp_data, status=201)


@atomic
@auth_required
@check_api_params(
    t.Dict({
        tx.AliasedKey(['email', 'username']): t.String,
        t.Key('password'): t.String,
    }))
async def signout(request: web.Request, params: Any) -> web.Response:
    domain_name = request['user']['domain_name']
    log.info('AUTH.SIGNOUT(d:{}, email:{})', domain_name, params['email'])
    dbpool = request.app['dbpool']
    if request['user']['email'] != params['email']:
        raise GenericForbidden('Not the account owner')
    result = await check_credential(
        dbpool,
        domain_name, params['email'], params['password'])
    if result is None:
        raise GenericBadRequest('Invalid email and/or password')
    async with dbpool.acquire() as conn, conn.begin():
        # Inactivate the user.
        query = (users.update()
                      .values(is_active=False)
                      .where(users.c.email == params['email']))
        await conn.execute(query)
        # Inactivate every keypairs of the user.
        query = (keypairs.update()
                         .values(is_active=False)
                         .where(keypairs.c.user_id == params['email']))
        await conn.execute(query)
    return web.json_response({})


@atomic
@auth_required
@check_api_params(
    t.Dict({
        t.Key('old_password'): t.String,
        t.Key('new_password'): t.String,
        t.Key('new_password2'): t.String,
    }))
async def update_password(request: web.Request, params: Any) -> web.Response:
    domain_name = request['user']['domain_name']
    email = request['user']['email']
    log_fmt = 'AUTH.UDPATE_PASSWORD(d:{}, email:{})'
    log_args = (domain_name, email)
    log.info(log_fmt, *log_args)
    dbpool = request.app['dbpool']

    user = await check_credential(dbpool, domain_name, email, params['old_password'])
    if user is None:
        log.info(log_fmt + ': old password mismtach', *log_args)
        raise AuthorizationFailed('Old password mismatch')
    if params['new_password'] != params['new_password2']:
        log.info(log_fmt + ': new password mismtach', *log_args)
        return web.json_response({'error_msg': 'new password mismitch'}, status=400)

    # Check password validaty if one of plugins provide CHECK_PASSWORD handler.
    for plugin in request.app['plugins'].values():
        hook_event_types = plugin.get_hook_event_types()
        hook_event_handlers = plugin.get_handlers()
        for ev_types in hook_event_types:
            if 'CHECK_PASSWORD' not in ev_types._member_names_:
                continue
            for ev_handlers in hook_event_handlers:
                for ev_handler in ev_handlers:
                    if 'CHECK_PASSWORD' in ev_types._member_names_ and \
                            ev_types.CHECK_PASSWORD == ev_handler[0]:
                        check_password = ev_handler[1]
                        result = await check_password(params['new_password'])
                        if not result['success']:
                            reason = result.get('reason', 'too simple password')
                            return web.json_response({'title': reason}, status=403)

    async with dbpool.acquire() as conn:
        # Update user password.
        data = {
            'password': params['new_password'],
            'need_password_change': False,
        }
        query = (users.update().values(data).where(users.c.email == email))
        result = await conn.execute(query)
    return web.json_response({}, status=200)


@atomic
@auth_required
async def get_ssh_keypair(request: web.Request) -> web.Response:
    domain_name = request['user']['domain_name']
    access_key = request['keypair']['access_key']
    log_fmt = 'AUTH.GET_SSH_KEYPAIR(d:{}, ak:{})'
    log_args = (domain_name, access_key)
    log.info(log_fmt, *log_args)
    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        # Get SSH public key. Return partial string from the public key just for checking.
        query = (sa.select([keypairs.c.ssh_public_key])
                   .where(keypairs.c.access_key == access_key))
        pubkey = await conn.scalar(query)
    return web.json_response({'ssh_public_key': pubkey}, status=200)


@atomic
@auth_required
async def refresh_ssh_keypair(request: web.Request) -> web.Response:
    domain_name = request['user']['domain_name']
    access_key = request['keypair']['access_key']
    log_fmt = 'AUTH.REFRESH_SSH_KEYPAIR(d:{}, ak:{})'
    log_args = (domain_name, access_key)
    log.info(log_fmt, *log_args)
    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        pubkey, privkey = generate_ssh_keypair()
        data = {
            'ssh_public_key': pubkey,
            'ssh_private_key': privkey,
        }
        query = (keypairs.update()
                         .values(data)
                         .where(keypairs.c.access_key == access_key))
        await conn.execute(query)
    return web.json_response(data, status=200)


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
    cors.add(app.router.add_route('GET', '/role', get_role))
    cors.add(app.router.add_route('POST', '/signup', signup))
    cors.add(app.router.add_route('POST', '/signout', signout))
    cors.add(app.router.add_route('POST', '/update-password', update_password))
    cors.add(app.router.add_route('GET', '/ssh-keypair', get_ssh_keypair))
    cors.add(app.router.add_route('PATCH', '/ssh-keypair', refresh_ssh_keypair))
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
