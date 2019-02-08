import base64
from datetime import datetime, timedelta
import functools
import hashlib, hmac
import json
import logging
import secrets

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
from dateutil.tz import tzutc
from dateutil.parser import parse as dtparse
import sqlalchemy as sa

from ai.backend.common.logging import Logger, BraceStyleAdapter
from .exceptions import InvalidAuthParameters, AuthorizationFailed
from .config import load_config
from ..manager.models import keypairs, keypair_resource_policies
from .utils import TZINFOS, set_handler_attr, get_handler_attr

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
        # abbr timezones are not automatically parsable without tzinfos
        date = dtparse(raw_date, tzinfos=TZINFOS)
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
            j = sa.join(keypairs, keypair_resource_policies,
                        keypairs.c.resource_policy ==
                            keypair_resource_policies.c.name)
            query = (sa.select([keypairs,
                                keypair_resource_policies],
                               use_labels=True)
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
                    'id': row['keypairs_user_id'],
                }
                if row['keypairs_is_admin']:
                    request['is_admin'] = True
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

    set_handler_attr(wrapped, 'admin_required', True)
    return wrapped


@auth_required
@atomic
async def auth_test(request) -> web.Response:
    try:
        params = json.loads(await request.text())
    except json.decoder.JSONDecodeError:
        raise InvalidAuthParameters('Malformed body')
    resp_data = {'authorized': 'yes'}
    if 'echo' in params:
        resp_data['echo'] = params['echo']
    return web.json_response(resp_data)


def generate_keypair():
    '''
    AWS-like access key and secret key generation.
    '''
    ak = 'AKIA' + base64.b32encode(secrets.token_bytes(10)).decode('ascii')
    sk = secrets.token_urlsafe(30)
    return ak, sk


def create_app(default_cors_options):
    app = web.Application()
    app['prefix'] = 'auth'  # slashed to distinguish with "/vN/authorize"
    app['api_versions'] = (1, 2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    root_resource = cors.add(app.router.add_resource(r''))
    cors.add(root_resource.add_route('GET', auth_test))
    cors.add(root_resource.add_route('POST', auth_test))
    test_resource = cors.add(app.router.add_resource(r'/test'))
    cors.add(test_resource.add_route('GET', auth_test))
    cors.add(test_resource.add_route('POST', auth_test))
    return app, [auth_middleware]


if __name__ == '__main__':

    def auth_args(parser):
        parser.add('--generate-keypair',
                   action='store_true', default=False,
                   help='Generate a pair of access key and secret key.')

    config = load_config(extra_args_funcs=(auth_args, Logger.update_log_args))
    logger = Logger(config)
    logger.add_pkg('ai.backend')
    with logger:
        if config.generate_keypair:
            ak, sk = generate_keypair()
            print(f'Access Key: {ak} ({len(ak)} bytes)')
            print(f'Secret Key: {sk} ({len(sk)} bytes)')
