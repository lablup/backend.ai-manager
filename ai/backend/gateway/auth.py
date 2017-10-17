import base64
from datetime import datetime, timedelta
import functools
import hashlib, hmac
import logging
import secrets

from aiohttp import web
from dateutil.tz import tzutc
from dateutil.parser import parse as dtparse
import simplejson as json

from .exceptions import InvalidAuthParameters, AuthorizationFailed
from .config import load_config, init_logger
from ..manager.models import keypairs
from .utils import TZINFOS

log = logging.getLogger('ai.backend.gateway.auth')


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

        if request.has_body and not request.content_type == 'multipart/form-data':
            body = await request.read()
        else:
            body = b''
        body_hash = hashlib.new(hash_type, body).hexdigest()
        new_api_version = request.headers.get('X-BackendAI-Version')
        legacy_api_version = request.headers.get('X-Sorna-Version')
        api_version = new_api_version or legacy_api_version
        assert api_version is not None, 'API version missing in request headers'

        sign_bytes = ('{0}\n{1}\n{2}\nhost:{3}\ncontent-type:{4}\n'
                      'x-{name}-version:{5}\n{6}').format(
            request.method, str(request.rel_url), request['raw_date'],
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


async def auth_middleware_factory(app, handler):
    '''
    Fetches user information and sets up keypair, uesr, and is_authorized attributes.
    '''
    async def auth_middleware_handler(request):
        request['is_authorized'] = False
        request['is_admin'] = False
        request['keypair'] = None
        request['user'] = None
        if not check_date(request):
            raise InvalidAuthParameters('Date/time sync error')
        params = _extract_auth_params(request)
        if params:
            sign_method, access_key, signature = params
            async with app['dbpool'].acquire() as conn:
                query = (keypairs.select()
                                 .where(keypairs.c.access_key == access_key))
                result = await conn.execute(query)
                row = await result.first()
                if row is None:
                    raise AuthorizationFailed('Access key not found')
                my_signature = \
                    await sign_request(sign_method, request, row.secret_key)
                if secrets.compare_digest(my_signature, signature):
                    query = (keypairs.update()
                                     .values(last_used=datetime.now(tzutc()),
                                             num_queries=keypairs.c.num_queries + 1)
                                     .where(keypairs.c.access_key == access_key))
                    await conn.execute(query)
                    request['is_authorized'] = True
                    if row.is_admin:
                        request['is_admin'] = True
                    request['keypair'] = {
                        'access_key': access_key,
                        'concurrency_limit': row.concurrency_limit,
                        'rate_limit': row.rate_limit,
                    }
                    request['user'] = {
                        'id': row.user_id,
                    }
        # No matter if authenticated or not, pass-through to the handler.
        # (if it's required, auth_required decorator will handle the situation.)
        return (await handler(request))
    return auth_middleware_handler


def auth_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        if request.get('is_authorized', False):
            return (await handler(request))
        raise AuthorizationFailed
    return wrapped


def admin_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        if request.get('is_authorized', False) and request.get('is_admin', False):
            return (await handler(request))
        raise AuthorizationFailed
    return wrapped


@auth_required
async def authorize(request) -> web.Response:
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


async def init(app):
    app.router.add_route('GET', r'/v{version:\d+}/authorize', authorize)
    app.middlewares.append(auth_middleware_factory)


async def shutdown(app):
    pass


if __name__ == '__main__':

    def auth_args(parser):
        parser.add('--generate-keypair', action='store_true', default=False,
                   help='Generate a pair of access key and secret key.')

    config = load_config(extra_args_func=auth_args)
    init_logger(config)

    if config.generate_keypair:
        ak, sk = generate_keypair()
        print(f'Access Key: {ak} ({len(ak)} bytes)')
        print(f'Secret Key: {sk} ({len(sk)} bytes)')
