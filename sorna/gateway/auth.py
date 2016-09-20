'''
OAuth 2.0 facilities
'''

import asyncio
from datetime import datetime, timedelta
import functools
import hashlib, hmac
import logging

from aiohttp import web
import asyncpg
from dateutil.tz import tzutc
from dateutil.parser import parse as dtparse
import simplejson as json


log = logging.getLogger('sorna.gateway.auth')

TEST_ACCOUNTS = [
    {
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'userid': 'test-user',
        'name': 'Testion1',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    },
]


def _extract_auth_params(request):
    """
    HTTP Authorization header must be formatted as:
    "Authorization: Sorna signMethod=HMAC-SHA256, credential=<ACCESS_KEY>:<SIGNATURE>"
    """
    auth_hdr = request.headers.get('Authorization')
    if not auth_hdr:
        return None
    pieces = auth_hdr.split(' ', 1)
    if len(pieces) != 2:
        return None
    auth_type, auth_str = pieces
    if auth_type != 'Sorna':
        return None

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
        return None

def check_date(request) -> bool:
    date = request.headers.get('Date')
    if not date:
        date = request.headers.get('X-Sorna-Date')
    if not date:
        return False
    try:
        dt = dtparse(date)
        request.date = dt
        now = datetime.now(tzutc())
        min_time = now - timedelta(minutes=15)
        max_time = now + timedelta(minutes=15)
        if dt < min_time or dt > max_time:
            return False
    except ValueError:
        return False
    return True

async def sign_request(sign_method, request, secret) -> str:
    assert hasattr(request, 'date')
    mac_type, hash_type = sign_method.split('-')
    hash_type = hash_type.lower()
    assert mac_type.lower() == 'hmac'
    assert hash_type in hashlib.algorithms_guaranteed

    req_hash = hashlib.new(hash_type, await request.read()).hexdigest()
    sign_bytes = request.method.encode() + b'\n' \
                 + request.path_qs.encode() + b'\n' \
                 + request.date.isoformat().encode() + b'\n' \
                 + b'host:' + request.host.encode() + b'\n' \
                 + b'content-type:' + request.content_type.encode() + b'\n' \
                 + b'x-sorna-version:' + request.headers['X-Sorna-Version'].encode() + b'\n' \
                 + req_hash.encode()

    sign_key = hmac.new(secret.encode(),
                        request.date.strftime('%Y%m%d').encode(), hash_type).digest()
    sign_key = hmac.new(sign_key, request.host.encode(), hash_type).digest()
    return hmac.new(sign_key, sign_bytes, hash_type).hexdigest()

def auth_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        params = _extract_auth_params(request)
        if not params:
            raise web.HTTPBadRequest(text='Missing authentication parameters.')
        sign_method, access_key, signature = params
        if not check_date(request):
            raise web.HTTPBadRequest(text='Missing datetime or datetime mismatch.')
        # TODO: lookup access_key from user database
        try:
            for user in TEST_ACCOUNTS:
                if user['access_key'] == access_key:
                    request.user = user
                    break
            else:
                raise KeyError
        except KeyError:
            raise web.HTTPUnauthorized(text='User not found.')
        my_signature = await sign_request(sign_method, request, request.user['secret_key'])
        if not my_signature:
            raise web.HTTPBadRequest(text='Missing or invalid signature params.')
        if my_signature == signature:
            # authenticated successfully.
            return (await handler(request))
        raise web.HTTPUnauthorized(text='Signature mismatch.')
    return wrapped


@auth_required
async def authorize(request) -> web.Response:
    req_data = json.loads(await request.text())
    resp_data = { 'authorized': 'yes' }
    if 'echo' in req_data:
        resp_data['echo'] = req_data['echo']
    return web.json_response(resp_data)


async def init(app):
    app.router.add_route('GET', '/v1/authorize', authorize)
