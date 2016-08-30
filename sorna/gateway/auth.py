'''
OAuth 2.0 facilities
'''

import asyncio
import base64
from datetime import datetime, timedelta
import functools
import hashlib, hmac
import logging

from aiohttp import web
import asyncpg
from dateutil.tz import tzutc
from dateutil.parser import parse as dtparse


log = logging.getLogger('sorna.gateway.auth')

TEST_ACCOUNTS = {
    'AKIAIOSFODNN7EXAMPLE': {
        'userid': 'test-user',
        'name': 'Testion1',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    }
}


def _extract_auth_params(request):
    """
    HTTP Authorization header must be formatted as:
    "Authorization: Sorna <ACCESS_KEY>:<SIGNATURE>"
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
    pieces = auth_str.split(':', 1)
    if len(pieces) != 2:
        return None
    return pieces

def check_date(request) -> bool:
    date = request.headers.get('Date')
    if not date:
        date = request.headers.get('X-Sorna-Date')
    if not date:
        return False
    try:
        dt = dtparse(date)
        now = datetime.now(tzutc())
        min_time = now - timedelta(min=15)
        max_time = now + timedelta(min=15)
        if dt < min_time or dt > max_time:
            return False
    except ValueError:
        return False
    return True

async def sign_request(request, secret) -> str:
    assert hasattr(request, 'date')
    str_to_sign = request.method + '\n' \
                  + request.content_type + '\n' \
                  + request.path_qs + '\n' \
                  + dt.isoformat() + '\n' \
                  + (await request.text())
    hashed = hmac.new(secret.encode('ascii'),
                      str_to_sign.encode('utf8'),
                      hashlib.sha1)
    return base64.b64encode(hashed.digest())

def auth_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        params = _extract_auth_params(request)
        if not params:
            raise web.HTTPBadRequest(text='Missing authentication parameters.')
        access_key, signature = params
        if not check_date(request):
            raise web.HTTPBadRequest(text='Missing datetime or datetime mismatch.')
        # TODO: lookup access_key from user database
        try:
            user = TEST_ACCOUNTS[access_key]
            request.user = user
        except KeyError:
            raise web.HTTPUnauthorized(text='User not found.')
        my_signature = await sign_request(request, user['secret_key'])
        if not my_signature:
            raise web.HTTPBadRequest(text='Missing or invalid signature params.')
        if my_signature == signature:
            # authenticated successfully.
            return (await handler(request))
        raise web.HTTPUnauthorized(text='Signature mismatch.')
    return wrapped


async def init(app):
    pass
