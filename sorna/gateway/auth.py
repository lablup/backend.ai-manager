'''
OAuth 2.0 facilities
'''

import asyncio
import base64
import functools
import hashlib, hmac
import logging

from aiohttp import web
import asyncpg


log = logging.getLogger('sorna.gateway.auth')

TEST_ACCOUNTS = {
    'AKIAIOSFODNN7EXAMPLE': {
        'userid': 'test-user',
        'name': 'Testion1',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    }
}


def _extract_auth_str(request):
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

async def sign_request(request, secret):
    # TODO: add timestamp
    str_to_sign = request.method + '\n' \
                  + request.content_type + '\n' \
                  + request.path_qs + '\n' \
                  + (await request.text())
    hashed = hmac.new(secret.encode('ascii'), str_to_sign.encode('utf8'), hashlib.sha1)
    return base64.b64encode(hashed.digest())

def auth_required(handler):
    @functools.wraps(handler)
    async def wrapped(request):
        access_key, signature = _extract_auth_str(request)
        # TODO: lookup access_key from user database
        try:
            user = TEST_ACCOUNTS[access_key]
            request.user = user
        except KeyError:
            raise web.HTTPForbidden()
        my_signature = await sign_request(request, user['secret_key'])
        if not my_signature:
            raise web.HTTPForbidden()
        if my_signature == signature:
            # authenticated successfully.
            return (await handler(request))
        raise web.HTTPForbidden()
    return wrapped


async def init(app):
    pass
