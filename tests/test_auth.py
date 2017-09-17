from collections import UserDict
from datetime import datetime, timedelta
import hashlib, hmac
from pprint import pprint
import uuid

from dateutil.tz import tzutc, gettz
import simplejson as json

from backend.ai.gateway.auth import init as auth_init
from backend.ai.gateway.auth import check_date
from backend.ai.gateway.server import LATEST_API_VERSION


async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    resp = await client.get('/v1')
    assert resp.status == 200
    data = json.loads(await resp.text())
    assert data['version'] == LATEST_API_VERSION


async def test_auth(create_app_and_client, unused_port, default_keypair):
    app, client = await create_app_and_client(extra_inits=[auth_init])

    async def do_authorize(hash_type, api_version):
        now = datetime.now(tzutc())
        hostname = 'localhost:{}'.format(unused_port)
        headers = {
            'Date': now.isoformat(),
            'Content-Type': 'application/json',
            'X-Backend.Ai-Version': api_version,
        }
        req_data = {
            'echo': str(uuid.uuid4()),
        }
        req_bytes = json.dumps(req_data).encode()
        req_hash = hashlib.new(hash_type, req_bytes).hexdigest()
        sign_bytes = b'GET\n' \
                     + b'/v1/authorize\n' \
                     + now.isoformat().encode() + b'\n' \
                     + b'host:' + hostname.encode() + b'\n' \
                     + b'content-type:application/json\n' \
                     + b'x-backend.ai-version:' + api_version.encode() + b'\n' \
                     + req_hash.encode()
        sign_key = hmac.new(default_keypair['secret_key'].encode(),
                            now.strftime('%Y%m%d').encode(), hash_type).digest()
        sign_key = hmac.new(sign_key, hostname.encode(), hash_type).digest()
        signature = hmac.new(sign_key, sign_bytes, hash_type).hexdigest()
        headers['Authorization'] = 'Backend.Ai signMethod=HMAC-{}, '.format(hash_type.upper()) \
                                   + 'credential={}:{}'.format(default_keypair['access_key'], signature)
        # Only shown when there are failures in this test case
        print('Request headers')
        pprint(headers)
        resp = await client.get('/v1/authorize',
                                data=req_bytes,
                                headers=headers,
                                skip_auto_headers=('User-Agent',))
        assert resp.status == 200
        data = json.loads(await resp.text())
        assert data['authorized'] == 'yes'
        assert data['echo'] == req_data['echo']

    # Try multiple different hashing schemes
    await do_authorize('sha1', 'v1.20160915')
    await do_authorize('sha256', 'v1.20160915')
    await do_authorize('md5', 'v1.20160915')


def test_check_date(mocker):

    # UserDict allows attribute assignment like types.SimpleNamespace
    # but also works like a plain dict.
    request = UserDict()

    request.headers = {'X-Nothing': ''}
    assert not check_date(request)

    now = datetime.now(tzutc())
    request.headers = {'Date': now.isoformat()}
    assert check_date(request)

    # Timestamps without timezone info
    request.headers = {'Date': f'{now:%Y%m%dT%H:%M:%S}'}
    assert check_date(request)

    request.headers = {'Date': (now - timedelta(minutes=14, seconds=55)).isoformat()}
    assert check_date(request)
    request.headers = {'Date': (now + timedelta(minutes=14, seconds=55)).isoformat()}
    assert check_date(request)

    request.headers = {'Date': (now - timedelta(minutes=15, seconds=5)).isoformat()}
    assert not check_date(request)
    request.headers = {'Date': (now + timedelta(minutes=15, seconds=5)).isoformat()}
    assert not check_date(request)

    # RFC822-style date formatting used in plain HTTP
    request.headers = {'Date': '{:%a, %d %b %Y %H:%M:%S GMT}'.format(now)}
    assert check_date(request)

    # RFC822-style date formatting used in plain HTTP with a non-UTC timezone
    now_kst = now.astimezone(gettz('Asia/Seoul'))
    request.headers = {'Date': '{:%a, %d %b %Y %H:%M:%S %Z}'.format(now_kst)}
    assert check_date(request)

    request.headers = {'Date': 'some-unrecognizable-malformed-date-time'}
    assert not check_date(request)

    request.headers = {'X-Backend.Ai-Date': now.isoformat()}
    assert check_date(request)
