from datetime import datetime, timedelta
import hashlib, hmac
from pprint import pprint
from types import SimpleNamespace
import uuid

from dateutil.tz import tzutc, gettz
import simplejson as json

from ..auth import init as auth_init
from ..auth import check_date


async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    resp = await client.get('/v1')
    assert resp.status == 200
    data = json.loads(await resp.text())
    assert data['version'] == 'v1.20160915'


async def test_auth(create_app_and_client, unused_port, default_keypair):
    app, client = await create_app_and_client()
    await auth_init(app)

    async def do_authorize(hash_type, api_version):
        now = datetime.now(tzutc())
        hostname = 'localhost:{}'.format(unused_port)
        headers = {
            'Date': now.isoformat(),
            'Content-Type': 'application/json',
            'X-Sorna-Version': api_version,
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
                     + b'x-sorna-version:' + api_version.encode() + b'\n' \
                     + req_hash.encode()
        sign_key = hmac.new(default_keypair['secret_key'].encode(),
                            now.strftime('%Y%m%d').encode(), hash_type).digest()
        sign_key = hmac.new(sign_key, hostname.encode(), hash_type).digest()
        signature = hmac.new(sign_key, sign_bytes, hash_type).hexdigest()
        headers['Authorization'] = 'Sorna signMethod=HMAC-{}, '.format(hash_type.upper()) \
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
    request = SimpleNamespace()

    request.headers = {'X-Nothing': ''}
    assert not check_date(request)

    now = datetime.now(tzutc())
    request.headers = {'Date': now.isoformat()}
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

    request.headers = {'X-Sorna-Date': now.isoformat()}
    assert check_date(request)
