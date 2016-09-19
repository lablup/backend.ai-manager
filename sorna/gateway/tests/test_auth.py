import base64
from datetime import datetime, timedelta
import hashlib, hmac
from types import SimpleNamespace
import uuid

from dateutil.tz import tzutc, gettz
import simplejson as json

from ..auth import init as auth_init
from ..auth import check_date, sign_request, TEST_ACCOUNTS


async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    resp = await client.get('/v1')
    assert resp.status == 200
    data = json.loads(await resp.text())
    assert data['version'] == 'v1.20160915'

async def test_auth(create_app_and_client, unused_port):
    app, client = await create_app_and_client()
    await auth_init(app)
    now = datetime.now(tzutc())
    hostname = '127.0.0.1:{}'.format(unused_port)
    headers = {
        'Date': now.isoformat(),
        'Content-Type': 'application/json',
        'X-Sorna-Version': 'v1.20160915',
    }
    req_data = {
        'echo': str(uuid.uuid4()),
    }
    req_bytes = json.dumps(req_data).encode()
    req_hash = hashlib.sha256(req_bytes).hexdigest()
    sign_bytes = b'GET\n' \
                 + b'/v1/authorize\n' \
                 + now.isoformat().encode() + b'\n' \
                 + b'host:' + hostname.encode() + b'\n' \
                 + b'content-type:application/json\n' \
                 + b'x-sorna-version:v1.20160915\n' \
                 + req_hash.encode()
    sign_key = hmac.new(TEST_ACCOUNTS[0]['secret_key'].encode(),
                        now.strftime('%Y%m%d').encode(), 'sha256').digest()
    sign_key = hmac.new(sign_key, hostname.encode(), 'sha256').digest()
    signature = str(base64.b64encode(hmac.new(sign_key, sign_bytes, 'sha256').digest()))
    headers['Authorization'] = 'Sorna signMethod=HMAC-SHA256, ' \
                               'credential={}:{}'.format(TEST_ACCOUNTS[0]['access_key'], signature)
    resp = await client.get('/v1/authorize',
                            data=req_bytes,
                            headers=headers,
                            skip_auto_headers=('User-Agent',))
    assert resp.status == 200
    data = json.loads(await resp.text())
    assert data['authorized'] == 'yes'
    assert data['echo'] == req_data['echo']


def test_check_date(mocker):
    request = SimpleNamespace()

    request.headers = { 'X-Nothing': '' }
    assert not check_date(request)

    now = datetime.now(tzutc())
    request.headers = { 'Date': now.isoformat() }
    assert check_date(request)

    request.headers = { 'Date': (now - timedelta(minutes=14, seconds=55)).isoformat() }
    assert check_date(request)
    request.headers = { 'Date': (now + timedelta(minutes=14, seconds=55)).isoformat() }
    assert check_date(request)

    request.headers = { 'Date': (now - timedelta(minutes=15, seconds=5)).isoformat() }
    assert not check_date(request)
    request.headers = { 'Date': (now + timedelta(minutes=15, seconds=5)).isoformat() }
    assert not check_date(request)

    # RFC822-style date formatting used in plain HTTP
    request.headers = { 'Date': '{:%a, %d %b %Y %H:%M:%S GMT}'.format(now) }
    assert check_date(request)

    # RFC822-style date formatting used in plain HTTP with a non-UTC timezone
    now_kst = now.astimezone(gettz('Asia/Seoul'))
    request.headers = { 'Date': '{:%a, %d %b %Y %H:%M:%S %Z}'.format(now_kst) }
    assert check_date(request)

    request.headers = { 'Date': 'some-unrecognizable-malformed-date-time' }
    assert not check_date(request)

    request.headers = { 'X-Sorna-Date': now.isoformat() }
    assert check_date(request)
