from collections import UserDict
from datetime import datetime, timedelta
import json
from pprint import pprint
import uuid

from dateutil.tz import tzutc, gettz
import pytest

from ai.backend.gateway.auth import _extract_auth_params, check_date
from ai.backend.gateway.exceptions import InvalidAuthParameters


def test_extract_auth_params():
    request = UserDict()

    request.headers = {}
    assert _extract_auth_params(request) is None

    request.headers = {'Authorization': 'no-space'}
    with pytest.raises(InvalidAuthParameters):
        _extract_auth_params(request)

    request.headers = {'Authorization': ('BadAuthType signMethod=HMAC-SHA256,'
                                         'credential=fake-ak:fake-sig')}
    with pytest.raises(InvalidAuthParameters):
        _extract_auth_params(request)

    request.headers = {'Authorization': ('BackendAI signMethod=HMAC-SHA256,'
                                         'credential=fake-ak:fake-sig')}
    ret = _extract_auth_params(request)
    assert ret[0] == 'HMAC-SHA256'
    assert ret[1] == 'fake-ak'
    assert ret[2] == 'fake-sig'


def test_check_date():
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
    now_est = now.astimezone(gettz('America/Panama'))
    request.headers = {'Date': '{:%a, %d %b %Y %H:%M:%S %Z}'.format(now_est)}
    assert check_date(request)

    request.headers = {'Date': 'some-unrecognizable-malformed-date-time'}
    assert not check_date(request)

    request.headers = {'X-BackendAI-Date': now.isoformat()}
    assert check_date(request)


@pytest.mark.asyncio
async def test_authorize(create_app_and_client, get_headers):
    app, client = await create_app_and_client(extras=['auth'])

    async def do_authorize(hash_type, api_version):
        req_data = {'echo': str(uuid.uuid4())}
        req_bytes = json.dumps(req_data).encode()
        headers = get_headers(req_bytes, hash_type, api_version)
        # Only shown when there are failures in this test case
        print('Request headers')
        pprint(headers)
        resp = await client.get('/v3/authorize',
                                data=req_bytes,
                                headers=headers,
                                skip_auto_headers=('User-Agent',))
        assert resp.status == 200
        data = json.loads(await resp.text())
        assert data['authorized'] == 'yes'
        assert data['echo'] == req_data['echo']

    # Try multiple different hashing schemes
    await do_authorize('sha1', 'v3.20170615')
    await do_authorize('sha256', 'v3.20170615')
    await do_authorize('md5', 'v3.20170615')
