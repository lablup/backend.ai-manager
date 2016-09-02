from datetime import datetime, timedelta
from types import SimpleNamespace

from dateutil.tz import tzutc, gettz

from ..auth import init as auth_init
from ..auth import check_date
from ..server import init as server_init


async def test_hello(create_app_and_client):
    app, client = await create_app_and_client()
    await server_init(app)
    resp = await client.get('/1.0')
    body = await resp.read()
    assert body == b'OK'

async def test_auth(create_app_and_client):
    app, client = await create_app_and_client()
    await auth_init(app)
    # TODO: implement

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
