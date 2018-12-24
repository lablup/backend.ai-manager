from datetime import datetime
import functools
import io
import numbers
import re
import traceback

from aiohttp import web
from dateutil.tz import gettz
import pytz

from .exceptions import QueryNotImplemented

_rx_sitepkg_path = re.compile(r'^.+/site-packages/')


def method_placeholder(orig_method):
    async def _handler(request):
        raise web.HTTPMethodNotAllowed(request.method, [orig_method])
    return _handler


class _Infinity(numbers.Number):

    def __lt__(self, o):
        return False

    def __le__(self, o):
        return False

    def __gt__(self, o):
        return True

    def __ge__(self, o):
        return False

    def __float__(self):
        return float('inf')

    def __int__(self):
        return 0xffff_ffff_ffff_ffff  # a practical 64-bit maximum


numbers.Number.register(_Infinity)
Infinity = _Infinity()


def prettify_traceback(exc):
    # Make a compact stack trace string
    with io.StringIO() as buf:
        while exc is not None:
            print(f'Exception: {exc!r}', file=buf)
            if exc.__traceback__ is None:
                print('  (no traceback available)', file=buf)
            else:
                for frame in traceback.extract_tb(exc.__traceback__):
                    short_path = _rx_sitepkg_path.sub('<sitepkg>/', frame.filename)
                    print(f'  {short_path}:{frame.lineno} ({frame.name})', file=buf)
            exc = exc.__context__
        return f'Traceback:\n{buf.getvalue()}'


def catch_unexpected(log, raven=None):

    def _wrap(func):

        @functools.wraps(func)
        async def _wrapped(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except:
                if raven:
                    raven.captureException()
                log.exception('unexpected error!')
                raise

        return _wrapped

    return _wrap


def gen_tzinfos():
    """ Yield abbreviated timezone name and tzinfo"""
    for zone in pytz.common_timezones:
        try:
            tzdate = pytz.timezone(zone).localize(datetime.utcnow(), is_dst=None)
        except pytz.NonExistentTimeError:
            pass
        else:
            tzinfo = gettz(zone)

            if tzinfo:
                yield tzdate.tzname(), tzinfo


TZINFOS = dict(gen_tzinfos())


def set_handler_attr(func, key, value):
    attrs = getattr(func, '_backend_attrs', None)
    if attrs is None:
        attrs = {}
    attrs[key] = value
    setattr(func, '_backend_attrs', attrs)


def get_handler_attr(request, key, default=None):
    # When used in the aiohttp server-side codes, we should use
    # request.match_info.hanlder instead of handler passed to the middleware
    # functions because aiohttp wraps this original handler with functools.partial
    # multiple times to implement its internal middleware processing.
    attrs = getattr(request.match_info.handler, '_backend_attrs', None)
    if attrs is not None:
        return attrs.get(key, default)
    return default


async def not_impl_stub(request) -> web.Response:
    raise QueryNotImplemented
