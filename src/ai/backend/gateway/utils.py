from collections import defaultdict
from datetime import datetime
import functools
import io
import inspect
import itertools
import json
import logging
import numbers
import re
import time
import traceback
from typing import Any, Callable, Sequence

from aiohttp import web
from dateutil.tz import gettz
import trafaret as t
from trafaret.lib import _empty
import pytz

from ai.backend.common.logging import BraceStyleAdapter

from .exceptions import InvalidAPIParameters, GenericForbidden, QueryNotImplemented

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.utils'))

_rx_sitepkg_path = re.compile(r'^.+/site-packages/')


def method_placeholder(orig_method):
    async def _handler(request):
        raise web.HTTPMethodNotAllowed(request.method, [orig_method])
    return _handler


def get_access_key_scopes(request):
    assert request['is_authorized'], \
           'Only authorized requests may have access key scopes'
    requester_access_key = request['keypair']['access_key']
    owner_access_key = request.query.get('owner_access_key', None)
    if owner_access_key is not None:
        if owner_access_key != requester_access_key and not request['is_admin']:
            raise GenericForbidden(
                'Only admins can access or control sessions owned by others.')
        return requester_access_key, owner_access_key
    return requester_access_key, requester_access_key


def check_api_params(checker: t.Trafaret, loads: Callable = None) -> Any:

    def wrap(handler: Callable[[web.Request], web.Response]):

        @functools.wraps(handler)
        async def wrapped(request: web.Request) -> web.Response:
            try:
                params = await request.json(loads=loads or json.loads)
                params = checker.check(params)
            except json.decoder.JSONDecodeError:
                raise InvalidAPIParameters('Malformed body')
            except t.DataError as e:
                raise InvalidAPIParameters(f'Input validation error',
                                           extra_data=e.as_dict())
            return await handler(request, params)

        return wrapped

    return wrap


class AliasedKey(t.Key):
    '''
    An extension to trafaret.Key which accepts multiple aliases of a single key.
    When successfully matched, the returned key name is the first one of the given aliases
    or the renamed key set via ``to_name()`` method or the ``>>`` operator.
    '''

    def __init__(self, names: Sequence[str], **kwargs):
        super().__init__(names[0], **kwargs)
        self.names = names

    def __call__(self, data, context=None):
        for name in self.names:
            if name in data:
                key = name
                break
        else:
            key = None

        if key is None:  # not specified
            if self.default is not _empty:
                default = self.default() if callable(self.default) else self.default
                try:
                    result = self.trafaret(default, context=context)
                except t.DataError as inner_error:
                    yield self.get_name(), inner_error, self.names
                else:
                    yield self.get_name(), result, self.names
                return
            if not self.optional:
                yield self.get_name(), t.DataError(error='is required'), self.names
            # if optional, just bypass
        else:
            try:
                result = self.trafaret(data[key], context=context)
            except t.DataError as inner_error:
                yield key, inner_error, self.names
            else:
                yield self.get_name(), result, self.names


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


def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


_burst_last_call = 0
_burst_times = dict()
_burst_counts = defaultdict(int)


async def call_non_bursty(key, coro, *, max_bursts=64, max_idle=100):
    '''
    Execute a coroutine once upon max_bursts bursty invocations or max_idle
    milliseconds after bursts smaller than max_bursts.
    '''
    global _burst_last_call, _burst_calls, _burst_counts
    if inspect.iscoroutine(coro):
        # Coroutine objects may not be called before garbage-collected
        # as this function throttles the frequency of invocation.
        # That will generate a bogus warning by the asyncio's debug facility.
        raise TypeError('You must pass coroutine function, not coroutine object.')
    now = time.monotonic()

    if now - _burst_last_call > 3.0:
        # garbage-collect keys
        cleaned_keys = []
        for k, t in _burst_times.items():
            if now - t > (max_idle / 1e3):
                cleaned_keys.append(k)
        for k in cleaned_keys:
            del _burst_times[k]
            _burst_counts.pop(k, None)

    last_called = _burst_times.get(key, 0)
    _burst_times[key] = now
    _burst_last_call = now
    invoke = False

    if now - last_called > (max_idle / 1e3):
        invoke = True
        _burst_counts.pop(key, None)
    else:
        _burst_counts[key] += 1
    if _burst_counts[key] >= max_bursts:
        invoke = True
        del _burst_counts[key]

    if invoke:
        if inspect.iscoroutinefunction(coro):
            return await coro()
        else:
            return coro()
