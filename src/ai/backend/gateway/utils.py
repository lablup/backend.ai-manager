import asyncio
from collections import defaultdict
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
from typing import Any, Callable

from aiohttp import web
import trafaret as t

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

    def wrap(handler: Callable[[web.Request, Any], web.Response]):

        @functools.wraps(handler)
        async def wrapped(request: web.Request, *args, **kwargs) -> web.Response:
            try:
                if request.can_read_body:
                    params = await request.json(loads=loads or json.loads)
                else:
                    params = request.query
                params = checker.check(params)
            except json.decoder.JSONDecodeError:
                raise InvalidAPIParameters('Malformed body')
            except t.DataError as e:
                raise InvalidAPIParameters(f'Input validation error',
                                           extra_data=e.as_dict())
            return await handler(request, params, *args, **kwargs)

        return wrapped

    return wrap


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
        for k, tick in _burst_times.items():
            if now - tick > (max_idle / 1e3):
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


if hasattr(asyncio, 'get_running_loop'):  # Python 3.7+
    current_loop = asyncio.get_running_loop
else:
    current_loop = asyncio.get_event_loop
