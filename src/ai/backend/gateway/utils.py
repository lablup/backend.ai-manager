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
from typing import (
    Any, Union,
    Awaitable, Callable, Hashable,
    MutableMapping,
    Tuple,
)

from aiohttp import web
import trafaret as t
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import AccessKey

from .exceptions import InvalidAPIParameters, GenericForbidden, QueryNotImplemented
from ..manager.models import keypairs, users, UserRole

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.utils'))

_rx_sitepkg_path = re.compile(r'^.+/site-packages/')


def method_placeholder(orig_method):
    async def _handler(request):
        raise web.HTTPMethodNotAllowed(request.method, [orig_method])
    return _handler


async def get_access_key_scopes(request: web.Request, params: Any = None) -> Tuple[AccessKey, AccessKey]:
    if not request['is_authorized']:
        raise GenericForbidden('Only authorized requests may have access key scopes.')
    requester_access_key = request['keypair']['access_key']
    owner_access_key = request.query.get('owner_access_key', None)
    if owner_access_key is None and params is not None:
        owner_access_key = params.get('owner_access_key', None)
    if owner_access_key is not None and owner_access_key != requester_access_key:
        async with request.app['dbpool'].acquire() as conn:
            query = (
                sa.select([users.c.domain_name, users.c.role])
                .select_from(
                    sa.join(keypairs, users,
                            keypairs.c.user == users.c.uuid))
                .where(keypairs.c.access_key == owner_access_key)
            )
            result = await conn.execute(query)
            row = await result.fetchone()
            owner_domain = row['domain_name']
            owner_role = row['role']
        if request['is_superadmin']:
            pass
        elif request['is_admin']:
            if request['user']['domain_name'] != owner_domain:
                raise GenericForbidden(
                    'Domain-admins can perform operations on behalf of '
                    'other users in the same domain only.')
            if owner_role == UserRole.SUPERADMIN:
                raise GenericForbidden(
                    'Domain-admins cannot perform operations on behalf of super-admins.')
            pass
        else:
            raise GenericForbidden(
                'Only admins can perform operations on behalf of other users.')
        return requester_access_key, owner_access_key
    return requester_access_key, requester_access_key


def check_api_params(checker: t.Trafaret, loads: Callable[[str], Any] = None) -> Any:

    # FIXME: replace ... with [web.Request, Any...] in the future mypy
    def wrap(handler: Callable[..., Awaitable[web.Response]]):

        @functools.wraps(handler)
        async def wrapped(request: web.Request, *args, **kwargs) -> web.Response:
            try:
                if request.can_read_body:
                    orig_params = await request.json(loads=loads or json.loads)
                else:
                    orig_params = request.query
                # owner_access_key is a special case; we strip it for input validation.
                stripped_params = orig_params.copy()
                stripped_params.pop('owner_access_key', None)
                checked_params = checker.check(stripped_params)
            except json.decoder.JSONDecodeError:
                raise InvalidAPIParameters('Malformed body')
            except t.DataError as e:
                raise InvalidAPIParameters('Input validation error',
                                           extra_data=e.as_dict())
            return await handler(request, checked_params, *args, **kwargs)

        return wrapped

    return wrap


def trim_text(value: str, maxlen: int) -> str:
    if len(value) <= maxlen:
        return value
    value = value[:maxlen - 3] + '...'
    return value


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

    def __hash__(self):
        return hash(self)


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


def catch_unexpected(log, reraise_cancellation: bool = True, raven=None):

    def _wrap(func):

        @functools.wraps(func)
        async def _wrapped(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                if reraise_cancellation:
                    raise
            except Exception:
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


_burst_last_call: float = 0.0
_burst_times: MutableMapping[Hashable, float] = dict()
_burst_counts: MutableMapping[Hashable, int] = defaultdict(int)


async def call_non_bursty(key: Hashable, coro: Callable[[], Any], *,
                          max_bursts: int = 64,
                          max_idle: Union[int, float] = 100.0):
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


current_loop: Callable[[], asyncio.AbstractEventLoop]
if hasattr(asyncio, 'get_running_loop'):  # Python 3.7+
    current_loop = asyncio.get_running_loop  # type: ignore
else:
    current_loop = asyncio.get_event_loop
