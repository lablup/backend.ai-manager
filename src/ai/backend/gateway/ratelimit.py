from decimal import Decimal
import functools
import logging
import time

from aiohttp import web
import aioredis

from ai.backend.common import redis
from ai.backend.common.logging import BraceStyleAdapter

from .defs import REDIS_RLIM_DB
from .exceptions import RateLimitExceeded

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.ratelimit'))

_time_prec = Decimal('1e-3')  # msec
_rlim_window = 60 * 15

# We implement rate limiting using a rolling counter, which prevents
# last-minute and first-minute bursts between the intervals.

_rlim_script = '''
local access_key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local request_id = tonumber(redis.call('INCR', '__request_id'))
if request_id >= 1e12 then
    redis.call('SET', '__request_id', 1)
end
if redis.call('EXISTS', access_key) == 1 then
    redis.call('ZREMRANGEBYSCORE', access_key, 0, now - window)
end
redis.call('ZADD', access_key, now, tostring(request_id))
redis.call('EXPIRE', access_key, window)
return redis.call('ZCARD', access_key)
'''


@web.middleware
async def rlim_middleware(app, request, handler):
    # This is a global middleware: request.app is the root app.
    now = Decimal(time.time()).quantize(_time_prec)
    rr = app['redis_rlim']
    if request['is_authorized']:
        rate_limit = request['keypair']['rate_limit']
        access_key = request['keypair']['access_key']
        while True:
            try:
                ret = await redis.execute_with_retries(lambda: rr.evalsha(
                    app['redis_rlim_script'],
                    keys=[access_key],
                    args=[str(now), str(_rlim_window)]))
                break
            except aioredis.errors.ReplyError:
                # Redis may have been restarted.
                app['redis_rlim_script'] = await rr.script_load(_rlim_script)
                continue
        rolling_count = int(ret)
        if rolling_count > rate_limit:
            raise RateLimitExceeded
        remaining = rate_limit - rolling_count
        response = await handler(request)
        response.headers['X-RateLimit-Limit'] = str(rate_limit)
        response.headers['X-RateLimit-Remaining'] = str(remaining)
        response.headers['X-RateLimit-Window'] = str(_rlim_window)
        return response
    else:
        # No checks for rate limiting for non-authorized queries.
        response = await handler(request)
        response.headers['X-RateLimit-Limit'] = '1000'
        response.headers['X-RateLimit-Remaining'] = '1000'
        response.headers['X-RateLimit-Window'] = str(_rlim_window)
        return response


async def init(app: web.Application) -> None:
    rr = await redis.connect_with_retries(
        app['config']['redis']['addr'].as_sockaddr(),
        password=(app['config']['redis']['password']
                  if app['config']['redis']['password'] else None),
        timeout=3.0,
        encoding='utf8',
        db=REDIS_RLIM_DB)
    app['redis_rlim'] = rr
    app['redis_rlim_script'] = await rr.script_load(_rlim_script)


async def shutdown(app: web.Application) -> None:
    try:
        await app['redis_rlim'].flushdb()
    except (ConnectionResetError, ConnectionRefusedError):
        pass
    app['redis_rlim'].close()
    await app['redis_rlim'].wait_closed()


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (1, 2, 3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    # middleware must be wrapped by web.middleware at the outermost level.
    return app, [web.middleware(functools.partial(rlim_middleware, app))]
