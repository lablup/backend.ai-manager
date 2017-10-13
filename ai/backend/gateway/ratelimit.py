from decimal import Decimal
import logging
import time

import aioredis

from .defs import REDIS_RLIM_DB
from .exceptions import RateLimitExceeded

log = logging.getLogger('ai.backend.gateway.ratelimit')

_time_prec = Decimal('1e-3')  # msec
_rlim_window = 60 * 15        # 15 minutes


async def rlim_middleware_factory(app, handler):
    async def rlim_middleware_handler(request):
        # TODO: use a global timer if we scale out the gateway.
        now = Decimal(time.monotonic()).quantize(_time_prec)
        if request['is_authorized']:
            rate_limit = request['keypair']['rate_limit']
            access_key = request['keypair']['access_key']
            rlim_next_reset = now + _rlim_window
            async with app['redis_rlim'].get() as rr:
                tracker = await rr.hgetall(access_key)
                if tracker:
                    rlim_reset = Decimal(tracker['rlim_reset'])
                if not tracker or rlim_reset <= now:
                    # create a new tracker info
                    tracker = {
                        'rlim_limit': rate_limit,
                        'rlim_remaining': rate_limit,
                        'rlim_reset': str(rlim_next_reset),
                    }
                else:
                    tracker = {
                        'rlim_limit': rate_limit,
                        'rlim_remaining': int(tracker['rlim_remaining']) - 1,
                        'rlim_reset': tracker['rlim_reset'],  # copy
                    }
                await rr.hmset_dict(access_key, tracker)
            if tracker['rlim_remaining'] <= 0:
                raise RateLimitExceeded
            rlim_reset = Decimal(tracker['rlim_reset'])
            count_remaining = tracker['rlim_remaining']
            window_remaining = int((rlim_reset - now) * 1000)
            response = await handler(request)
            response.headers['X-RateLimit-Limit'] = str(rate_limit)
            response.headers['X-RateLimit-Remaining'] = str(count_remaining)
            response.headers['X-RateLimit-Reset'] = str(window_remaining)
            return response
        else:
            # No checks for rate limiting for non-authorized queries.
            response = await handler(request)
            response.headers['X-RateLimit-Limit'] = '1000'
            response.headers['X-RateLimit-Remaining'] = '1000'
            response.headers['X-RateLimit-Reset'] = str(_rlim_window * 1000)
            return response
    return rlim_middleware_handler


async def init(app):
    app['redis_rlim'] = await aioredis.create_pool(
        app.config.redis_addr.as_sockaddr(),
        create_connection_timeout=3.0,
        encoding='utf8',
        db=REDIS_RLIM_DB)
    app.middlewares.append(rlim_middleware_factory)


async def shutdown(app):
    try:
        async with app['redis_rlim'].get() as rr:
            await rr.flushdb()
    except ConnectionRefusedError:
        pass
    app['redis_rlim'].close()
    await app['redis_rlim'].wait_closed()
