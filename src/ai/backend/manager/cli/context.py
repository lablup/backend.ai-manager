from __future__ import annotations

import atexit
import contextlib
import attr
import os
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, TypedDict

from ai.backend.common import redis
from ai.backend.common.config import redis_config_iv
from ai.backend.common.logging import Logger
from ai.backend.common.types import RedisConnectionInfo

from ai.backend.manager.config import SharedConfig
from ai.backend.manager.defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB, REDIS_STREAM_DB


if TYPE_CHECKING:
    from ..config import LocalConfig


@attr.s(auto_attribs=True, frozen=True)
class CLIContext:
    logger: Logger
    local_config: LocalConfig


class RedisObjects(TypedDict):
    live: RedisConnectionInfo
    stat: RedisConnectionInfo
    image: RedisConnectionInfo
    stream: RedisConnectionInfo


def init_logger(local_config: LocalConfig) -> Logger:
    if 'file' in local_config['logging']['drivers']:
        local_config['logging']['drivers'].remove('file')
    # log_endpoint = f'tcp://127.0.0.1:{find_free_port()}'
    log_sockpath = Path(f'/tmp/backend.ai/ipc/manager-cli-{os.getpid()}.sock')
    log_sockpath.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{log_sockpath}'

    def _clean_logger():
        try:
            os.unlink(log_sockpath)
        except FileNotFoundError:
            pass

    atexit.register(_clean_logger)
    return Logger(local_config['logging'], is_master=True, log_endpoint=log_endpoint)


@contextlib.asynccontextmanager
async def redis_ctx(cli_ctx: CLIContext) -> AsyncIterator[RedisObjects]:
    local_config = cli_ctx.local_config
    shared_config = SharedConfig(
        local_config['etcd']['addr'],
        local_config['etcd']['user'],
        local_config['etcd']['password'],
        local_config['etcd']['namespace'],
    )
    await shared_config.reload()
    raw_redis_config = await shared_config.etcd.get_prefix('config/redis')
    local_config['redis'] = redis_config_iv.check(raw_redis_config)
    redis_live = redis.get_redis_object(shared_config.data['redis'], db=REDIS_LIVE_DB)
    redis_stat = redis.get_redis_object(shared_config.data['redis'], db=REDIS_STAT_DB)
    redis_image = redis.get_redis_object(
        shared_config.data['redis'], db=REDIS_IMAGE_DB,
    )
    redis_stream = redis.get_redis_object(
        shared_config.data['redis'], db=REDIS_STREAM_DB,
    )
    yield {
        'live': redis_live,
        'stat': redis_stat,
        'image': redis_image,
        'stream': redis_stream,
    }
    await redis_stream.close()
    await redis_image.close()
    await redis_stat.close()
    await redis_live.close()
