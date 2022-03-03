from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import traceback
from typing import TYPE_CHECKING, AsyncIterator, TypedDict

import click
import sqlalchemy as sa

from ai.backend.common import redis
from ai.backend.common.config import redis_config_iv
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import RedisConnectionInfo

from ai.backend.manager.config import SharedConfig
from ai.backend.manager.defs import REDIS_STAT_DB, REDIS_LIVE_DB, REDIS_IMAGE_DB, REDIS_STREAM_DB

from ai.backend.manager.models import kernels
from ai.backend.manager.models.utils import connect_database

from ai.backend.manager.models.kernel import KernelStatus

if TYPE_CHECKING:
    from .context import CLIContext

log = BraceStyleAdapter(logging.getLogger(__name__))
date_multiplier = {
    's': 1,
    'm': 60,
    'h': 60 * 60,
    'd': 60 * 60 * 24,
    'y': 60 * 60 * 24 * 365,
}


class RedisObjects(TypedDict):
    live: RedisConnectionInfo
    stat: RedisConnectionInfo
    image: RedisConnectionInfo
    stream: RedisConnectionInfo


@click.group()
def cli(args) -> None:
    pass


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


@cli.command()
@click.argument('timedelta', type=str)
@click.pass_obj
def clear_history(cli_ctx: CLIContext, timedelta):
    '''
    Remove statistics data of terminated kernels from redis.
    Only kernels terminated before <timedelta> will be removed.
    Combine integer value with single character
    abbreviations (s, m, h, ...) to represent time values.
    Defaults to seconds if no suffix is provided.
    '''
    async def _impl():
        try:
            suffix = timedelta[-1].lower()
            delta_seconds = 0
            try:
                if multiplier := date_multiplier.get(suffix):
                    delta_seconds = int(timedelta[:-1]) * multiplier
                else:
                    delta_seconds = int(timedelta)
            except ValueError:
                log.exception('invalid timedelta value provided')
                return
            target_datetime = datetime.datetime.now() - datetime.timedelta(seconds=delta_seconds)
            async with connect_database(cli_ctx.local_config) as db:
                async with db.begin_readonly() as conn:
                    query = (
                        sa.select([kernels.c.id])
                        .select_from(kernels)
                        .where(
                            (kernels.c.status == KernelStatus.TERMINATED) &
                            (kernels.c.terminated_at < target_datetime),
                        )
                    )
                    result = await conn.execute(query)
                    target_kernels = [str(x['id']) for x in result.all()]
            async with redis_ctx(cli_ctx) as redis_objects:
                await redis.execute(
                    redis_objects['stat'],
                    lambda r: r.delete(*target_kernels),
                )
            log.info('Statistics cleared.')
        except:
            log.exception(traceback.format_exc())
    with cli_ctx.logger:
        asyncio.run(_impl())
