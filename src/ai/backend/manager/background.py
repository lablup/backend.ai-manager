from __future__ import annotations

import asyncio
import logging
import time
from typing import (
    Awaitable,
    Callable,
    Final,
    Literal,
    Optional,
    Union,
    Set,
    Type,
)
import uuid

import aioredis

from ai.backend.common import redis
from ai.backend.common.events import (
    BgtaskCancelledEvent,
    BgtaskDoneEvent,
    BgtaskFailedEvent,
    BgtaskUpdatedEvent,
    EventProducer,
)
from ai.backend.common.logging import BraceStyleAdapter


log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.background'))

MAX_BGTASK_ARCHIVE_PERIOD = 86400  # 24  hours

TaskResult = Literal['bgtask_done', 'bgtask_cancelled', 'bgtask_failed']


class ProgressReporter:
    event_producer: Final[EventProducer]
    task_id: Final[uuid.UUID]
    total_progress: Union[int, float]
    current_progress: Union[int, float]

    def __init__(
        self,
        event_dispatcher: EventProducer,
        task_id: uuid.UUID,
        current_progress: int = 0,
        total_progress: int = 0,
    ) -> None:
        self.event_producer = event_dispatcher
        self.task_id = task_id
        self.current_progress = current_progress
        self.total_progress = total_progress

    async def update(self, increment: Union[int, float] = 0, message: str = None):
        self.current_progress += increment
        # keep the state as local variables because they might be changed
        # due to interleaving at await statements below.
        current, total = self.current_progress, self.total_progress
        redis_producer = self.event_producer.redis_client

        async def _pipe_builder(r: aioredis.Redis):
            pipe = r.pipeline()
            tracker_key = f'bgtask.{self.task_id}'
            pipe.hmset(tracker_key, {
                'current': str(current),
                'total': str(total),
                'msg': message or '',
                'last_update': str(time.time()),
            })
            pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            await pipe.execute()

        await redis.execute(redis_producer, _pipe_builder)
        await self.event_producer.produce_event(
            BgtaskUpdatedEvent(
                self.task_id,
                message=message,
                current_progress=current,
                total_progress=total,
            ),
        )


BackgroundTask = Callable[[ProgressReporter], Awaitable[Optional[str]]]


class BackgroundTaskManager:
    event_producer: EventProducer
    ongoing_tasks: Set[asyncio.Task]

    def __init__(self, event_producer: EventProducer) -> None:
        self.event_producer = event_producer
        self.ongoing_tasks = set()

    async def start(
        self,
        func: BackgroundTask,
        name: str = None,
    ) -> uuid.UUID:
        task_id = uuid.uuid4()
        redis_producer = self.event_producer.redis_client

        async def _pipe_builder(r: aioredis.Redis):
            pipe = r.pipeline()
            tracker_key = f'bgtask.{task_id}'
            now = str(time.time())
            pipe.hmset(tracker_key, {
                'status': 'started',
                'current': '0',
                'total': '0',
                'msg': '',
                'started_at': now,
                'last_update': now,
            })
            pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            await pipe.execute()

        await redis.execute(redis_producer, _pipe_builder)

        task = asyncio.create_task(self._wrapper_task(func, task_id, name))
        self.ongoing_tasks.add(task)
        task.add_done_callback(self.ongoing_tasks.remove)
        return task_id

    async def _wrapper_task(
        self,
        func: BackgroundTask,
        task_id: uuid.UUID,
        task_name: Optional[str],
    ) -> None:
        task_result: TaskResult
        reporter = ProgressReporter(self.event_producer, task_id)
        message = ''
        event_cls: Type[BgtaskDoneEvent] | Type[BgtaskCancelledEvent] | Type[BgtaskFailedEvent] = \
            BgtaskDoneEvent
        try:
            message = await func(reporter) or ''
            task_result = 'bgtask_done'
        except asyncio.CancelledError:
            task_result = 'bgtask_cancelled'
            event_cls = BgtaskCancelledEvent
        except Exception as e:
            task_result = 'bgtask_failed'
            event_cls = BgtaskFailedEvent
            message = repr(e)
            log.exception("Task {} ({}): unhandled error", task_id, task_name)
        finally:
            redis_producer = self.event_producer.redis_client

            async def _pipe_builder(r: aioredis.Redis):
                pipe = r.pipeline()
                tracker_key = f'bgtask.{task_id}'
                pipe.hmset(tracker_key, {
                    'status': task_result[7:],  # strip "bgtask_"
                    'msg': message,
                    'last_update': str(time.time()),
                })
                pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
                await pipe.execute()

            await redis.execute(redis_producer, _pipe_builder)
            await self.event_producer.produce_event(
                event_cls(
                    task_id,
                    message=message,
                ),
            )
            log.info('Task {} ({}): {}', task_id, task_name or '', task_result)

    async def shutdown(self) -> None:
        log.info('Cancelling remaining background tasks...')
        for task in self.ongoing_tasks.copy():
            task.cancel()
            await task
