from __future__ import annotations

import asyncio
import logging
import time
from typing import (
    Awaitable, Callable, Optional,
    Literal, Union,
    Set,
)
import uuid

from aiojobs import Scheduler

from ai.backend.common import redis
from ai.backend.common.logging import BraceStyleAdapter

from ..gateway.events import EventDispatcher

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))

MAX_BGTASK_ARCHIVE_PERIOD = 86400  # 24  hours

TaskResult = Literal['task_done', 'task_cancelled', 'task_failed']


class ProgressReporter:
    event_dispatcher: EventDispatcher
    total_progress: Union[int, float]
    current_progress: Union[int, float]
    task_id: uuid.UUID

    def __init__(self, event_dispatcher: EventDispatcher, task_id: uuid.UUID) -> None:
        self.event_dispatcher = event_dispatcher
        self.task_id = task_id

    async def set_progress_total(self, value: Union[int, float]):
        self.total_progress = value

    async def update_progress(self, current: Union[int, float], message: str = None):
        self.current_progress = current
        redis_producer = self.event_dispatcher.redis_producer

        def _pipe_builder():
            pipe = redis_producer.pipeline()
            tracker_key = f'bgtask.{self.task_id}'
            pipe.hmset_dict(tracker_key, {
                'current': str(current),
                'total': str(self.total_progress),
                'msg': message or '',
                'last_update': str(time.time()),
            })
            pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            return pipe

        await redis.execute_with_retries(_pipe_builder, max_retries=2)
        await self.event_dispatcher.produce_event(
            'task_updated',
            (str(self.task_id), current, self.total_progress, message, )
        )


BackgroundTask = Callable[[ProgressReporter], Awaitable[Optional[str]]]


class BackgroundTaskManager:
    event_dispatcher: EventDispatcher
    ongoing_tasks: Set[asyncio.Task]

    def __init__(self, event_dispatcher: EventDispatcher) -> None:
        self.event_dispatcher = event_dispatcher
        self.ongoing_tasks = set()

    async def start_background_task(
        self,
        coro: BackgroundTask,
        name: str = None,
        sched: Scheduler = None,
    ) -> uuid.UUID:
        task_id = uuid.uuid4()
        redis_producer = self.event_dispatcher.redis_producer

        def _pipe_builder():
            pipe = redis_producer.pipeline()
            tracker_key = f'bgtask.{task_id}'
            now = str(time.time())
            pipe.hmset_dict(tracker_key, {
                'status': 'started',
                'current': '0',
                'total': '0',
                'msg': '',
                'started_at': now,
                'last_update': now,
            })
            pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            return pipe

        await redis.execute_with_retries(_pipe_builder)

        if sched:
            # aiojobs' Scheduler doesn't support add_done_callback yet
            raise NotImplementedError
        else:
            task = asyncio.create_task(self._wrapper_task(coro, task_id, name))
            self.ongoing_tasks.add(task)
            task.add_done_callback(self.ongoing_tasks.remove)
        return task_id

    async def _wrapper_task(
        self,
        coro: BackgroundTask,
        task_id: uuid.UUID,
        task_name: Optional[str],
    ) -> None:
        task_result: TaskResult
        reporter = ProgressReporter(self.event_dispatcher, task_id)
        message = ''
        try:
            message = await coro(reporter) or ''
            task_result = 'task_done'
        except asyncio.CancelledError:
            task_result = 'task_cancelled'
        except Exception as e:
            task_result = 'task_failed'
            message = repr(e)
        finally:
            redis_producer = self.event_dispatcher.redis_producer

            def _pipe_builder():
                pipe = redis_producer.pipeline()
                tracker_key = f'bgtask.{task_id}'
                pipe.hmset_dict(tracker_key, {
                    'status': task_result[5:],  # strip "task_"
                    'msg': message,
                    'last_update': str(time.time()),
                })
                pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
                return pipe

            await redis.execute_with_retries(_pipe_builder, max_retries=2)
            await self.event_dispatcher.produce_event(
                task_result,
                (str(task_id), message, )
            )
            log.info('{} ({}): {}', task_id, task_name or '', task_result)

    async def shutdown(self) -> None:
        log.info('Clenaing up remaining tasks...')
        for task in self.ongoing_tasks:
            task.cancel()
            await task
