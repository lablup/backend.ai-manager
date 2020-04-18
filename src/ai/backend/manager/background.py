from __future__ import annotations

import asyncio
import logging
import time
from typing import (
    Awaitable, Callable, Final, Optional,
    Literal, Union,
    Set,
)
import uuid

from aiojobs import Scheduler
import attr

from ai.backend.common import redis
from ai.backend.common.logging import BraceStyleAdapter

from ..gateway.events import EventDispatcher
from .types import BackgroundTaskEventArgs

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.background'))

MAX_BGTASK_ARCHIVE_PERIOD = 86400  # 24  hours

TaskResult = Literal['task_done', 'task_cancelled', 'task_failed']


class ProgressReporter:
    event_dispatcher: Final[EventDispatcher]
    task_id: Final[uuid.UUID]
    total_progress: Union[int, float]
    current_progress: Union[int, float]

    def __init__(
        self,
        event_dispatcher: EventDispatcher,
        task_id: uuid.UUID,
        current_progress: int = 0,
        total_progress: int = 0,
    ) -> None:
        self.event_dispatcher = event_dispatcher
        self.task_id = task_id
        self.current_progress = current_progress
        self.total_progress = total_progress

    async def update(self, increment: Union[int, float] = 0, message: str = None):
        self.current_progress += increment
        # keep the state as local variables because they might be changed
        # due to interleaving at await statements below.
        current, total = self.current_progress, self.total_progress
        redis_producer = self.event_dispatcher.redis_producer

        def _pipe_builder():
            pipe = redis_producer.pipeline()
            tracker_key = f'bgtask.{self.task_id}'
            pipe.hmset_dict(tracker_key, {
                'current': str(current),
                'total': str(total),
                'msg': message or '',
                'last_update': str(time.time()),
            })
            pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            return pipe

        await redis.execute_with_retries(_pipe_builder, max_retries=2)
        await self.event_dispatcher.produce_event(
            'task_updated', (
                attr.asdict(BackgroundTaskEventArgs(
                    str(self.task_id),
                    message=message,
                    current_progress=current,
                    total_progress=total,
                )),
            )
        )


BackgroundTask = Callable[[ProgressReporter], Awaitable[Optional[str]]]


class BackgroundTaskManager:
    event_dispatcher: EventDispatcher
    ongoing_tasks: Set[asyncio.Task]

    def __init__(self, event_dispatcher: EventDispatcher) -> None:
        self.event_dispatcher = event_dispatcher
        self.ongoing_tasks = set()

    async def start(
        self,
        func: BackgroundTask,
        name: str = None,
        *,
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
        reporter = ProgressReporter(self.event_dispatcher, task_id)
        message = ''
        try:
            message = await func(reporter) or ''
            task_result = 'task_done'
        except asyncio.CancelledError:
            task_result = 'task_cancelled'
        except Exception as e:
            task_result = 'task_failed'
            message = repr(e)
            log.exception("Task {} ({}): unhandled error", task_id, task_name)
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
                task_result, (
                    attr.asdict(BackgroundTaskEventArgs(
                        str(task_id),
                        message=message,
                    )),
                )
            )
            log.info('Task {} ({}): {}', task_id, task_name or '', task_result)

    async def shutdown(self) -> None:
        log.info('Cancelling remaining background tasks...')
        for task in self.ongoing_tasks.copy():
            task.cancel()
            await task
