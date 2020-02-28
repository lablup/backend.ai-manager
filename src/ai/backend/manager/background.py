import asyncio
from asyncio import AbstractEventLoop, Task
import functools
import logging
from typing import Callable, Set, Union
import uuid

from aiojobs import Scheduler

from ai.backend.common.logging import BraceStyleAdapter

from ..gateway.events import EventDispatcher

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.stream'))


class ProgressReporter:
    event_dispatcher: EventDispatcher
    total_progress: Union[int, float]
    current_progress: Union[int, float]
    task_id: uuid.UUID

    def __init__(self, event_dispatcher: EventDispatcher,
                       task_id: uuid.UUID):
        self.event_dispatcher = event_dispatcher
        self.task_id = task_id

    async def set_progress_total(self, value: Union[int, float]):
        self.total_progress = value

    async def update_progress(self, current: Union[int, float], message: str = None):
        self.current_progress = current
        await self.event_dispatcher.produce_event(
            'task_update',
            (str(self.task_id), current, self.total_progress, message, )
        )


class BackgroundTaskManager:
    event_dispatcher: EventDispatcher
    loop: AbstractEventLoop
    ongoing_tasks: Set[Task]

    def __init__(self, event_dispatcher, loop=None):
        self.event_dispatcher = event_dispatcher
        self.loop = loop or asyncio.get_event_loop()
        self.ongoing_tasks: Set[Task] = set()

    def start_background_task(self, coro: Callable,
                                    name: str = None,
                                    sched: Scheduler = None) -> uuid.UUID:
        task_id = uuid.uuid4()
        async def _callback_wrapper():
            reporter = ProgressReporter(self.event_dispatcher, task_id)
            try:
                ret = await coro(reporter)
                task_result = 'task_done'
            except asyncio.CancelledError:
                task_result = 'task_cancel'
            except:
                task_result = 'task_fail'
            await self.done_cb(task_id, task_result, task_name=name)
        p = _callback_wrapper()

        if sched:
            # aiojobs' Scheduler doesn't support add_done_callback yet
            raise NotImplementedError
        else:
            task: Task = self.loop.create_task(p)
            task.add_done_callback(self.task_cleanup)
            self.ongoing_tasks.add(task)
        return task_id

    async def done_cb(self, task_id, task_result, task_name=None):
        await self.event_dispatcher.produce_event(
            task_result,
            (str(task_id), )
        )

        if task_name is None:
            task_name = ''
        log.info('{} ({}): {}', task_id, task_name, task_result)

    def task_cleanup(self, task):
        self.ongoing_tasks.remove(task)

    async def shutdown(self):
        log.info('Clenaing up remaining tasks...')
        for task in self.ongoing_tasks:
            task.cancel()
            await task
