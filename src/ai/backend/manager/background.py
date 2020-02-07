import asyncio
from asyncio import AbstractEventLoop, Task
import functools
import logging
from typing import Callable, List, Union
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
    loop: AbstractEventLoop

    def __init__(self, event_dispatcher: EventDispatcher,
                       task_id: uuid.UUID,
                       loop: AbstractEventLoop):
        self.event_dispatcher = event_dispatcher
        self.task_id = task_id
        self.loop = loop

    def set_progress_total(self, value: Union[int, float]):
        self.total_progress = value

    def update_progress(self, current: Union[int, float]):
        self.current_progress = current
        asyncio.ensure_future(
            self.event_dispatcher.produce_event(
                'task_update',
                (str(self.task_id), current, self.total_progress, )
            )
        )


class BackgroundTask:
    event_dispatcher: EventDispatcher
    loop: AbstractEventLoop
    ongoing_tasks: List[Task]

    def __init__(self, event_dispatcher, loop=None):
        self.event_dispatcher = event_dispatcher
        self.loop = loop or asyncio.get_event_loop()
        self.ongoing_tasks: List[Task] = []

    def start_background_task(self,
                                    coro: Callable,
                                    sched: Scheduler = None) -> uuid.UUID:
        task_id = uuid.uuid4()
        reporter = ProgressReporter(self.event_dispatcher, task_id, self.loop)
        p = coro(reporter)

        if sched:
            # aiojobs' Scheduler doesn't support add_done_callback yet
            raise NotImplementedError
        else:
            task: Task = self.loop.create_task(p)
            task.add_done_callback(functools.partial(self.done_cb, task_id))
            self.ongoing_tasks.append(task)
        return task_id

    def done_cb(self, task, task_id):
        asyncio.ensure_future(
            self.event_dispatcher.produce_event(
                'task_done',
                (str(task_id), )
            )
        )

    async def shutdown(self):
        log.info('Clenaing up remaining tasks...')
        for task in self.ongoing_tasks:
            task.cancel()
            await task
