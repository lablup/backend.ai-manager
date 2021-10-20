from __future__ import annotations

import asyncio
from decimal import Decimal
import queue
import threading
import time
from typing import (
    Any,
    Iterable,
    Optional,
    TYPE_CHECKING,
)

import attr

from ai.backend.common.events import AbstractEvent, EventDispatcher, EventProducer

from ai.backend.manager.defs import REDIS_STREAM_DB, AdvisoryLock
from ai.backend.manager.distributed import GlobalTimer
from ai.backend.manager.models.utils import connect_database

if TYPE_CHECKING:
    from ai.backend.common.types import AgentId

    from ai.backend.manager.config import LocalConfig, SharedConfig


def drange(start: Decimal, stop: Decimal, step: Decimal) -> Iterable[Decimal]:
    while start < stop:
        yield start
        start += step


def dslice(start: Decimal, stop: Decimal, num: int):
    """
    A simplified version of numpy.linspace with default options
    """
    delta = stop - start
    step = delta / (num - 1)
    yield from (start + step * Decimal(tick) for tick in range(0, num))


@attr.s(slots=True, frozen=True)
class NoopEvent(AbstractEvent):
    name = "_noop"

    test_id: str = attr.ib()

    def serialize(self) -> tuple:
        return (self.test_id, )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


class TimerNode(threading.Thread):

    stop_event: Optional[asyncio.Event]
    loop: Optional[asyncio.AbstractEventLoop]

    def __init__(
        self,
        interval: float,
        thread_idx: int,
        test_id: str,
        local_config: LocalConfig,
        shared_config: SharedConfig,
        event_records: queue.Queue[float],
    ) -> None:
        super().__init__()
        self.interval = interval
        self.thread_idx = thread_idx
        self.test_id = test_id
        self.local_config = local_config
        self.shared_config = shared_config
        self.event_records = event_records
        self.stop_event = None
        self.loop = None

    async def timer_node_async(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.stop_event = asyncio.Event()

        async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
            self.event_records.put(time.monotonic())

        event_dispatcher = await EventDispatcher.new(self.shared_config.data['redis'],
                                                     db=REDIS_STREAM_DB)
        event_producer = await EventProducer.new(self.shared_config.data['redis'], db=REDIS_STREAM_DB)
        event_dispatcher.consume(NoopEvent, None, _tick)

        async with connect_database(self.local_config) as db:
            timer = GlobalTimer(
                db,
                AdvisoryLock.LOCKID_TEST,
                event_producer,
                lambda: NoopEvent(self.test_id),
                self.interval,
            )
            try:
                await timer.join()
                await self.stop_event.wait()
            finally:
                await timer.leave()
                await event_producer.close()
                await event_dispatcher.close()

    def run(self) -> None:
        asyncio.run(self.timer_node_async())


async def test_global_timer(test_id, local_config, shared_config, database_engine) -> None:
    event_records: queue.Queue[float] = queue.Queue()
    num_threads = 7
    num_records = 0
    delay = 3.0
    interval = 0.5
    target_count = (delay / interval)
    threads = []
    for thread_idx in range(num_threads):
        timer_node = TimerNode(
            interval,
            thread_idx,
            test_id,
            local_config,
            shared_config,
            event_records,
        )
        threads.append(timer_node)
        timer_node.start()
    print(f"spawned {num_threads} timers")
    time.sleep(delay)
    print("stopping timers")
    for timer_node in threads:
        assert timer_node.loop is not None
        assert timer_node.stop_event is not None
        timer_node.loop.call_soon_threadsafe(timer_node.stop_event.set)
    print("joining timer threads")
    for timer_node in threads:
        timer_node.join()
    print("checking records")
    while True:
        try:
            tick = event_records.get_nowait()
            print(tick)
        except queue.Empty:
            break
        num_records += 1
    print(f"{num_records=}")
    assert target_count - 2 <= num_records <= target_count + 2
