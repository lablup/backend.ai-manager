from __future__ import annotations

import asyncio
from decimal import Decimal
import queue
import random
import threading
import time
from typing import (
    Any,
    Iterable,
    Optional,
    TYPE_CHECKING,
)

import aioredis
import attr

from ai.backend.common.events import AbstractEvent, EventDispatcher, EventProducer

from ai.backend.manager.defs import REDIS_STREAM_DB, AdvisoryLock
from ai.backend.manager.distributed import GlobalTimer
from ai.backend.manager.models.utils import ExtendedAsyncSAEngine

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

    def __init__(
        self,
        join_delay: float,
        leave_delay: float,
        interval: float,
        thread_idx: int,
        test_id: str,
        db: ExtendedAsyncSAEngine,
        local_config: LocalConfig,
        shared_config: SharedConfig,
        event_records: queue.Queue[float],
    ) -> None:
        super().__init__()
        self.join_delay = join_delay
        self.leave_delay = leave_delay
        self.interval = interval
        self.thread_idx = thread_idx
        self.test_id = test_id
        self.db = db
        self.local_config = local_config
        self.shared_config = shared_config
        self.event_records = event_records

    async def timer_node_async(self) -> None:

        async def redis_connector():
            redis_url = self.shared_config.get_redis_url(db=REDIS_STREAM_DB)
            return await aioredis.create_redis(str(redis_url))

        async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
            self.event_records.put(time.monotonic())

        event_dispatcher = await EventDispatcher.new(redis_connector)
        event_producer = await EventProducer.new(redis_connector)
        event_dispatcher.consume(NoopEvent, None, _tick)

        await asyncio.sleep(self.join_delay)
        timer = GlobalTimer(
            self.db,
            AdvisoryLock.LOCKID_TEST,
            event_producer,
            lambda: NoopEvent(self.test_id),
            self.interval,
        )
        try:
            await timer.join()
            await asyncio.sleep(self.leave_delay)
        finally:
            await timer.leave()
            await event_producer.close()
            await event_dispatcher.close()

    def run(self) -> None:
        asyncio.run(self.timer_node_async())


def test_global_timer(test_id, local_config, shared_config, database_engine) -> None:
    event_records: queue.Queue[float] = queue.Queue()
    num_threads = 7
    num_records = 0
    q = Decimal('0.00')
    interval = Decimal('1')
    join_delays = [
        interval * x.quantize(q)
        for x in dslice(Decimal('0'), Decimal('2'), num_threads)
    ]
    leave_delays = [
        interval * (x.quantize(q) + Decimal('2.5'))
        for x in dslice(Decimal('1'), Decimal('3'), num_threads)
    ]
    random.shuffle(join_delays)
    random.shuffle(leave_delays)
    print('')
    print(join_delays)
    print(leave_delays)

    active_ticks = {
        str(Decimal(t).quantize(q)): 0
        for t in drange(
            min(join_delays),
            max(Decimal(j + leave_delays[i]) for i, j in enumerate(join_delays)),
            interval,
        )
    }
    print(list(active_ticks.keys()))
    for idx, j in enumerate(join_delays):
        for t in drange(j, j + leave_delays[idx], interval):
            quantized_tick = t - (t % interval)
            active_ticks[str(quantized_tick.quantize(q))] += 1
    target_count = len([*filter(lambda v: v > 0, active_ticks.values())])
    print(f"{target_count=}")

    threads = []
    for thread_idx in range(num_threads):
        timer_node = TimerNode(
            float(join_delays[thread_idx]),
            float(leave_delays[thread_idx]),
            float(interval),
            thread_idx,
            test_id,
            database_engine,
            local_config,
            shared_config,
            event_records,
        )
        threads.append(timer_node)
        timer_node.start()
    for timer_node in threads:
        timer_node.join()
    prev_record: Optional[float] = None
    while True:
        try:
            tick = event_records.get_nowait()
            print(tick)
            if prev_record is not None:
                assert tick - prev_record < interval * Decimal('1.8')
            prev_record = tick
        except queue.Empty:
            break
        num_records += 1
    print(f"{num_records=}")
    assert target_count - 2 <= num_records <= target_count + 2
