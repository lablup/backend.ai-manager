from __future__ import annotations

import asyncio
from decimal import Decimal
from multiprocessing import Process, SimpleQueue
from signal import SIGINT, SIGTERM
import signal
import time
from typing import (
    Any,
    Iterable,
    List,
    TYPE_CHECKING,
    Mapping,
    Tuple,
)

import attr
import pytest

from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.events import AbstractEvent, EventDispatcher, EventProducer
from ai.backend.common.types import EtcdRedisConfig

from ai.backend.manager.defs import REDIS_STREAM_DB, AdvisoryLock
from ai.backend.manager.distributed import GlobalTimer
from ai.backend.manager.models.utils import connect_database

if TYPE_CHECKING:
    # from ai.backend.common.types import AgentId, EtcdRedisConfig, HostPortPair
    from ai.backend.common.types import AgentId


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


def timer_node_process(
    test_id: str, node_id: str,
    redis: EtcdRedisConfig,
    etcd_args: Tuple[Any],
    etcd_kwargs: Mapping[Any, Any],
    interval: float,
    event_records: Any,
):
    stop_event = asyncio.Event()

    async def _async_job():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
            print("_tick")

            event_records.put(time.monotonic())

        event_dispatcher = await EventDispatcher.new(
            redis,
            db=REDIS_STREAM_DB,
            node_id=node_id,
        )
        event_producer = await EventProducer.new(
            redis,
            db=REDIS_STREAM_DB,
        )
        event_dispatcher.consume(NoopEvent, None, _tick)

        timer = GlobalTimer(
            AsyncEtcd(*etcd_args, **etcd_kwargs),
            AdvisoryLock.LOCKID_TEST,
            event_producer,
            lambda: NoopEvent(test_id),
            interval,
        )

        try:
            await timer.join()
            await stop_event.wait()
        finally:
            await timer.leave()
            await event_producer.close()
            await event_dispatcher.close()

    def _on_sigterm(signum, frame):
        print('timer_node_process(): signal called')
        stop_event.set()
    signal.signal(SIGTERM, _on_sigterm)
    signal.signal(SIGINT, _on_sigterm)
    asyncio.run(_async_job())
    return 0


def test_global_timer(test_id, local_config, shared_config, database_engine) -> None:
    event_records: Any = SimpleQueue()
    processes: List[Process] = []
    num_threads = 7
    num_records = 0
    delay = 3.0
    interval = 0.5
    target_count = (delay / interval)

    etcd_args = (
        local_config['etcd']['addr'],
        local_config['etcd']['namespace'],
        {ConfigScopes.GLOBAL: ''},
    )
    etcd_kwargs = {}
    if local_config['etcd']['user']:
        etcd_kwargs = {
            'credentials': {
                'user': local_config['etcd']['user'],
                'password': local_config['etcd']['password'],
            },
        }

    for i in range(num_threads):
        args = (
            test_id, local_config['manager']['id'],
            local_config['redis'], etcd_args, etcd_kwargs, interval, event_records,
        )
        p = Process(target=timer_node_process, args=args)
        p.start()
        processes.append(p)

    print(f"spawned {num_threads} timers")
    print(processes)
    print("waiting")
    time.sleep(delay + 1)
    print("joining timer threads")
    for timer_node in processes:
        timer_node.terminate()
    print("checking records")
    print(event_records)
    num_records = 0
    while not event_records.empty():
        num_records += 1
        event_records.get()

    print(f"num_records={num_records}")
    assert target_count - 2 <= num_records <= target_count + 2


@pytest.mark.asyncio
async def test_global_timer_join_leave(test_id, local_config, shared_config, database_engine) -> None:

    event_records = []

    async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
        print("_tick")
        event_records.append(time.monotonic())

    event_dispatcher = await EventDispatcher.new(
        shared_config.data['redis'],
        db=REDIS_STREAM_DB,
        node_id=local_config['manager']['id'],
    )
    event_producer = await EventProducer.new(
        shared_config.data['redis'],
        db=REDIS_STREAM_DB,
    )
    event_dispatcher.consume(NoopEvent, None, _tick)

    for _ in range(10):
        async with connect_database(local_config) as db:
            timer = GlobalTimer(
                db,
                AdvisoryLock.LOCKID_TEST,
                event_producer,
                lambda: NoopEvent(test_id),
                0.01,
            )
            await timer.join()
            await timer.leave()

    await event_producer.close()
    await event_dispatcher.close()
