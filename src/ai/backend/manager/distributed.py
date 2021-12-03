from __future__ import annotations

import asyncio
from typing import (
    Callable,
    Final,
    TYPE_CHECKING,
)

import aioredis.lock

from ai.backend.common.redis import Lock

if TYPE_CHECKING:
    from ai.backend.common.types import RedisConnectionInfo
    from ai.backend.common.events import AbstractEvent, EventProducer


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _event_producer: Final[EventProducer]

    def __init__(
        self,
        redis_lock: RedisConnectionInfo,
        timer_id: int,
        event_producer: EventProducer,
        event_factory: Callable[[], AbstractEvent],
        interval: float = 10.0,
        initial_delay: float = 0.0,
    ) -> None:
        self._redis_lock = redis_lock
        self._event_producer = event_producer
        self._event_factory = event_factory
        self.timer_id = timer_id
        self.interval = interval
        self.initial_delay = initial_delay

    async def generate_tick(self) -> None:
        lock_key = f'lock:{self.timer_id}'
        redis_client = await self._redis_lock.get_client()
        lock: aioredis.lock.Lock = redis_client.lock(
            lock_key,
            blocking_timeout=0.1,
            lock_class=Lock,
        )
        try:
            await asyncio.sleep(self.initial_delay)
            while True:
                print(f"lock[{self.timer_id}] enter loop")
                async with lock:
                    print(f"lock[{self.timer_id}] acquired")
                    await self._event_producer.produce_event(self._event_factory())
                    await lock.extend(self.interval)
                    await asyncio.sleep(self.interval)
                print(f"lock[{self.timer_id}] released")
        except asyncio.CancelledError:
            pass

    async def join(self) -> None:
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        self._tick_task.cancel()
        await self._tick_task
