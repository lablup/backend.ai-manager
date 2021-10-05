from __future__ import annotations

import asyncio
from re import S
from typing import (
    Callable,
    Final,
    TYPE_CHECKING,
)

from aioredis import Redis
from aioredis.lock import Lock

if TYPE_CHECKING:
    from ai.backend.common.events import AbstractEvent, EventProducer


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _lock_manager: Final[Lock]
    _event_producer: Final[EventProducer]

    def __init__(
        self,
        redis: Redis,
        timer_name: str,
        event_producer: EventProducer,
        event_factory: Callable[[], AbstractEvent],
        interval: float = 10.0,
        initial_delay: float = 0.0,
    ) -> None:
        self.lock_key = f"timer.{timer_name}.lock"
        self.interval = interval
        self.initial_delay = initial_delay

        self._lock_manager = Lock(redis, self.lock_key, timeout=self.interval)
        self._event_producer = event_producer
        self._event_factory = event_factory

    async def generate_tick(self) -> None:
        try:
            await asyncio.sleep(self.initial_delay)
            while True:
                await self._lock_manager.acquire(blocking=True)
                await self._event_producer.produce_event(self._event_factory())
                await self._lock_manager.reacquire()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass
        finally:
            if await self._lock_manager.owned():
                await self._lock_manager.release()

    async def join(self) -> None:
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        self._tick_task.cancel()
        await self._tick_task
