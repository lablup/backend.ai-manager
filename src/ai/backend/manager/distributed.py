from __future__ import annotations

import asyncio
from typing import (
    Callable,
    Final,
    TYPE_CHECKING,
)

from aioredis import Redis
from aioredlock import Aioredlock, LockError

if TYPE_CHECKING:
    from ai.backend.common.events import AbstractEvent, EventProducer


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _lock_manager: Final[Aioredlock]
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
        self._lock_manager = Aioredlock([redis])
        self._event_producer = event_producer
        self._event_factory = event_factory
        self.lock_key = f"timer.{timer_name}.lock"
        self.interval = interval
        self.initial_delay = initial_delay

    async def generate_tick(self) -> None:
        try:
            await asyncio.sleep(self.initial_delay)
            while True:
                try:
                    async with (
                        await self._lock_manager.lock(self.lock_key, lock_timeout=self.interval)
                    ) as lock:
                        await self._event_producer.produce_event(self._event_factory())
                        await self._lock_manager.extend(lock, lock_timeout=self.interval)
                        await asyncio.sleep(self.interval)
                except LockError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            await self._lock_manager.destroy()

    async def join(self) -> None:
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        self._tick_task.cancel()
        await self._tick_task
