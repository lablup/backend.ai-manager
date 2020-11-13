from __future__ import annotations

import asyncio
from typing import (
    Final,
    TYPE_CHECKING,
)

from aioredis import Redis
from aioredlock import Aioredlock, LockError

if TYPE_CHECKING:
    from ..gateway.events import EventDispatcher


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _lock_manager: Final[Aioredlock]
    _event_dispatcher: Final[EventDispatcher]

    def __init__(
        self,
        redis: Redis,
        event_dispatcher: EventDispatcher,
        event_name: str,
        interval: float = 10.0,
    ) -> None:
        self._lock_manager = Aioredlock([redis])
        self._event_dispatcher = event_dispatcher
        self.event_name = event_name
        self.lock_key = f"timer.{event_name}.lock"
        self.interval = interval

    async def generate_tick(self) -> None:
        try:
            while True:
                try:
                    async with (
                        await self._lock_manager.lock(self.lock_key, lock_timeout=self.interval)
                    ) as lock:
                        await self._event_dispatcher.produce_event(self.event_name)
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
