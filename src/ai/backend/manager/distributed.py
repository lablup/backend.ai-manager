from __future__ import annotations

import asyncio
from typing import (
    Any,
    Awaitable,
    Callable,
    Final,
    TYPE_CHECKING,
)

from aioredis import Redis
from aioredlock import Aioredlock, LockError

if TYPE_CHECKING:
    from ai.backend.common.types import AgentId
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
        key: str,
        func: Callable[[], Awaitable[None]],
        interval: float = 10.0,
    ) -> None:
        self._lock_manager = Aioredlock([redis])
        self._event_dispatcher = event_dispatcher
        self.event_key = f"timer.{key}"
        self.lock_key = f"timer.{key}.lock"
        self.func = func
        self.interval = interval

    async def generate_tick(self) -> None:
        try:
            while True:
                try:
                    async with (await self._lock_manager.lock(self.lock_key)):
                        await self._event_dispatcher.produce_event(self.event_key)
                        await asyncio.sleep(self.interval)
                except LockError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            await self._lock_manager.destroy()

    async def _tick(self, context: Any, agent_id: AgentId, event_name: str) -> None:
        await self.func()

    async def join(self) -> None:
        self._evhandler = self._event_dispatcher.consume(self.event_key, None, self._tick)
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        self._event_dispatcher.unconsume(self.event_key, self._evhandler)
        self._tick_task.cancel()
        await self._tick_task
