from __future__ import annotations

import asyncio
from typing import (
    Callable,
    Final,
    TYPE_CHECKING,
)

from .defs import AdvisoryLock

if TYPE_CHECKING:
    from ai.backend.manager.models.utils import ExtendedAsyncSAEngine
    from ai.backend.common.events import AbstractEvent, EventProducer


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _event_producer: Final[EventProducer]

    def __init__(
        self,
        db: ExtendedAsyncSAEngine,
        timer_id: int,
        event_producer: EventProducer,
        event_factory: Callable[[], AbstractEvent],
        interval: float = 10.0,
        initial_delay: float = 0.0,
    ) -> None:
        self.db = db
        self.timer_id = timer_id
        self._event_producer = event_producer
        self._event_factory = event_factory
        self.interval = interval
        self.initial_delay = initial_delay

    async def generate_tick(self) -> None:
        await asyncio.sleep(self.initial_delay)
        while True:
            async with self.db.advisory_lock(AdvisoryLock(self.timer_id)):
                await self._event_producer.produce_event(self._event_factory())
                await asyncio.sleep(self.interval)

    async def join(self) -> None:
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        if not self._tick_task.done():
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
