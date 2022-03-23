from __future__ import annotations

import asyncio
from typing import (
    Callable,
    Final,
    TYPE_CHECKING,
)

from .defs import AdvisoryLock

if TYPE_CHECKING:
    from ai.backend.common.etcd import AsyncEtcd
    from ai.backend.common.events import AbstractEvent, EventProducer


class GlobalTimer:

    """
    Executes the given async function only once in the given interval,
    uniquely among multiple manager instances across multiple nodes.
    """

    _event_producer: Final[EventProducer]

    def __init__(
        self,
        etcd: AsyncEtcd,
        timer_name: AdvisoryLock,
        event_producer: EventProducer,
        event_factory: Callable[[], AbstractEvent],
        interval: float = 10.0,
        initial_delay: float = 0.0,
    ) -> None:
        self.etcd = etcd
        self.timer_name = timer_name
        self._event_producer = event_producer
        self._event_factory = event_factory
        self._stopped = False
        self.interval = interval
        self.initial_delay = initial_delay

    async def generate_tick(self) -> None:
        try:
            await asyncio.sleep(self.initial_delay)
            if self._stopped:
                return
            while True:
                timeout_seconds = int(await self.etcd.get('config/etcd_lock/timeout') or '60')
                async with self.etcd.etcd.with_lock(self.timer_name.value, timeout=timeout_seconds):
                    if self._stopped:
                        return
                    await self._event_producer.produce_event(self._event_factory())
                    if self._stopped:
                        return
                    await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass

    async def join(self) -> None:
        self._tick_task = asyncio.create_task(self.generate_tick())

    async def leave(self) -> None:
        self._stopped = True
        await asyncio.sleep(0)
        if not self._tick_task.done():
            try:
                self._tick_task.cancel()
                await self._tick_task
            except asyncio.CancelledError:
                pass
