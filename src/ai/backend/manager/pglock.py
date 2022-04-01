from __future__ import annotations

from typing import Any

from ai.backend.common.distributed import AbstractDistributedLock

from .models.utils import ExtendedAsyncSAEngine
from .defs import AdvisoryLock


class PgAdvisoryLock(AbstractDistributedLock):

    def __init__(self, db: ExtendedAsyncSAEngine, timer_id: AdvisoryLock) -> None:
        self.db = db
        self.timer_id = timer_id

    async def __aenter__(self) -> Any:
        await self.db.advisory_lock(self.timer_id).__aenter__()

    async def __aexit__(self, *exc_info) -> bool | None:
        return await self.db.advisory_lock(self.timer_id).__aexit__(*exc_info)
