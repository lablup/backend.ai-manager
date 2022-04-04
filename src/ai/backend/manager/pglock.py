from __future__ import annotations

from typing import Any

from ai.backend.common.distributed import AbstractDistributedLock

from .models.utils import ExtendedAsyncSAEngine
from .defs import LockID


class PgAdvisoryLock(AbstractDistributedLock):

    def __init__(self, db: ExtendedAsyncSAEngine, lock_id: LockID) -> None:
        self.db = db
        self.lock_id = lock_id
        self._conn = None

    async def __aenter__(self) -> Any:
        self._conn = self.db.advisory_lock(self.lock_id)
        await self._conn.__aenter__()

    async def __aexit__(self, *exc_info) -> bool | None:
        assert self._conn is not None
        try:
            return await self._conn.__aexit__(*exc_info)
        finally:
            self._conn = None
