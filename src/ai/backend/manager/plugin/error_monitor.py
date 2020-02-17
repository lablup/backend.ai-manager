from typing import Any

import sys

from ..models import error_logs, LogSeverity, GUID


class ErrorMonitor:
    def __init__(self, dbpool):
        self.dbpool = dbpool

    async def capture_exception(self, exc: Exception, user: GUID = None, context_env: Any = None,
                                severity: LogSeverity = None):
        if exc:
            tb = exc.__traceback__
        else:
            _, exc, tb = sys.exc_info()
        if severity is None:
            severity = LogSeverity.ERROR
        async with self.dbpool.acquire() as conn, conn.begin():
            query = error_logs.insert().values({
                'severity': severity,
                'source': 'manager',
                'user': user,
                'message': str(exc),
                'context_lang': 'python',
                'context_env': context_env,
                'traceback': tb
            })
            await conn.execute(query)
