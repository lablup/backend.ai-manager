import logging
import sys
import traceback
from typing import Any

from ai.backend.common.logging import BraceStyleAdapter
from ..models import error_logs, LogSeverity

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))

class ErrorMonitor:
    def __init__(self, dbpool):
        self.dbpool = dbpool

    async def capture_exception(self, exc: Exception = None, user = None, context_env: Any = None,
                                severity: LogSeverity = None):
        if exc:
            exc_type = type(exc)
            tb = exc.__traceback__
        else:
            exc_type, exc, tb = sys.exc_info()
        if severity is None:
            severity = LogSeverity.ERROR
        async with self.dbpool.acquire() as conn, conn.begin():
            query = error_logs.insert().values({
                'severity': severity,
                'source': 'manager',
                'user': user,
                'message': ''.join(traceback.format_exception_only(exc_type, exc)).strip(),
                'context_lang': 'python',
                'context_env': context_env,
                'traceback': ''.join(traceback.format_tb(tb)).strip()
            })
            await conn.execute(query)
        log.debug('Manager log collected: {}', str(exc))

    async def handle_agent_error(app: web.Application, agent_id: AgentId, event_name: str,
                                message: str,
                                traceback: str = None,
                                user = None,
                                context_env: Any = None,
                                severity: LogSeverity = None) -> None:
        if severity is None:
            severity = LogSeverity.ERROR
        async with self.dbpool.acquire() as conn, conn.begin():
            query = error_logs.insert().values({
                'severity': severity,
                'source': 'agent',
                'user': user,
                'message': message,
                'context_lang': 'python',
                'context_env': context_env,
                'traceback': traceback
            })
            await conn.execute(query)
        log.debug('Agent log collected: {}', str(exc))
