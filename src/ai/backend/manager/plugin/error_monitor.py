import logging
import sys
import traceback
from typing import Any, Optional

from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId
)
from ..models import error_logs, LogSeverity

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))


class ErrorMonitor:
    def __init__(self, app):
        self.dbpool = app['dbpool']
        app['event_dispatcher'].consume('agent_error', app, self.handle_agent_error)

    async def capture_exception(self, exc: Optional[BaseException] = None, user: Any = None,
                                context_env: Any = None,
                                severity: Optional[LogSeverity] = None):
        if exc:
            tb = exc.__traceback__
        else:
            _, exc, tb = sys.exc_info()
        exc_type: Any = type(exc)
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

    async def handle_agent_error(self, app: web.Application, agent_id: AgentId, event_name: str,
                                 message: str,
                                 traceback: str = None,
                                 user: Any = None,
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
        log.debug('Agent log collected: {}', message)
