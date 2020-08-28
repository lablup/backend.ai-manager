import logging
import sys
import traceback
from typing import Any, Optional, Mapping

from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId
)
from ai.backend.common.plugin import AbstractPlugin
from ..models import error_logs, LogSeverity
from .exceptions import PluginError

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.server'))


class ErrorMonitor(AbstractPlugin):
    async def init(self, context: Any = None) -> None:
        if context is None or 'app' not in context:
            raise PluginError('App was not passed in to ErrorMonitor plugin')
        app = context['app']
        app['event_dispatcher'].consume('agent_error', app, self.handle_agent_error)
        self.dbpool = app['dbpool']

    async def cleanup(self) -> None:
        pass

    async def update_plugin_config(self, plugin_config: Mapping[str, Any]) -> None:
        pass

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
