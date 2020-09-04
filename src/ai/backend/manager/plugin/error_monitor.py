import logging
import sys
import traceback
from typing import Any, Mapping

from aiohttp import web

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId
)
from ai.backend.common.plugin.monitor import AbstractErrorReporterPlugin
from ..models import error_logs, LogSeverity
from .exceptions import PluginError

log = BraceStyleAdapter(logging.getLogger(__name__))


class ErrorMonitor(AbstractErrorReporterPlugin):
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

    async def capture_message(self, message: str) -> None:
        pass

    async def capture_exception(
        self,
        exc_instance: Exception = None,
        context: Mapping[str, Any] = None,
    ) -> None:
        if exc_instance:
            tb = exc_instance.__traceback__
        else:
            _, sys_exc_instance, tb = sys.exc_info()
            if (
                isinstance(sys_exc_instance, BaseException)
                and not isinstance(sys_exc_instance, Exception)
            ):
                # bypass BaseException as they are used for controlling the process/coroutine lifecycles
                # instead of indicating actual errors
                return
            exc_instance = sys_exc_instance
        exc_type: Any = type(exc_instance)

        if context is None or 'severity' not in context:
            severity = LogSeverity.ERROR
        else:
            severity = context['severity']
        if context is None or 'user' not in context:
            user = None
        else:
            user = context['user']

        async with self.dbpool.acquire() as conn, conn.begin():
            query = error_logs.insert().values({
                'severity': severity,
                'source': 'manager',
                'user': user,
                'message': ''.join(traceback.format_exception_only(exc_type, exc_instance)).strip(),
                'context_lang': 'python',
                'context_env': context,
                'traceback': ''.join(traceback.format_tb(tb)).strip()
            })
            await conn.execute(query)
        log.debug('Manager log collected: {}', str(exc_instance))

    async def handle_agent_error(
        self,
        app: web.Application,
        agent_id: AgentId,
        event_name: str,
        # additional event args
        message: str,
        traceback: str = None,
        user: Any = None,
        context_env: Mapping[str, Any] = None,
        severity: LogSeverity = None,
    ) -> None:
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
