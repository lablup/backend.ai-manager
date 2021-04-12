from __future__ import annotations

import logging
import sys
import traceback
from typing import Any, Mapping, TYPE_CHECKING

from ai.backend.common.events import AgentErrorEvent
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AgentId,
    LogSeverity,
)
from ai.backend.common.plugin.monitor import AbstractErrorReporterPlugin

from ..models import error_logs

if TYPE_CHECKING:
    from ai.backend.manager.api.context import RootContext

log = BraceStyleAdapter(logging.getLogger(__name__))


class ErrorMonitor(AbstractErrorReporterPlugin):

    async def init(self, context: Any = None) -> None:
        root_ctx: RootContext = context['_root.context']  # type: ignore
        self.event_dispatcher = root_ctx.event_dispatcher
        self._evh = self.event_dispatcher.consume(AgentErrorEvent, None, self.handle_agent_error)
        self.db = root_ctx.db

    async def cleanup(self) -> None:
        self.event_dispatcher.unconsume(self._evh)

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

        async with self.db.begin() as conn:
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
        context: None,
        source: AgentId,
        event: AgentErrorEvent,
    ) -> None:
        async with self.db.begin() as conn:
            query = error_logs.insert().values({
                'severity': event.severity,
                'source': source,
                'user': event.user,
                'message': event.message,
                'context_lang': 'python',
                'context_env': event.context_env,
                'traceback': event.traceback
            })
            await conn.execute(query)
        log.debug(
            'collected AgentErrorEvent: [{}:{}] {}',
            source, event.severity, event.message,
        )
