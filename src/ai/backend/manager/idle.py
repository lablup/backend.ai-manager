from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from typing import (
    Any,
    ClassVar,
    Mapping,
    Sequence,
    Type,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from aiopg.sa.engine import _PoolAcquireContextManager as SAPool
import aioredis
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import aobject
if TYPE_CHECKING:
    from ai.backend.common.types import AgentId, SessionId

from .distributed import GlobalTimer
from .models import kernels
from .models.kernel import LIVE_STATUS
from ..gateway.defs import REDIS_LIVE_DB
if TYPE_CHECKING:
    from ..gateway.config import SharedConfig
    from ..gateway.events import EventDispatcher

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.idle'))


class BaseIdleChecker(aobject, metaclass=ABCMeta):

    name: ClassVar[str] = "base"

    def __init__(
        self,
        dbpool: SAPool,
        shared_config: SharedConfig,
        event_dispatcher: EventDispatcher,
    ) -> None:
        self._dbpool = dbpool
        self._shared_config = shared_config
        self._event_dispatcher = event_dispatcher

    async def __ainit__(self) -> None:
        self._redis = await aioredis.create_redis(
            str(self._shared_config.get_redis_url(db=REDIS_LIVE_DB))
        )
        config = await self._shared_config.etcd.get_prefix_dict(f"config/idle/checkers/{self.name}")
        if config is not None:
            await self.populate_config(config)
        self.timer = GlobalTimer(self._redis, self._event_dispatcher, 'do_idle_check', 10.0)
        self._evh_idle_check = self._event_dispatcher.consume('do_idle_check', None, self._do_idle_check)
        await self.timer.join()

    async def aclose(self) -> None:
        await self.timer.leave()
        self._event_dispatcher.unconsume('do_idle_check', self._evh_idle_check)
        self._redis.close()
        await self._redis.wait_closed()

    @abstractmethod
    def populate_config(self, config: Mapping[str, Any]) -> None:
        raise NotImplementedError

    async def _do_idle_check(self, context: Any, agent_id: AgentId, event_name: str) -> None:
        log.debug('do_idle_check')
        async with self._dbpool.acquire() as conn:
            query = (
                sa.select([sa.text("*")])
                .select_from(kernels)
                .where(
                    (kernels.c.status.in_(LIVE_STATUS))
                )
            )
            result = await conn.execute(query)
            rows = await result.fetchall()
        for row in rows:
            if not (await self.check_session(row)):
                await self._event_dispatcher.produce_event(
                    "terminate_session",
                    (),
                )


    @abstractmethod
    async def check_session(self, session) -> bool:
        """
        Return True if the session should be kept alive or
        return False if the session should be terminated.
        """
        return True


class TimeoutIdleChecker(BaseIdleChecker):
    """
    Checks the idleness of a session by the elapsed time since last used.
    The usage means processing of any computation requests, such as
    query/batch-mode code execution and having active service-port connections.
    """

    name: ClassVar[str] = "timeout"
    default_idle_timeout: ClassVar[float] = 600.0  # 10 minutes
    # default_idle_timeout: ClassVar[float] = 30.0   # for testing

    async def __ainit__(self) -> None:
        await super().__ainit__()
        self._evh_execution_started = \
            self._event_dispatcher.consume("execution_started", None, self._disable_timeout)
        self._evh_execution_finished = \
            self._event_dispatcher.consume("execution_finished", None, self._update_last_access)
        self._evh_execution_timeout = \
            self._event_dispatcher.consume("execution_timeout", None, self._update_last_access)
        self._evh_execution_cancelled = \
            self._event_dispatcher.consume("execution_cancelled", None, self._update_last_access)

    async def aclose(self) -> None:
        self._event_dispatcher.unconsume("execution_started", self._evh_execution_started)
        self._event_dispatcher.unconsume("execution_finished", self._evh_execution_finished)
        self._event_dispatcher.unconsume("execution_timeout", self._evh_execution_timeout)
        self._event_dispatcher.unconsume("execution_cancelled", self._evh_execution_cancelled)
        await super().aclose()

    async def populate_config(self, config: Mapping[str, Any]) -> None:
        self.idle_timeout = float(config.get('idle_timeout', self.default_idle_timeout))

    async def _disable_timeout(
        self,
        context: Any,
        agent_id: AgentId,
        event_name: str,
        session_id: SessionId,
    ) -> None:
        """
        Disable the timeout check for this particular session during code execution.
        """
        await self._redis.set(f"session.{session_id}.last_access", "0", exists=self._redis.SET_IF_EXIST)

    async def _update_last_access(
        self,
        context: Any,
        agent_id: AgentId,
        event_name: str,
        session_id: SessionId,
    ) -> None:
        """
        Enable the timeout check and reset the last-access timestamp.
        """
        t = await self._redis.time()
        await self._redis.set(f"session.{session_id}.last_access", f"{t:.06f}", expire=86400)

    async def check_session(self, session) -> bool:
        session_id = session['id']
        active_streams = await self._redis.zcount(f"session.{session_id}.active_app_connections")
        if active_streams is not None and active_streams > 0:
            return True
        t = await self._redis.time()
        last_access = await self._redis.get(f"session.{session_id}.last_access")
        if last_access is None or last_access == "0":
            return True
        if session['idle_timeout'] is not None:
            idle_timeout = session['idle_timeout']
        else:
            idle_timeout = self.idle_timeout
        if t - float(last_access) <= idle_timeout:
            return True
        return False


class UtilizationIdleChecker(BaseIdleChecker):
    """
    Checks the idleness of a session by the current utilization of all
    compute devices and agents assigned to it.
    """

    name: ClassVar[str] = "utilization"
    default_cpu_util_threshold: ClassVar[float] = 30.0
    default_accelerator_util_threshold: ClassVar[float] = 10.0
    default_initial_grace_period: ClassVar[float] = 300.0  # allow first 5 minutes to be idle

    async def check_session(self, session) -> bool:
        # last_stat = session['last_stat']
        # TODO: implement
        return True


checker_registry: Mapping[str, Type[BaseIdleChecker]] = {
    TimeoutIdleChecker.name: TimeoutIdleChecker,
    UtilizationIdleChecker.name: UtilizationIdleChecker,
}


async def create_idle_checkers(
    dbpool: SAPool,
    shared_config: SharedConfig,
    event_dispatcher: EventDispatcher,
) -> Sequence[BaseIdleChecker]:
    """
    Create an instance of session idleness checker
    from the given configuration and using the given event dispatcher.
    """
    checkers = await shared_config.etcd.get('config/idle/enabled')
    if not checkers:
        return []
    instances = []
    for checker_name in checkers.split(","):
        checker_cls = checker_registry.get(checker_name, None)
        if checker_cls is None:
            log.warning("ignoring an unknown idle checker name: {checker_name}")
        log.debug(f"initializing idle checker: {checker_name}")
        checker_instance = await checker_cls.new(dbpool, shared_config, event_dispatcher)
        instances.append(checker_instance)
    return instances
