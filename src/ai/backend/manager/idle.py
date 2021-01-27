from __future__ import annotations

from abc import ABCMeta, abstractmethod
from contextvars import ContextVar
from datetime import timedelta
import enum
import logging
import math
from typing import (
    Any,
    ClassVar,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Type,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from aiopg.sa.connection import SAConnection
    from aiopg.sa.engine import _PoolAcquireContextManager as SAPool
    from aiopg.sa.result import RowProxy
import aioredis
import sqlalchemy as sa
import trafaret as t

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import aobject, AccessKey
import ai.backend.common.validators as tx
if TYPE_CHECKING:
    from ai.backend.common.types import AgentId, SessionId

from .distributed import GlobalTimer
from .models import kernels, keypairs, keypair_resource_policies
from .models.kernel import LIVE_STATUS
from ..gateway.defs import REDIS_LIVE_DB
if TYPE_CHECKING:
    from ..gateway.config import SharedConfig
    from ..gateway.events import EventDispatcher

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.idle'))


class AppStreamingStatus(enum.Enum):
    NO_ACTIVE_CONNECTIONS = 0
    HAS_ACTIVE_CONNECTIONS = 1


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
        raw_config = await self._shared_config.etcd.get_prefix_dict(f"config/idle/checkers/{self.name}")
        await self.populate_config(raw_config or {})
        self.timer = GlobalTimer(self._redis, self._event_dispatcher, 'do_idle_check', 10.0)
        self._evh_idle_check = self._event_dispatcher.consume('do_idle_check', None, self._do_idle_check)
        await self.timer.join()

    async def aclose(self) -> None:
        await self.timer.leave()
        self._event_dispatcher.unconsume('do_idle_check', self._evh_idle_check)
        self._redis.close()
        await self._redis.wait_closed()

    @abstractmethod
    async def populate_config(self, config: Mapping[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        pass

    async def _do_idle_check(
        self,
        context: None,
        agent_id: AgentId,
        event_name: str,
        *args,
    ) -> None:
        log.debug('do_idle_check(): triggered')
        async with self._dbpool.acquire() as conn:
            query = (
                sa.select([kernels])
                .select_from(kernels)
                .where(
                    (kernels.c.status.in_(LIVE_STATUS))
                )
            )
            result = await conn.execute(query)
            rows = await result.fetchall()
            for row in rows:
                if not (await self.check_session(row, conn)):
                    log.info(f"The {self.name} idle checker triggered termination of s:{row['id']}")
                    await self._event_dispatcher.produce_event(
                        "do_terminate_session",
                        (str(row['id']), "idle-timeout"),
                    )

    @abstractmethod
    async def check_session(self, session: RowProxy, dbconn: SAConnection) -> bool:
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

    _config_iv = t.Dict({
        t.Key('threshold', default="10m"): tx.TimeDuration(),
    }).allow_extra('*')

    idle_timeout: timedelta
    _policy_cache: ContextVar[Dict[AccessKey, Optional[Mapping[str, Any]]]]

    async def __ainit__(self) -> None:
        await super().__ainit__()
        self._policy_cache = ContextVar('_policy_cache')
        self._evh_session_started = self._event_dispatcher.consume(
            "session_started", None, self._session_started_cb)
        self._evh_execution_started = self._event_dispatcher.consume(
            "execution_started", None, self._execution_started_cb)
        self._evh_execution_finished = self._event_dispatcher.consume(
            "execution_finished", None, self._execution_exited_cb)
        self._evh_execution_timeout = self._event_dispatcher.consume(
            "execution_timeout", None, self._execution_exited_cb)
        self._evh_execution_cancelled = self._event_dispatcher.consume(
            "execution_cancelled", None, self._execution_exited_cb)

    async def aclose(self) -> None:
        self._event_dispatcher.unconsume("session_started", self._evh_session_started)
        self._event_dispatcher.unconsume("execution_started", self._evh_execution_started)
        self._event_dispatcher.unconsume("execution_finished", self._evh_execution_finished)
        self._event_dispatcher.unconsume("execution_timeout", self._evh_execution_timeout)
        self._event_dispatcher.unconsume("execution_cancelled", self._evh_execution_cancelled)
        await super().aclose()

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        config = self._config_iv.check(raw_config)
        self.idle_timeout = config['threshold']
        log.info(
            'TimeoutIdleChecker: default idle_timeout = {0:,} seconds',
            self.idle_timeout.total_seconds(),
        )

    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        if status == AppStreamingStatus.HAS_ACTIVE_CONNECTIONS:
            await self._disable_timeout(session_id)
        elif status == AppStreamingStatus.NO_ACTIVE_CONNECTIONS:
            await self._update_timeout(session_id)

    async def _disable_timeout(self, session_id: SessionId) -> None:
        log.debug(f"TimeoutIdleChecker._disable_timeout({session_id})")
        await self._redis.set(f"session.{session_id}.last_access", "0", exist=self._redis.SET_IF_EXIST)

    async def _update_timeout(self, session_id: SessionId) -> None:
        log.debug(f"TimeoutIdleChecker._update_timeout({session_id})")
        t = await self._redis.time()
        await self._redis.set(
            f"session.{session_id}.last_access",
            f"{t:.06f}",
            expire=max(86400, self.idle_timeout.total_seconds() * 2),
        )

    async def _session_started_cb(
        self,
        context: None,
        agent_id: AgentId,
        event_name: str,
        *args,
    ) -> None:
        session_id: SessionId = args[0]
        await self._update_timeout(session_id)

    async def _execution_started_cb(
        self,
        context: None,
        agent_id: AgentId,
        event_name: str,
        *args,
    ) -> None:
        session_id: SessionId = args[0]
        await self._disable_timeout(session_id)

    async def _execution_exited_cb(
        self,
        context: None,
        agent_id: AgentId,
        event_name: str,
        *args,
    ) -> None:
        session_id: SessionId = args[0]
        await self._update_timeout(session_id)

    async def _do_idle_check(
        self,
        context: None,
        agent_id: AgentId,
        event_name: str,
        *args,
    ) -> None:
        cache_token = self._policy_cache.set(dict())
        try:
            return await super()._do_idle_check(context, agent_id, event_name)
        finally:
            self._policy_cache.reset(cache_token)

    async def check_session(self, session: RowProxy, dbconn: SAConnection) -> bool:
        session_id = session['id']
        active_streams = await self._redis.zcount(f"session.{session_id}.active_app_connections")
        if active_streams is not None and active_streams > 0:
            return True
        t = await self._redis.time()
        raw_last_access = await self._redis.get(f"session.{session_id}.last_access")
        if raw_last_access is None or raw_last_access == "0":
            return True
        last_access = float(raw_last_access)
        # serves as the default fallback if keypair resource policy's idle_timeout is "undefined"
        idle_timeout = self.idle_timeout.total_seconds()
        policy_cache = self._policy_cache.get()
        policy = policy_cache.get(session['access_key'], None)
        if policy is None:
            query = (
                sa.select([keypair_resource_policies])
                .select_from(
                    sa.join(
                        keypairs,
                        keypair_resource_policies,
                        (keypair_resource_policies.c.name == keypairs.c.resource_policy),
                    )
                )
                .where(
                    keypairs.c.access_key == session['access_key']
                )
            )
            result = await dbconn.execute(query)
            policy = await result.first()
            assert policy is not None
            policy_cache[session['access_key']] = policy
        # setting idle_timeout:
        # - zero/inf means "infinite"
        # - negative means "undefined"
        if policy['idle_timeout'] >= 0:
            idle_timeout = float(policy['idle_timeout'])
        if (
            (idle_timeout <= 0)
            or (math.isinf(idle_timeout) and idle_timeout > 0)
            or (t - last_access <= idle_timeout)
        ):
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

    async def populate_config(self, config: Mapping[str, Any]) -> None:
        pass

    async def check_session(self, session: RowProxy, dbconn: SAConnection) -> bool:
        # last_stat = session['last_stat']
        # TODO: implement
        return True

    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        pass


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
            continue
        log.info(f"Initializing idle checker: {checker_name}")
        checker_instance = await checker_cls.new(dbpool, shared_config, event_dispatcher)
        instances.append(checker_instance)
    return instances
