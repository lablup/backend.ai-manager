from __future__ import annotations

import enum
import logging
import math
from abc import ABCMeta, abstractmethod
from contextvars import ContextVar
from datetime import timedelta
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TYPE_CHECKING,
)

import aioredis
import sqlalchemy as sa
import trafaret as t
from sqlalchemy.engine import Row

import ai.backend.common.validators as tx
from ai.backend.common import msgpack
from ai.backend.common.events import (
    AbstractEvent,
    DoIdleCheckEvent,
    DoTerminateSessionEvent,
    EventDispatcher,
    EventHandler,
    EventProducer,
    ExecutionCancelledEvent,
    ExecutionFinishedEvent,
    ExecutionStartedEvent,
    ExecutionTimeoutEvent,
    SessionStartedEvent,
)
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import AccessKey, aobject
from ai.backend.common.utils import nmget, str_to_timedelta

from .defs import REDIS_LIVE_DB
from .distributed import GlobalTimer
from .models import kernels, keypair_resource_policies, keypairs
from .models.kernel import LIVE_STATUS

if TYPE_CHECKING:
    from .config import SharedConfig
    from ai.backend.common.types import AgentId, SessionId
    from sqlalchemy.ext.asyncio import (
        AsyncConnection as SAConnection,
        AsyncEngine as SAEngine,
    )

log = BraceStyleAdapter(logging.getLogger("ai.backend.manager.idle"))


class AppStreamingStatus(enum.Enum):
    NO_ACTIVE_CONNECTIONS = 0
    HAS_ACTIVE_CONNECTIONS = 1


class BaseIdleChecker(aobject, metaclass=ABCMeta):

    name: ClassVar[str] = "base"

    _db: SAEngine
    _shared_config: SharedConfig
    _event_dispatcher: EventDispatcher
    _event_producer: EventProducer

    def __init__(
        self,
        db: SAEngine,
        shared_config: SharedConfig,
        event_dispatcher: EventDispatcher,
        event_producer: EventProducer,
    ) -> None:
        self._db = db
        self._shared_config = shared_config
        self._event_dispatcher = event_dispatcher
        self._event_producer = event_producer

    async def __ainit__(self) -> None:
        self._redis = await aioredis.create_redis(
            str(self._shared_config.get_redis_url(db=REDIS_LIVE_DB))
        )
        raw_config = await self._shared_config.etcd.get_prefix_dict(
            f"config/idle/checkers/{self.name}"
        )
        await self.populate_config(raw_config or {})
        self.timer = GlobalTimer(
            self._redis,
            "idle_check",
            self._event_producer,
            lambda: DoIdleCheckEvent(),
            10.0,
        )
        self._evh_idle_check = self._event_dispatcher.consume(
            DoIdleCheckEvent,
            None,
            self._do_idle_check,
        )
        await self.timer.join()

    async def aclose(self) -> None:
        await self.timer.leave()
        self._event_dispatcher.unconsume(self._evh_idle_check)
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
        source: AgentId,
        event: DoIdleCheckEvent,
    ) -> None:
        log.debug("do_idle_check(): triggered")
        async with self._db.begin() as conn:
            query = (
                sa.select([kernels])
                .select_from(kernels)
                .where((kernels.c.status.in_(LIVE_STATUS)))
            )
            result = await conn.execute(query)
            rows = result.fetchall()
            for row in rows:
                if not (await self.check_session(row, conn)):
                    log.info(
                        f"The {self.name} idle checker triggered termination of s:{row['id']}"
                    )
                    await self._event_producer.produce_event(
                        DoTerminateSessionEvent(row["id"], "idle-timeout")
                    )

    @abstractmethod
    async def check_session(self, session: Row, dbconn: SAConnection) -> bool:
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

    _config_iv = t.Dict(
        {
            t.Key("threshold", default="10m"): tx.TimeDuration(),
        }
    ).allow_extra("*")

    idle_timeout: timedelta
    _policy_cache: ContextVar[Dict[AccessKey, Optional[Mapping[str, Any]]]]
    _evhandlers: List[EventHandler[None, AbstractEvent]]

    async def __ainit__(self) -> None:
        await super().__ainit__()
        self._policy_cache = ContextVar("_policy_cache")
        d = self._event_dispatcher
        self._evhandlers = [
            d.consume(SessionStartedEvent, None, self._session_started_cb),  # type: ignore
            d.consume(ExecutionStartedEvent, None, self._execution_started_cb),  # type: ignore
            d.consume(ExecutionFinishedEvent, None, self._execution_exited_cb),  # type: ignore
            d.consume(ExecutionTimeoutEvent, None, self._execution_exited_cb),  # type: ignore
            d.consume(ExecutionCancelledEvent, None, self._execution_exited_cb),  # type: ignore
        ]

    async def aclose(self) -> None:
        for _evh in self._evhandlers:
            self._event_dispatcher.unconsume(_evh)
        await super().aclose()

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        config = self._config_iv.check(raw_config)
        self.idle_timeout = config["threshold"]
        log.info(
            "TimeoutIdleChecker: default idle_timeout = {0:,} seconds",
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
        await self._redis.set(
            f"session.{session_id}.last_access", "0", exist=self._redis.SET_IF_EXIST
        )

    async def _update_timeout(self, session_id: SessionId) -> None:
        log.debug(f"TimeoutIdleChecker._update_timeout({session_id})")
        t = await self._redis.time()
        await self._redis.set(
            f"session.{session_id}.last_access",
            f"{t:.06f}",
            expire=max(86400, int(self.idle_timeout.total_seconds() * 2)),
        )

    async def _session_started_cb(
        self,
        context: None,
        source: AgentId,
        event: SessionStartedEvent,
    ) -> None:
        await self._update_timeout(event.session_id)

    async def _execution_started_cb(
        self,
        context: None,
        source: AgentId,
        event: ExecutionStartedEvent,
    ) -> None:
        await self._disable_timeout(event.session_id)

    async def _execution_exited_cb(
        self,
        context: None,
        source: AgentId,
        event: ExecutionFinishedEvent | ExecutionTimeoutEvent | ExecutionCancelledEvent,
    ) -> None:
        await self._update_timeout(event.session_id)

    async def _do_idle_check(
        self,
        context: None,
        source: AgentId,
        event: DoIdleCheckEvent,
    ) -> None:
        cache_token = self._policy_cache.set(dict())
        try:
            return await super()._do_idle_check(context, source, event)
        finally:
            self._policy_cache.reset(cache_token)

    async def check_session(self, session: Row, dbconn: SAConnection) -> bool:
        session_id = session["id"]
        active_streams = await self._redis.zcount(
            f"session.{session_id}.active_app_connections"
        )
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
        policy = policy_cache.get(session["access_key"], None)
        if policy is None:
            query = (
                sa.select([keypair_resource_policies])
                .select_from(
                    sa.join(
                        keypairs,
                        keypair_resource_policies,
                        (
                            keypair_resource_policies.c.name
                            == keypairs.c.resource_policy
                        ),
                    )
                )
                .where(keypairs.c.access_key == session["access_key"])
            )
            result = await dbconn.execute(query)
            policy = result.first()
            assert policy is not None
            policy_cache[session["access_key"]] = policy
        # setting idle_timeout:
        # - zero/inf means "infinite"
        # - negative means "undefined"
        if policy["idle_timeout"] >= 0:
            idle_timeout = float(policy["idle_timeout"])
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

    cpu_util_series: List[float] = []
    mem_util_series: List[float] = []
    cuda_util_series: List[float] = []
    cuda_mem_util_series: List[float] = []

    resource_thresholds: Mapping[str, Any]
    threshold_condition: str | None
    window: timedelta
    _policy_cache: ContextVar[Dict[AccessKey, Optional[Mapping[str, Any]]]]
    _evhandlers: List[EventHandler[None, AbstractEvent]]

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        self.resource_thresholds = {
            "cpu_threshold": nmget(raw_config, "resource-thresholds.cpu.average"),
            "mem_threshold": nmget(raw_config, "resource-thresholds.mem.average"),
            "cuda_threshold": nmget(raw_config, "resource-thresholds.cuda.average"),
            "cuda_mem_threshold": nmget(
                raw_config, "resource-thresholds.cuda_mem.average"
            ),
        }

        self.threshold_condition = "and" # for debugging # raw_config.get("thresholds-check-condition", "and")
        self.window = "1m" 
        self.window = str_to_timedelta(self.window) # raw_config.get("window", "10m")

        # Generate string of available resources while maintaining index order
        self.resource_list = [
            self.resource_thresholds["cpu_threshold"],
            self.resource_thresholds["mem_threshold"],
            self.resource_thresholds["cuda_mem_threshold"],
            self.resource_thresholds["cuda_threshold"],
        ]
        self.resource_avail = "".join(
            ["1" if x is not None else "0" for x in self.resource_list]
        )

        log.info(
            f"UtilizationIdleChecker(%): "
            f"cpu({self.resource_thresholds['cpu_threshold']}), "
            f"mem({self.resource_thresholds['mem_threshold']}), "
            f"cuda({self.resource_thresholds['cuda_threshold']}), "
            f"cuda.mem({self.resource_thresholds['cuda_mem_threshold']}), "
            f"resource availablity string: {self.resource_avail}, "
            f"threshold check condition: {self.threshold_condition}, "
            f"moving window: {self.window.total_seconds()}s"
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
        log.debug(f"UtilizationIdleChecker._disable_timeout({session_id})")
        await self._redis.set(
            f"session.{session_id}.last_execution", "0", exist=self._redis.SET_IF_EXIST
        )

    async def _update_timeout(self, session_id: SessionId) -> None:
        log.debug(f"UtilizationIdleChecker._update_timeout({session_id})")
        t = await self._redis.time()
        await self._redis.set(
            f"session.{session_id}.last_execution",
            f"{t:.06f}",
            expire=max(86400, int(self.window.total_seconds() * 2)),
        ),

    async def _session_started_cb(
        self,
        context: None,
        source: AgentId,
        event: SessionStartedEvent,
    ) -> None:
        await self._update_timeout(event.session_id)

    async def _execution_started_cb(
        self,
        context: None,
        source: AgentId,
        event: ExecutionStartedEvent,
    ) -> None:
        await self._disable_timeout(event.session_id)

    async def _execution_exited_cb(
        self,
        context: None,
        source: AgentId,
        event: ExecutionFinishedEvent | ExecutionTimeoutEvent | ExecutionCancelledEvent,
    ) -> None:
        await self._update_timeout(event.session_id)

    async def _do_idle_check(
        self,
        context: None,
        source: AgentId,
        event: DoIdleCheckEvent,
    ) -> None:

        try:
            return await super()._do_idle_check(context, source, event)
        finally:
            pass

    async def check_session(self, session: Row, dbconn: SAConnection) -> bool:
        session_id = session["id"]
        active_streams = await self._redis.zcount(
            f"session.{session_id}.active_app_connections"
        )
        if active_streams is not None and active_streams > 0:
            return True

        redis = await aioredis.create_redis("redis://127.0.0.1:8111/")
        redis_out = await redis.get(str(session["session_id"]), encoding=None)
        try:
            live_stat = msgpack.unpackb(redis_out)
        except Exception:
            return True

        cpu_util_pct = float(live_stat["cpu_util"]["pct"])
        mem_util_pct = float(live_stat["mem"]["pct"])
        try:
            cuda_util_pct = float(live_stat["cuda_util"]["pct"])
            cuda_mem_util_pct = float(live_stat["cuda_mem"]["pct"])
        except Exception:
            cuda_util_pct = 30.0
            cuda_mem_util_pct = 30.0

        interval = self.timer.interval

        window_size = self.window.total_seconds() / interval

        self.cpu_util_series.append(float(cpu_util_pct))
        self.mem_util_series.append(float(mem_util_pct))
        self.cuda_mem_util_series.append(float(cuda_mem_util_pct))
        self.cuda_util_series.append(float(cuda_util_pct))

        if len(self.cpu_util_series) < window_size:
            return True
        avg_cpu_util = sum(self.cpu_util_series) / len(self.cpu_util_series)
        avg_mem_util = sum(self.mem_util_series) / len(self.mem_util_series)
        avg_cuda_util = sum(self.cuda_util_series) / len(self.cuda_util_series)
        cuda_mem_util = sum(self.cuda_mem_util_series) / len(self.cuda_mem_util_series)
        avg_list = [avg_cpu_util, avg_mem_util, avg_cuda_util, cuda_mem_util]

        self.cpu_util_series.pop(0)
        self.mem_util_series.pop(0)
        self.cuda_mem_util_series.pop(0)
        self.cuda_util_series.pop(0)

        def check_threshold_condition(
            resource_thresholds, resource_list, resource_avail, avg_list, condition
        ):

            """ This function selected available resource based on ordered strings \
            and auto-generates the boolean condtions """
            avail_index = [
                pos for pos, char in enumerate(resource_avail) if char == "1"
            ]
            selected_resources = [resource_list[i] for i in avail_index]
            selected_values = [avg_list[i] for i in avail_index]

            eval_str = ""
            i = 0
            while i < len(selected_resources):
                resource_thresh = selected_resources[i]
                resource_value = selected_values[i]
                eval_str += (
                    str(resource_value)
                    + " <= "
                    + (str(resource_thresh))
                    + f" {condition} "
                )
                i += 1
            eval_str = eval_str[: -len(condition) - 1]
            print(eval_str, eval(eval_str))

            if eval(eval_str):
                return True
            else:
                return False

        if check_threshold_condition(
            self.resource_thresholds,
            self.resource_list,
            self.resource_avail,
            avg_list,
            self.threshold_condition,
        ):
            return True
        else:
            return False


checker_registry: Mapping[str, Type[BaseIdleChecker]] = {
    TimeoutIdleChecker.name: TimeoutIdleChecker,
    UtilizationIdleChecker.name: UtilizationIdleChecker,
}


async def create_idle_checkers(
    db: SAEngine,
    shared_config: SharedConfig,
    event_dispatcher: EventDispatcher,
    event_producer: EventProducer,
) -> Sequence[BaseIdleChecker]:
    """
    Create an instance of session idleness checker
    from the given configuration and using the given event dispatcher.
    """

    checkers = await shared_config.etcd.get("config/idle/enabled")
    if not checkers:
        return []

    instances = []
    for checker_name in checkers.split(","):
        checker_cls = checker_registry.get(checker_name, None)
        if checker_cls is None:
            log.warning("ignoring an unknown idle checker name: {checker_name}")
            continue
        log.info(f"Initializing idle checker: {checker_name}")
        checker_instance = await checker_cls.new(
            db, shared_config, event_dispatcher, event_producer
        )
        instances.append(checker_instance)
    return instances
