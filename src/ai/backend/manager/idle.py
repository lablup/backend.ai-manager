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

from .defs import REDIS_LIVE_DB, REDIS_STAT_DB
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

from icecream import ic

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
        log.debug("do_idle_check({}): triggered", self.name)
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
                        DoTerminateSessionEvent(row["id"], f"idle-{self.name}")
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

    _config_iv = t.Dict(
        {
            t.Key("time-window", default="10m"): tx.TimeDuration(),
            t.Key("thresholds-check-condition", default="and"): t.String,
            t.Key("resource-thresholds"): t.Dict(
                {
                    t.Key("cpu", default=None): t.Null | t.Dict({t.Key("average"): t.Float}),
                    t.Key("mem", default=None): t.Null | t.Dict({t.Key("average"): t.Float}),
                    t.Key("cuda", default=None): t.Null | t.Dict({t.Key("average"): t.Float}),
                    t.Key("cuda.mem", default=None): t.Null | t.Dict({t.Key("average"): t.Float}),
                }
            ),
        }
    ).allow_extra("*")

    cpu_util_series: List[float] = []
    mem_util_series: List[float] = []
    cuda_util_series: List[float] = []
    cuda_mem_util_series: List[float] = []

    resource_thresholds: Mapping[str, Any]
    thresholds_check_operator: str | None
    time_window: timedelta
    _evhandlers: List[EventHandler[None, AbstractEvent]]

    async def __ainit__(self) -> None:
        await super().__ainit__()
        self._redis_stat = await aioredis.create_redis(
            str(self._shared_config.get_redis_url(db=REDIS_STAT_DB))
        )

    async def aclose(self) -> None:
        self._redis_stat.close()
        await self._redis_stat.wait_closed()
        await super().aclose()

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        config = self._config_iv.check(raw_config)
        self.resource_thresholds = {
            "cpu": nmget(config, "resource-thresholds.cpu.average"),
            "mem": nmget(config, "resource-thresholds.mem.average"),
            "cuda": nmget(config, "resource-thresholds.cuda.average"),
            "cuda_mem": nmget(config, "resource-thresholds.cuda_mem.average"),
        }
        self.thresholds_check_operator = config.get("thresholds-check-operator")
        self.time_window = config.get("time-window")

        # Generate string of available resources while maintaining index order
        self.resource_list = [
            self.resource_thresholds["cpu"],
            self.resource_thresholds["mem"],
            self.resource_thresholds["cuda"],
            self.resource_thresholds["cuda_mem"],
        ]
        self.resource_avail = "".join(
            ["1" if x is not None else "0" for x in self.resource_list]
        )

        log.info(
            f"UtilizationIdleChecker(%): "
            f"cpu({self.resource_thresholds['cpu']}), "
            f"mem({self.resource_thresholds['mem']}), "
            f"cuda({self.resource_thresholds['cuda']}), "
            f"cuda.mem({self.resource_thresholds['cuda_mem']}), "
            f"thresholds-check-operator(\"{self.thresholds_check_operator}\"), "
            f"time-window({self.time_window.total_seconds()}s)"
        )

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

        raw_live_stat = await self._redis_stat.get(str(session_id), encoding=None)
        try:
            live_stat = msgpack.unpackb(raw_live_stat)
        except Exception:
            return True

        stream_values = {"cpu_util": 0.0, "mem": 0.0, "cuda_util": 0.0, "cuda_mem": 0.0}

        def check_avail(_k, _live_stat):
            """This function checks key values for being availbe from Redis"""
            pct = nmget(_live_stat, f"{_k}.pct")
            return float(pct) if pct is not None else 0.0

        for k in stream_values.keys():
            stream_values[k] = check_avail(k, live_stat)
        interval = self.timer.interval
        window_size = int(self.time_window.total_seconds() / interval)

        self.cpu_util_series.append(stream_values["cpu_util"])
        self.mem_util_series.append(stream_values["mem"])
        self.cuda_mem_util_series.append(stream_values["cuda_util"])
        self.cuda_util_series.append(stream_values["cuda_mem"])

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

        def _check_threshold_condition(
            resource_list, resource_avail, avg_list, condition
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

            if eval(eval_str):
                return False
            else:
                return True

        if _check_threshold_condition(
            self.resource_list,
            self.resource_avail,
            avg_list,
            self.thresholds_check_operator,
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
