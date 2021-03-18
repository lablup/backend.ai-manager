from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

import attr

if TYPE_CHECKING:
    from aiopg.sa.engine import _PoolAcquireContextManager as SAPool
    from aioredis import Redis

    from ai.backend.common.events import EventDispatcher, EventProducer
    from ai.backend.common.plugin.hook import HookPluginContext
    from ai.backend.common.plugin.monitor import ErrorPluginContext, StatsPluginContext

    from ..manager.background import BackgroundTaskManager
    from ..manager.models.storage import StorageSessionManager
    from ..manager.idle import BaseIdleChecker
    from ..manager.plugin.webapp import WebappPluginContext
    from ..manager.registry import AgentRegistry
    from .config import LocalConfig, SharedConfig
    from .types import CORSOptions


class BaseContext:
    pass


@attr.s(slots=True, auto_attribs=True, init=False)
class RootContext(BaseContext):
    pidx: int
    dbpool: SAPool
    event_dispatcher: EventDispatcher
    event_producer: EventProducer
    redis_live: Redis
    redis_stat: Redis
    redis_image: Redis
    redis_stream: Redis
    shared_config: SharedConfig
    local_config: LocalConfig
    cors_options: CORSOptions

    webapp_plugin_ctx: WebappPluginContext
    idle_checkers: Sequence[BaseIdleChecker]
    storage_manager: StorageSessionManager
    hook_plugin_ctx: HookPluginContext

    registry: AgentRegistry

    error_monitor: ErrorPluginContext
    stats_monitor: StatsPluginContext
    background_task_manager: BackgroundTaskManager