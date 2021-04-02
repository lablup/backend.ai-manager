from __future__ import annotations

import asyncio
from contextvars import ContextVar
from datetime import datetime
import itertools
import logging
import pkg_resources
from typing import (
    Any,
    Awaitable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

import aioredis
import aioredlock
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection
from sqlalchemy.sql.expression import true

from ai.backend.common.docker import ImageRef
from ai.backend.common.events import (
    AgentStartedEvent,
    DoScheduleEvent,
    SessionCancelledEvent,
    SessionEnqueuedEvent,
    SessionScheduledEvent,
    SessionTerminatedEvent,
    EventDispatcher,
    EventProducer,
)
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    aobject,
    AgentId,
    ClusterMode,
    ResourceSlot,
    SessionId,
)

from ..api.exceptions import InstanceNotAvailable
from ..distributed import GlobalTimer
from ..defs import REDIS_STREAM_DB
from ..exceptions import convert_to_status_data
from ..models import (
    agents, kernels, keypairs, scaling_groups,
    recalc_agent_resource_occupancy,
    AgentStatus, KernelStatus,
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
)
from ..models.utils import sql_json_increment, sql_json_merge
from . import (
    PredicateResult,
    PendingSession,
    ExistingSession,
    SchedulingContext,
    AgentContext,
    AgentAllocationContext,
    AbstractScheduler,
    KernelInfo,
    KernelAgentBinding,
)
from .predicates import (
    check_reserved_batch_session,
    check_concurrency,
    check_dependencies,
    check_keypair_resource_limit,
    check_group_resource_limit,
    check_domain_resource_limit,
    check_scaling_group,
)

if TYPE_CHECKING:
    from ..config import LocalConfig, SharedConfig
    from ..registry import AgentRegistry

__all__ = (
    'load_scheduler',
    'SchedulerDispatcher',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))

_log_fmt: ContextVar[str] = ContextVar('_log_fmt')
_log_args: ContextVar[Tuple[Any, ...]] = ContextVar('_log_args')


def load_scheduler(name: str, scheduler_configs: Mapping[str, Any]) -> AbstractScheduler:
    entry_prefix = 'backendai_scheduler_v10'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        if entrypoint.name == name:
            log.debug('loading scheduler plugin "{}" from {}', name, entrypoint.module_name)
            scheduler_cls = entrypoint.load()
            scheduler_config = scheduler_configs.get(name, {})
            return scheduler_cls(scheduler_config)
    raise ImportError('Cannot load the scheduler plugin', name)


def merge_resource(src: MutableMapping[str, Any], val: MutableMapping[str, Any]) -> None:
    for k in val.keys():
        if k in src.keys():
            src[k] += val[k]
        else:
            src[k] = val[k]


StartTaskArgs = Tuple[
    Tuple[Any, ...],
    SchedulingContext,
    Tuple[PendingSession, List[KernelAgentBinding]],
    List[Tuple[str, Union[Exception, PredicateResult]]],
]


class SchedulerDispatcher(aobject):

    config: LocalConfig
    shared_config: SharedConfig
    registry: AgentRegistry

    lock_manager: aioredlock.Aioredlock
    timer_redis: aioredis.Redis
    event_dispatcher: EventDispatcher
    event_producer: EventProducer
    schedule_timer: GlobalTimer

    def __init__(
        self,
        local_config: LocalConfig,
        shared_config: SharedConfig,
        event_dispatcher: EventDispatcher,
        event_producer: EventProducer,
        registry: AgentRegistry,
    ) -> None:
        self.local_config = local_config
        self.shared_config = shared_config
        self.event_dispatcher = event_dispatcher
        self.event_producer = event_producer
        self.registry = registry
        self.db = registry.db
        self.schedule_lock_timeout = 120.0

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        self.registry.event_dispatcher.consume(SessionEnqueuedEvent, None, self.schedule)
        self.registry.event_dispatcher.consume(SessionTerminatedEvent, None, self.schedule)
        self.registry.event_dispatcher.consume(AgentStartedEvent, None, self.schedule)
        self.registry.event_dispatcher.consume(DoScheduleEvent, None, self.schedule)
        redis_url = self.shared_config.get_redis_url(db=REDIS_STREAM_DB)
        self.lock_manager = aioredlock.Aioredlock(
            [str(redis_url)],
            # we have explicit semantics: temporary locking failure -> try at the next chance,
            # such as scheduling timer ticks or upon scheduling events
            retry_count=2,
        )
        self.timer_redis = await aioredis.create_redis(str(redis_url))
        self.schedule_timer = GlobalTimer(
            self.timer_redis,
            "scheduler_tick",
            self.event_producer,
            lambda: DoScheduleEvent(),
            interval=10.0,
        )
        await self.schedule_timer.join()

    async def close(self) -> None:
        log.info('Session scheduler stopped')
        await self.schedule_timer.leave()
        self.timer_redis.close()
        await self.timer_redis.wait_closed()

    async def schedule(
        self,
        context: None,
        source: AgentId,
        event: SessionEnqueuedEvent | SessionTerminatedEvent | AgentStartedEvent | DoScheduleEvent,
    ) -> None:
        try:
            lock = await self.lock_manager.lock(
                'manager.scheduler',
                lock_timeout=self.schedule_lock_timeout,
            )
            async with lock:
                await self.schedule_impl()
        except aioredlock.LockError:
            log.debug('schedule(): temporary locking failure; will be retried.')
            # The dispatcher will try the next chance.

    async def schedule_impl(self) -> None:
        log.debug('schedule(): triggered')
        known_slot_types = await self.shared_config.get_resource_slots()
        sched_ctx = SchedulingContext(
            registry=self.registry,
            known_slot_types=known_slot_types,
        )
        start_task_args: List[StartTaskArgs] = []

        # We use short transaction blocks to prevent deadlock timeouts under heavy loads
        # because this scheduling handler will be executed by only one process.
        # It is executed under a globally exclusive context using aioredlock.
        async with self.db.connect() as agent_db_conn, \
                   self.db.connect() as kernel_db_conn:
            async with agent_db_conn.begin():
                query = (
                    sa.select([agents.c.scaling_group])
                    .select_from(agents)
                    .where(agents.c.status == AgentStatus.ALIVE)
                    .group_by(agents.c.scaling_group)
                )
                result = await agent_db_conn.execute(query)
                schedulable_scaling_groups = [
                    row.scaling_group for row in result.fetchall()
                ]
            for sgroup_name in schedulable_scaling_groups:
                try:
                    args_list = await self._schedule_in_sgroup(
                        sched_ctx, agent_db_conn, kernel_db_conn, sgroup_name,
                    )
                    start_task_args.extend(args_list)
                except InstanceNotAvailable:
                    # Proceed to the next scaling group and come back later.
                    log.debug('schedule({}): instance not available', sgroup_name)
                except Exception as e:
                    log.exception('schedule({}): scheduling error!\n{}', sgroup_name, repr(e))

        # At this point, all scheduling decisions are made
        # and the resource occupation is committed to the database.
        if start_task_args:
            start_coros = []
            for args in start_task_args:
                start_coros.append(self.start_session(*args))
            await asyncio.gather(*start_coros)

    async def _load_scheduler(
        self,
        db_conn: SAConnection,
        sgroup_name: str,
    ) -> AbstractScheduler:
        query = (
            sa.select([scaling_groups.c.scheduler])
            .select_from(scaling_groups)
            .where(scaling_groups.c.name == sgroup_name)
        )
        result = await db_conn.execute(query)
        scheduler_name = result.scalar()
        return load_scheduler(scheduler_name, self.shared_config['plugins']['scheduler'])

    async def _schedule_in_sgroup(
        self,
        sched_ctx: SchedulingContext,
        agent_db_conn: SAConnection,
        kernel_db_conn: SAConnection,
        sgroup_name: str,
    ) -> List[StartTaskArgs]:
        async with kernel_db_conn.begin():
            scheduler = await self._load_scheduler(kernel_db_conn, sgroup_name)
            pending_sessions = await _list_pending_sessions(kernel_db_conn, sgroup_name)
            existing_sessions = await _list_existing_sessions(kernel_db_conn, sgroup_name)
        log.debug('running scheduler (sgroup:{}, pending:{}, existing:{})',
                  sgroup_name, len(pending_sessions), len(existing_sessions))
        zero = ResourceSlot()
        args_list: List[StartTaskArgs] = []
        while len(pending_sessions) > 0:
            async with agent_db_conn.begin():
                candidate_agents = await _list_agents_by_sgroup(agent_db_conn, sgroup_name)
                total_capacity = sum((ag.available_slots for ag in candidate_agents), zero)

            picked_session_id = scheduler.pick_session(
                total_capacity,
                pending_sessions,
                existing_sessions,
            )
            if picked_session_id is None:
                # no session is picked.
                # continue to next sgroup.
                return []
            for picked_idx, sess_ctx in enumerate(pending_sessions):
                if sess_ctx.session_id == picked_session_id:
                    break
            else:
                # no matching entry for picked session?
                raise RuntimeError('should not reach here')
            sess_ctx = pending_sessions.pop(picked_idx)

            log_fmt = 'schedule(s:{}, type:{}, name:{}, ak:{}, cluster_mode:{}): '
            log_args = (
                sess_ctx.session_id,
                sess_ctx.session_type,
                sess_ctx.session_name,
                sess_ctx.access_key,
                sess_ctx.cluster_mode,
            )
            _log_fmt.set(log_fmt)
            _log_args.set(log_args)
            log.debug(log_fmt + 'try-scheduling', *log_args)
            session_agent_binding: Tuple[PendingSession, List[KernelAgentBinding]]

            async with kernel_db_conn.begin():
                predicates: Sequence[Tuple[str, Awaitable[PredicateResult]]] = [
                    (
                        'reserved_time',
                        check_reserved_batch_session(kernel_db_conn, sched_ctx, sess_ctx),
                    ),
                    ('concurrency', check_concurrency(kernel_db_conn, sched_ctx, sess_ctx)),
                    ('dependencies', check_dependencies(kernel_db_conn, sched_ctx, sess_ctx)),
                    (
                        'keypair_resource_limit',
                        check_keypair_resource_limit(kernel_db_conn, sched_ctx, sess_ctx),
                    ),
                    (
                        'user_group_resource_limit',
                        check_group_resource_limit(kernel_db_conn, sched_ctx, sess_ctx),
                    ),
                    (
                        'domain_resource_limit',
                        check_domain_resource_limit(kernel_db_conn, sched_ctx, sess_ctx),
                    ),
                    (
                        'scaling_group_resource_limit',
                        check_scaling_group(kernel_db_conn, sched_ctx, sess_ctx),
                    ),
                ]
                check_results: List[Tuple[str, Union[Exception, PredicateResult]]] = []
                for predicate_name, check_coro in predicates:
                    try:
                        check_results.append((predicate_name, await check_coro))
                    except Exception as e:
                        log.exception(log_fmt + 'predicate-error', *log_args)
                        check_results.append((predicate_name, e))
            has_failure = False
            # has_permanent_failure = False
            failed_predicates = []
            passed_predicates = []
            for predicate_name, result in check_results:
                if isinstance(result, Exception):
                    has_failure = True
                    failed_predicates.append({
                        'name': predicate_name,
                        'msg': repr(result),
                    })
                    continue
                if result.passed:
                    passed_predicates.append({
                        'name': predicate_name,
                    })
                else:
                    failed_predicates.append({
                        'name': predicate_name,
                        'msg': result.message or "",
                    })
                    has_failure = True
                    # if result.permanent:
                    #     has_permanent_failure = True
            if has_failure:
                log.debug(log_fmt + 'predicate-checks-failed (temporary)', *log_args)
                # TODO: handle has_permanent_failure as cancellation
                #  - An early implementation of it has caused DB query blocking due to
                #    the inclusion of the kernels.status field. :(
                #    Let's fix it.
                async with kernel_db_conn.begin():
                    await _invoke_failure_callbacks(
                        kernel_db_conn, sched_ctx, sess_ctx, check_results,
                    )
                    query = kernels.update().values({
                        'status_info': "predicate-checks-failed",
                        'status_data': sql_json_increment(
                            kernels.c.status_data,
                            ('scheduler', 'retries'),
                            parent_updates={
                                'last_try': datetime.now(tzutc()).isoformat(),
                                'failed_predicates': failed_predicates,
                                'passed_predicates': passed_predicates,
                            }
                        ),
                    }).where(kernels.c.id == sess_ctx.session_id)
                    await kernel_db_conn.execute(query)
                # Predicate failures are *NOT* permanent errors.
                # We need to retry the scheduling afterwards.
                continue
            else:
                async with kernel_db_conn.begin():
                    query = kernels.update().values({
                        'status_data': sql_json_merge(
                            kernels.c.status_data,
                            ('scheduler',),
                            {
                                'last_try': datetime.now(tzutc()).isoformat(),
                                'failed_predicates': failed_predicates,
                                'passed_predicates': passed_predicates,
                            }
                        ),
                    }).where(kernels.c.id == sess_ctx.session_id)
                    await kernel_db_conn.execute(query)

            if sess_ctx.cluster_mode == ClusterMode.SINGLE_NODE:
                session_agent_binding = await self._schedule_single_node_session(
                    sched_ctx,
                    scheduler,
                    agent_db_conn,
                    kernel_db_conn,
                    sgroup_name,
                    candidate_agents,
                    sess_ctx,
                    check_results,
                )
            elif sess_ctx.cluster_mode == ClusterMode.MULTI_NODE:
                session_agent_binding = await self._schedule_multi_node_session(
                    sched_ctx,
                    scheduler,
                    agent_db_conn,
                    kernel_db_conn,
                    sgroup_name,
                    candidate_agents,
                    sess_ctx,
                    check_results,
                )
            else:
                raise RuntimeError(
                    f"should not reach here; unknown cluster_mode: {sess_ctx.cluster_mode}"
                )
            args_list.append((
                log_args,
                sched_ctx,
                session_agent_binding,
                check_results,
            ))

        return args_list

    async def _schedule_single_node_session(
        self,
        sched_ctx: SchedulingContext,
        scheduler: AbstractScheduler,
        agent_db_conn: SAConnection,
        kernel_db_conn: SAConnection,
        sgroup_name: str,
        candidate_agents: Sequence[AgentContext],
        sess_ctx: PendingSession,
        check_results: List[Tuple[str, Union[Exception, PredicateResult]]],
    ) -> Tuple[PendingSession, List[KernelAgentBinding]]:
        # Assign agent resource per session.
        log_fmt = _log_fmt.get()
        log_args = _log_args.get()
        try:
            agent_id = scheduler.assign_agent_for_session(candidate_agents, sess_ctx)
            if agent_id is None:
                raise InstanceNotAvailable
            async with agent_db_conn.begin():
                agent_alloc_ctx = await _reserve_agent(
                    sched_ctx, agent_db_conn, sgroup_name, agent_id, sess_ctx.requested_slots,
                )
        except InstanceNotAvailable:
            log.debug(log_fmt + 'no-available-instances', *log_args)
            async with kernel_db_conn.begin():
                await _invoke_failure_callbacks(
                    kernel_db_conn, sched_ctx, sess_ctx, check_results,
                )
                query = kernels.update().values({
                    'status_info': "no-available-instances",
                    'status_data': sql_json_increment(
                        kernels.c.status_data,
                        ('scheduler', 'retries'),
                        parent_updates={
                            'last_try': datetime.now(tzutc()).isoformat(),
                        }
                    ),
                }).where(kernels.c.id == sess_ctx.session_id)
                await kernel_db_conn.execute(query)
            raise
        except Exception as e:
            log.exception(
                log_fmt + 'unexpected-error, during agent allocation',
                *log_args,
            )
            async with kernel_db_conn.begin():
                await _invoke_failure_callbacks(
                    kernel_db_conn, sched_ctx, sess_ctx, check_results,
                )
                query = kernels.update().values({
                    'status_info': "scheduler-error",
                    'status_data': convert_to_status_data(e),
                }).where(kernels.c.id == sess_ctx.session_id)
                await kernel_db_conn.execute(query)
            raise

        async with kernel_db_conn.begin():
            query = kernels.update().values({
                'agent': agent_alloc_ctx.agent_id,
                'agent_addr': agent_alloc_ctx.agent_addr,
                'scaling_group': sgroup_name,
                'status': KernelStatus.PREPARING,
                'status_info': 'scheduled',
                'status_data': {},
                'status_changed': datetime.now(tzutc()),
            }).where(kernels.c.session_id == sess_ctx.session_id)
            await kernel_db_conn.execute(query)
        return (
            sess_ctx,
            [
                KernelAgentBinding(kernel, agent_alloc_ctx)
                for kernel in sess_ctx.kernels
            ],
        )

    async def _schedule_multi_node_session(
        self,
        sched_ctx: SchedulingContext,
        scheduler: AbstractScheduler,
        agent_db_conn: SAConnection,
        kernel_db_conn: SAConnection,
        sgroup_name: str,
        candidate_agents: Sequence[AgentContext],
        sess_ctx: PendingSession,
        check_results: List[Tuple[str, Union[Exception, PredicateResult]]],
    ) -> Tuple[PendingSession, List[KernelAgentBinding]]:
        # Assign agent resource per kernel in the session.
        log_fmt = _log_fmt.get()
        log_args = _log_args.get()
        agent_query_extra_conds = None
        kernel_agent_bindings: List[KernelAgentBinding] = []
        async with agent_db_conn.begin():
            # This outer transaction is rolled back when any exception occurs inside,
            # including scheduling failures of a kernel.
            # It ensures that occupied_slots are recovered when there are partial
            # scheduling failures.
            for kernel in sess_ctx.kernels:
                try:
                    agent_id = scheduler.assign_agent_for_kernel(candidate_agents, kernel)
                    if agent_id is None:
                        raise InstanceNotAvailable
                    async with agent_db_conn.begin_nested():
                        agent_alloc_ctx = await _reserve_agent(
                            sched_ctx, agent_db_conn,
                            sgroup_name, agent_id, kernel.requested_slots,
                            extra_conds=agent_query_extra_conds,
                        )
                        candidate_agents = await _list_agents_by_sgroup(agent_db_conn, sgroup_name)
                except InstanceNotAvailable:
                    log.debug(log_fmt + 'no-available-instances', *log_args)
                    async with kernel_db_conn.begin():
                        await _invoke_failure_callbacks(
                            kernel_db_conn, sched_ctx, sess_ctx, check_results,
                        )
                        query = kernels.update().values({
                            'status_info': "no-available-instances",
                            'status_data': sql_json_increment(
                                kernels.c.status_data,
                                ('scheduler', 'retries'),
                                parent_updates={
                                    'last_try': datetime.now(tzutc()).isoformat(),
                                }
                            ),
                        }).where(kernels.c.id == kernel.kernel_id)
                        await kernel_db_conn.execute(query)
                    raise
                except Exception as e:
                    log.exception(
                        log_fmt + 'unexpected-error, during agent allocation',
                        *log_args,
                    )
                    async with kernel_db_conn.begin():
                        await _invoke_failure_callbacks(
                            kernel_db_conn, sched_ctx, sess_ctx, check_results,
                        )
                        query = kernels.update().values({
                            'status_info': "scheduler-error",
                            'status_data': convert_to_status_data(e),
                        }).where(kernels.c.id == kernel.kernel_id)
                        await kernel_db_conn.execute(query)
                    raise
                else:
                    kernel_agent_bindings.append(KernelAgentBinding(kernel, agent_alloc_ctx))

        if len(kernel_agent_bindings) == len(sess_ctx.kernels):
            # Proceed to PREPARING only when all kernels are successfully scheduled.
            async with kernel_db_conn.begin():
                for binding in kernel_agent_bindings:
                    query = kernels.update().values({
                        'agent': binding.agent_alloc_ctx.agent_id,
                        'agent_addr': binding.agent_alloc_ctx.agent_addr,
                        'scaling_group': sgroup_name,
                        'status': KernelStatus.PREPARING,
                        'status_info': 'scheduled',
                        'status_data': {},
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == binding.kernel.kernel_id)
                    await kernel_db_conn.execute(query)

        return (sess_ctx, kernel_agent_bindings)

    async def start_session(
        self,
        log_args,
        sched_ctx: SchedulingContext,
        session_agent_binding: Tuple[PendingSession, List[KernelAgentBinding]],
        check_results: List[Tuple[str, Union[Exception, PredicateResult]]],
    ) -> None:
        log_fmt = _log_fmt.get()
        log.debug(log_fmt + 'try-starting', *log_args)
        sess_ctx = session_agent_binding[0]
        await self.registry.event_producer.produce_event(
            SessionScheduledEvent(sess_ctx.session_id, sess_ctx.session_creation_id)
        )
        try:
            assert len(session_agent_binding[1]) > 0
            assert len(sess_ctx.kernels) == len(session_agent_binding[1])
            await self.registry.start_session(sched_ctx, session_agent_binding)
        except Exception as e:
            status_data = convert_to_status_data(e, self.local_config['debug']['enabled'])
            log.warning(log_fmt + 'failed-starting: {!r}', *log_args, status_data)
            try:
                async with self.db.begin() as db_conn:
                    query = (
                        sa.select([kernels.c.id, kernels.c.container_id])
                        .select_from(kernels)
                        .where(kernels.c.session_id == sess_ctx.session_id)
                    )
                    rows = (await db_conn.execute(query)).fetchall()
                    cid_map = {row['id']: row['container_id'] for row in rows}
                destroyed_kernels = [
                    {
                        "agent": binding.agent_alloc_ctx.agent_id,
                        "agent_addr": binding.agent_alloc_ctx.agent_addr,
                        "id": binding.kernel.kernel_id,
                        "container_id": cid_map[binding.kernel.kernel_id],
                    }
                    for binding in session_agent_binding[1]
                ]
                await self.registry.destroy_session_lowlevel(sess_ctx.session_id, destroyed_kernels)
            except Exception as destroy_err:
                log.error(log_fmt + 'failed-starting.cleanup', *log_args, exc_info=destroy_err)
            async with self.db.begin() as db_conn:
                for binding in session_agent_binding[1]:
                    await recalc_agent_resource_occupancy(db_conn, binding.agent_alloc_ctx.agent_id)
                await _invoke_failure_callbacks(db_conn, sched_ctx, sess_ctx, check_results)
                now = datetime.now(tzutc())
                query = kernels.update().values({
                    'status': KernelStatus.CANCELLED,
                    'status_changed': now,
                    'status_info': "failed-to-start",
                    'status_data': status_data,
                    'terminated_at': now,
                }).where(kernels.c.session_id == sess_ctx.session_id)
                await db_conn.execute(query)
            await self.registry.event_producer.produce_event(
                SessionCancelledEvent(
                    sess_ctx.session_id, sess_ctx.session_creation_id, "failed-to-start",
                )
            )
        else:
            log.info(log_fmt + 'started', *log_args)
            async with self.db.begin() as db_conn:
                await _invoke_success_callbacks(db_conn, sched_ctx, sess_ctx, check_results)


async def _list_pending_sessions(
    db_conn: SAConnection,
    sgroup_name: str,
) -> List[PendingSession]:
    query = (
        sa.select([
            kernels.c.id,
            kernels.c.session_creation_id,
            kernels.c.status,
            kernels.c.image,
            kernels.c.cluster_mode,
            kernels.c.cluster_size,
            kernels.c.cluster_role,
            kernels.c.cluster_idx,
            kernels.c.cluster_hostname,
            kernels.c.registry,
            kernels.c.session_id,
            kernels.c.session_type,
            kernels.c.session_name,
            kernels.c.access_key,
            kernels.c.domain_name,
            kernels.c.group_id,
            kernels.c.scaling_group,
            kernels.c.occupied_slots,
            kernels.c.resource_opts,
            kernels.c.environ,
            kernels.c.mounts,
            kernels.c.mount_map,
            kernels.c.bootstrap_script,
            kernels.c.startup_command,
            kernels.c.internal_data,
            kernels.c.preopen_ports,
            keypairs.c.resource_policy,
        ])
        .select_from(sa.join(
            kernels, keypairs,
            keypairs.c.access_key == kernels.c.access_key
        ))
        .where(
            (kernels.c.status == KernelStatus.PENDING) &
            (
                (kernels.c.scaling_group == sgroup_name) |
                (kernels.c.scaling_group.is_(None))
            )
        )
        .order_by(kernels.c.created_at)
    )
    items: Dict[SessionId, PendingSession] = {}
    rows = (await db_conn.execute(query)).fetchall()
    for row in rows:
        if row['cluster_role'] == "main":
            session = PendingSession(
                kernels=[],
                access_key=row['access_key'],
                session_id=row['session_id'],
                session_creation_id=row['session_creation_id'],
                session_type=row['session_type'],
                session_name=row['session_name'],
                cluster_mode=row['cluster_mode'],
                cluster_size=row['cluster_size'],
                domain_name=row['domain_name'],
                group_id=row['group_id'],
                scaling_group=row['scaling_group'],
                resource_policy=row['resource_policy'],
                resource_opts={},
                requested_slots=ResourceSlot(),
                internal_data=row['internal_data'],
                target_sgroup_names=[],
                environ={
                    k: v for k, v
                    in map(lambda s: s.split('=', maxsplit=1), row['environ'])
                },
                mounts=row['mounts'],
                mount_map=row['mount_map'],
                bootstrap_script=row['bootstrap_script'],
                startup_command=row['startup_command'],
                preopen_ports=row['preopen_ports'],
            )
            items[row['session_id']] = session
    for row in rows:
        session = items[row['session_id']]
        session.kernels.append(KernelInfo(
            kernel_id=row['id'],
            session_id=row['session_id'],
            access_key=row['access_key'],
            cluster_role=row['cluster_role'],
            cluster_idx=row['cluster_idx'],
            cluster_hostname=row['cluster_hostname'],
            image_ref=ImageRef(row['image'], [row['registry']]),
            bootstrap_script=row['bootstrap_script'],
            startup_command=row['startup_command'],
            resource_opts=row['resource_opts'],
            requested_slots=row['occupied_slots'],
        ))
        session.requested_slots += row['occupied_slots']  # type: ignore
        merge_resource(session.resource_opts, row['resource_opts'])  # type: ignore
    return list(items.values())


async def _list_existing_sessions(
    db_conn: SAConnection,
    sgroup: str,
) -> List[ExistingSession]:
    query = (
        sa.select([
            kernels.c.id,
            kernels.c.status,
            kernels.c.image,
            kernels.c.cluster_mode,
            kernels.c.cluster_size,
            kernels.c.cluster_role,
            kernels.c.cluster_idx,
            kernels.c.cluster_hostname,
            kernels.c.registry,
            kernels.c.session_id,
            kernels.c.session_type,
            kernels.c.session_name,
            kernels.c.access_key,
            kernels.c.domain_name,
            kernels.c.group_id,
            kernels.c.scaling_group,
            kernels.c.occupied_slots,
            kernels.c.resource_opts,
            kernels.c.environ,
            kernels.c.mounts,
            kernels.c.mount_map,
            kernels.c.startup_command,
            kernels.c.internal_data,
            keypairs.c.resource_policy,
        ])
        .select_from(sa.join(
            kernels, keypairs,
            keypairs.c.access_key == kernels.c.access_key
        ))
        .where(
            (kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES)) &
            (kernels.c.scaling_group == sgroup)
        )
        .order_by(kernels.c.created_at)
    )
    items: Dict[str, ExistingSession] = {}
    rows = (await db_conn.execute(query)).fetchall()
    for row in rows:
        if row['cluster_role'] == "main":
            session = ExistingSession(
                kernels=[],
                access_key=row['access_key'],
                session_id=row['session_id'],
                session_type=row['session_type'],
                session_name=row['session_name'],
                cluster_mode=row['cluster_mode'],
                cluster_size=row['cluster_size'],
                domain_name=row['domain_name'],
                group_id=row['group_id'],
                scaling_group=row['scaling_group'],
                occupying_slots=ResourceSlot(),
            )
            items[row['session_id']] = session
    for row in rows:
        session_id = row['session_id']
        if session_id not in items:
            # In some cases, sub containers are still RUNNING even though main container is TERMINATED.
            # To circumvent this edge case, we skip if main container is not registered in `items`.
            continue
        session = items[session_id]
        session.kernels.append(KernelInfo(  # type: ignore
            kernel_id=row['id'],
            session_id=row['session_id'],
            access_key=row['access_key'],
            cluster_role=row['cluster_role'],
            cluster_idx=row['cluster_idx'],
            cluster_hostname=row['cluster_hostname'],
            image_ref=ImageRef(row['image'], [row['registry']]),
            bootstrap_script=None,
            startup_command=None,
            resource_opts=row['resource_opts'],
            requested_slots=row['occupied_slots'],
        ))
        session.occupying_slots += row['occupied_slots']  # type: ignore
    return list(items.values())


async def _list_agents_by_sgroup(
    db_conn: SAConnection,
    sgroup_name: str,
) -> Sequence[AgentContext]:
    query = (
        sa.select([
            agents.c.id,
            agents.c.addr,
            agents.c.scaling_group,
            agents.c.available_slots,
            agents.c.occupied_slots,
        ])
        .select_from(agents)
        .where(
            (agents.c.status == AgentStatus.ALIVE) &
            (agents.c.scaling_group == sgroup_name) &
            (agents.c.schedulable == true())
        )
    )
    items = []
    for row in (await db_conn.execute(query)):
        item = AgentContext(
            row['id'],
            row['addr'],
            row['scaling_group'],
            row['available_slots'],
            row['occupied_slots'],
        )
        items.append(item)
    return items


async def _reserve_agent(
    sched_ctx: SchedulingContext,
    db_conn: SAConnection,
    scaling_group: str,
    agent_id: AgentId,
    requested_slots: ResourceSlot,
    extra_conds: Any = None,
) -> AgentAllocationContext:
    query = (
        sa.select([agents.c.occupied_slots])
        .select_from(agents)
        .where(agents.c.id == agent_id))
    if extra_conds is not None:
        query = query.where(extra_conds)
    current_occupied_slots = await db_conn.scalar(query)
    if current_occupied_slots is None:
        raise RuntimeError(f"No agent matching condition: {extra_conds}")
    query = (sa.update(agents)
               .values({
                   'occupied_slots': current_occupied_slots + requested_slots
               })
               .where(agents.c.id == agent_id))
    await db_conn.execute(query)

    # Get the agent address for later RPC calls
    query = (sa.select([agents.c.addr])
               .where(agents.c.id == agent_id))
    agent_addr = await db_conn.scalar(query)
    assert agent_addr is not None

    return AgentAllocationContext(agent_id, agent_addr, scaling_group)


async def _unreserve_agent_slots(
    db_conn: SAConnection,
    session_agent_binding: Tuple[PendingSession, List[KernelAgentBinding]],
) -> None:
    # Un-reserve agent slots, using the db transaction of the current invocation context.
    keyfunc = lambda item: item.agent_alloc_ctx.agent_id
    for agent_id, kernel_agent_bindings in itertools.groupby(
        sorted(session_agent_binding[1], key=keyfunc), key=keyfunc
    ):
        per_agent_requested_slots = sum(
            (binding.kernel.requested_slots for binding in kernel_agent_bindings),
            start=ResourceSlot(),
        )
        query = (
            sa.select([agents.c.occupied_slots])
            .select_from(agents)
            .where(agents.c.id == agent_id)
            .with_for_update()
        )
        current_occupied_slots = await db_conn.scalar(query)
        query = (
            sa.update(agents)
            .values({
                'occupied_slots': current_occupied_slots - per_agent_requested_slots
            })
            .where(agents.c.id == agent_id))
        await db_conn.execute(query)


async def _invoke_success_callbacks(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
    results: List[Tuple[str, Union[Exception, PredicateResult]]],
    *,
    use_new_txn: bool = False,
) -> None:
    """
    Give predicates chances to finalize/add DB changes.
    """
    callbacks: List[Awaitable[None]] = []
    for predicate_name, result in results:
        if isinstance(result, Exception):
            # This won't happen but this code is required to pass static check.
            continue
        if result.success_cb is not None:
            callbacks.append(result.success_cb(db_conn, sched_ctx, sess_ctx))
    for cb in reversed(callbacks):
        await cb


async def _invoke_failure_callbacks(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
    results: List[Tuple[str, Union[Exception, PredicateResult]]],
    *,
    use_new_txn: bool = False,
) -> None:
    """
    Rollback any changes performed by predicates.

    NOTE: We don't use the DB-level transaction rollback because we need to
    store the "ERROR" status to corresponding rows in the kernels table.
    """
    callbacks: List[Awaitable[None]] = []
    for predicate_name, result in results:
        if isinstance(result, Exception):
            # This won't happen but this code is required to pass static check.
            continue
        if result.failure_cb:
            callbacks.append(result.failure_cb(db_conn, sched_ctx, sess_ctx))
    for cb in reversed(callbacks):
        await cb
