from __future__ import annotations

import asyncio
from contextvars import ContextVar
from datetime import datetime
import logging
import pkg_resources
from typing import (
    Any,
    Awaitable,
    Final,
    List,
    Mapping,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

import aioredis
import aiotools
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection as SAConnection,
    AsyncEngine as SAEngine,
)
from sqlalchemy.sql.expression import true

from ai.backend.common.events import (
    AgentStartedEvent,
    CoalescingOptions,
    DoScheduleEvent,
    DoPrepareEvent,
    SessionCancelledEvent,
    SessionEnqueuedEvent,
    SessionPreparingEvent,
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
)

from ..api.exceptions import InstanceNotAvailable
from ..distributed import GlobalTimer
from ..defs import (
    AdvisoryLock,
    REDIS_STREAM_DB,
)
from ..exceptions import convert_to_status_data
from ..models import (
    agents, kernels, scaling_groups,
    recalc_agent_resource_occupancy,
    recalc_concurrency_used,
    AgentStatus, KernelStatus,
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
)
from ..models.utils import (
    execute_with_retry,
    sql_json_increment,
    sql_json_merge,
)
from .types import (
    PredicateResult,
    PendingSession,
    ExistingSession,
    SchedulingContext,
    AgentContext,
    AgentAllocationContext,
    AbstractScheduler,
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

_key_schedule_prep_tasks: Final = "scheduler.preptasks"


def load_scheduler(name: str, scheduler_configs: Mapping[str, Any]) -> AbstractScheduler:
    entry_prefix = 'backendai_scheduler_v10'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        if entrypoint.name == name:
            log.debug('loading scheduler plugin "{}" from {}', name, entrypoint.module_name)
            scheduler_cls = entrypoint.load()
            scheduler_config = scheduler_configs.get(name, {})
            return scheduler_cls(scheduler_config)
    raise ImportError('Cannot load the scheduler plugin', name)


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
    db: SAEngine

    schedule_timer_redis: aioredis.Redis
    prepare_timer_redis: aioredis.Redis
    event_dispatcher: EventDispatcher
    event_producer: EventProducer
    schedule_timer: GlobalTimer
    prepare_timer: GlobalTimer

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

    async def __ainit__(self) -> None:
        coalescing_opts: CoalescingOptions = {
            'max_wait': 0.5,
            'max_batch_size': 32,
        }
        # coalescing_opts = None
        evd = self.registry.event_dispatcher
        evd.consume(SessionEnqueuedEvent, None, self.schedule, coalescing_opts, name="dispatcher.enq")
        evd.consume(SessionTerminatedEvent, None, self.schedule, coalescing_opts, name="dispatcher.term")
        evd.consume(AgentStartedEvent, None, self.schedule)
        evd.consume(DoScheduleEvent, None, self.schedule, coalescing_opts)
        evd.consume(DoPrepareEvent, None, self.prepare)
        redis_url = self.shared_config.get_redis_url(db=REDIS_STREAM_DB)
        self.schedule_timer_redis = await aioredis.create_redis(str(redis_url))
        self.prepare_timer_redis = await aioredis.create_redis(str(redis_url))
        self.schedule_timer = GlobalTimer(
            self.schedule_timer_redis,
            "scheduler_tick",
            self.event_producer,
            lambda: DoScheduleEvent(),
            interval=10.0,
        )
        self.prepare_timer = GlobalTimer(
            self.prepare_timer_redis,
            "prepare_tick",
            self.event_producer,
            lambda: DoPrepareEvent(),
            interval=10.0,
            initial_delay=5.0,
        )
        await self.schedule_timer.join()
        await self.prepare_timer.join()
        log.info('Session scheduler started')

    async def close(self) -> None:
        await self.prepare_timer.leave()
        await self.schedule_timer.leave()
        log.info('Session scheduler stopped')
        self.prepare_timer_redis.close()
        self.schedule_timer_redis.close()
        await self.prepare_timer_redis.wait_closed()
        await self.schedule_timer_redis.wait_closed()

    async def schedule(
        self,
        context: None,
        source: AgentId,
        event: SessionEnqueuedEvent | SessionTerminatedEvent | AgentStartedEvent | DoScheduleEvent,
    ) -> None:
        """
        Trigger the scheduler to scan pending sessions and mark them scheduled if they fulfill
        the scheduling requirements.

        HoL blocking issue due to indefinitely preparing sessions will be mitigated because
        they will be treated as already "scheduled" sessions and the scheduler will continue to
        work on other pending sessions.

        Session status transition: PENDING -> SCHEDULED
        """
        log.debug('schedule(): triggered')
        known_slot_types = await self.shared_config.get_resource_slots()
        sched_ctx = SchedulingContext(
            registry=self.registry,
            known_slot_types=known_slot_types,
        )

        # We use short transaction blocks to prevent deadlock timeouts under heavy loads
        # because this scheduling handler will be executed by only one process.
        # It is executed under a globally exclusive context using aioredlock.
        try:
            async with self.db.advisory_lock(AdvisoryLock.LOCKID_SCHEDULE):
                async with self.db.begin_readonly() as conn:
                    query = (
                        sa.select([agents.c.scaling_group])
                        .select_from(agents)
                        .where(agents.c.status == AgentStatus.ALIVE)
                        .group_by(agents.c.scaling_group)
                    )
                    result = await conn.execute(query)
                    schedulable_scaling_groups = [
                        row.scaling_group for row in result.fetchall()
                    ]
                for sgroup_name in schedulable_scaling_groups:
                    try:
                        await self._schedule_in_sgroup(
                            sched_ctx, sgroup_name,
                        )
                    except InstanceNotAvailable:
                        # Proceed to the next scaling group and come back later.
                        log.debug('schedule({}): instance not available', sgroup_name)
                    except Exception as e:
                        log.exception('schedule({}): scheduling error!\n{}', sgroup_name, repr(e))
        except DBAPIError as e:
            if getattr(e.orig, 'pgcode', None) == '55P03':
                log.info("schedule(): cancelled due to advisory lock timeout; "
                         "maybe another schedule() call is still running")
                raise asyncio.CancelledError()
            raise

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
        sgroup_name: str,
    ) -> None:
        async with self.db.begin_readonly() as kernel_db_conn:
            scheduler = await self._load_scheduler(kernel_db_conn, sgroup_name)
            pending_sessions = await _list_pending_sessions(kernel_db_conn, sgroup_name)
            existing_sessions = await _list_existing_sessions(kernel_db_conn, sgroup_name)
        log.debug('running scheduler (sgroup:{}, pending:{}, existing:{})',
                  sgroup_name, len(pending_sessions), len(existing_sessions))
        zero = ResourceSlot()
        num_scheduled = 0
        while len(pending_sessions) > 0:

            async with self.db.begin() as conn:
                candidate_agents = await _list_agents_by_sgroup(conn, sgroup_name)
                total_capacity = sum((ag.available_slots for ag in candidate_agents), zero)

            picked_session_id = scheduler.pick_session(
                total_capacity,
                pending_sessions,
                existing_sessions,
            )
            if picked_session_id is None:
                # no session is picked.
                # continue to next sgroup.
                return
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

            async def _check_predicates() -> List[Tuple[str, Union[Exception, PredicateResult]]]:
                check_results: List[Tuple[str, Union[Exception, PredicateResult]]] = []
                async with self.db.begin() as kernel_db_conn:
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
                    for predicate_name, check_coro in predicates:
                        try:
                            check_results.append((predicate_name, await check_coro))
                        except DBAPIError:
                            raise
                        except Exception as e:
                            log.exception(log_fmt + 'predicate-error', *log_args)
                            check_results.append((predicate_name, e))
                return check_results

            check_results = await execute_with_retry(_check_predicates)
            has_failure = False
            has_permanent_failure = False
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
                    if result.permanent:
                        has_permanent_failure = True  # noqa
            if has_failure:
                log.debug(log_fmt + 'predicate-checks-failed (temporary)', *log_args)
                # TODO: handle has_permanent_failure as cancellation
                #  - An early implementation of it has caused DB query blocking due to
                #    the inclusion of the kernels.status field. :(
                #    Let's fix it.

                async def _update() -> None:
                    async with self.db.begin() as conn:
                        await _rollback_predicate_mutations(
                            conn, sched_ctx, sess_ctx,
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
                        await conn.execute(query)

                await execute_with_retry(_update)
                # Predicate failures are *NOT* permanent errors.
                # We need to retry the scheduling afterwards.
                continue
            else:
                async def _update() -> None:
                    async with self.db.begin() as conn:
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
                        await conn.execute(query)

                await execute_with_retry(_update)

            if sess_ctx.cluster_mode == ClusterMode.SINGLE_NODE:
                await self._schedule_single_node_session(
                    sched_ctx,
                    scheduler,
                    sgroup_name,
                    candidate_agents,
                    sess_ctx,
                    check_results,
                )
            elif sess_ctx.cluster_mode == ClusterMode.MULTI_NODE:
                await self._schedule_multi_node_session(
                    sched_ctx,
                    scheduler,
                    sgroup_name,
                    candidate_agents,
                    sess_ctx,
                    check_results,
                )
            else:
                raise RuntimeError(
                    f"should not reach here; unknown cluster_mode: {sess_ctx.cluster_mode}"
                )
            num_scheduled += 1
        if num_scheduled > 0:
            await self.event_producer.produce_event(DoPrepareEvent())

    async def _schedule_single_node_session(
        self,
        sched_ctx: SchedulingContext,
        scheduler: AbstractScheduler,
        sgroup_name: str,
        candidate_agents: Sequence[AgentContext],
        sess_ctx: PendingSession,
        check_results: List[Tuple[str, Union[Exception, PredicateResult]]],
    ) -> None:
        # Assign agent resource per session.
        log_fmt = _log_fmt.get()
        log_args = _log_args.get()
        try:
            agent_id = scheduler.assign_agent_for_session(candidate_agents, sess_ctx)
            if agent_id is None:
                raise InstanceNotAvailable
            async with self.db.begin() as agent_db_conn:
                agent_alloc_ctx = await _reserve_agent(
                    sched_ctx, agent_db_conn, sgroup_name, agent_id, sess_ctx.requested_slots,
                )
        except InstanceNotAvailable:
            log.debug(log_fmt + 'no-available-instances', *log_args)

            async def _update() -> None:
                async with self.db.begin() as kernel_db_conn:
                    await _rollback_predicate_mutations(
                        kernel_db_conn, sched_ctx, sess_ctx,
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

            await execute_with_retry(_update)
            raise
        except Exception as e:
            log.exception(
                log_fmt + 'unexpected-error, during agent allocation',
                *log_args,
            )
            exc_data = convert_to_status_data(e)

            async def _update() -> None:
                async with self.db.begin() as kernel_db_conn:
                    await _rollback_predicate_mutations(
                        kernel_db_conn, sched_ctx, sess_ctx,
                    )
                    query = kernels.update().values({
                        'status_info': "scheduler-error",
                        'status_data': exc_data,
                    }).where(kernels.c.id == sess_ctx.session_id)
                    await kernel_db_conn.execute(query)

            await execute_with_retry(_update)
            raise

        async def _finalize_scheduled() -> None:
            async with self.db.begin() as kernel_db_conn:
                query = kernels.update().values({
                    'agent': agent_alloc_ctx.agent_id,
                    'agent_addr': agent_alloc_ctx.agent_addr,
                    'scaling_group': sgroup_name,
                    'status': KernelStatus.SCHEDULED,
                    'status_info': 'scheduled',
                    'status_data': {},
                    'status_changed': datetime.now(tzutc()),
                }).where(kernels.c.session_id == sess_ctx.session_id)
                await kernel_db_conn.execute(query)

        await execute_with_retry(_finalize_scheduled)
        await self.registry.event_producer.produce_event(
            SessionScheduledEvent(sess_ctx.session_id, sess_ctx.session_creation_id)
        )

    async def _schedule_multi_node_session(
        self,
        sched_ctx: SchedulingContext,
        scheduler: AbstractScheduler,
        sgroup_name: str,
        candidate_agents: Sequence[AgentContext],
        sess_ctx: PendingSession,
        check_results: List[Tuple[str, Union[Exception, PredicateResult]]],
    ) -> None:
        # Assign agent resource per kernel in the session.
        log_fmt = _log_fmt.get()
        log_args = _log_args.get()
        agent_query_extra_conds = None
        kernel_agent_bindings: List[KernelAgentBinding] = []
        async with self.db.begin() as agent_db_conn:
            # This outer transaction is rolled back when any exception occurs inside,
            # including scheduling failures of a kernel.
            # It ensures that occupied_slots are recovered when there are partial
            # scheduling failures.
            for kernel in sess_ctx.kernels:
                try:
                    agent_alloc_ctx: AgentAllocationContext
                    agent_id = scheduler.assign_agent_for_kernel(candidate_agents, kernel)
                    if agent_id is None:
                        raise InstanceNotAvailable

                    async def _reserve() -> None:
                        nonlocal agent_alloc_ctx, candidate_agents
                        assert agent_id is not None
                        async with agent_db_conn.begin_nested():
                            agent_alloc_ctx = await _reserve_agent(
                                sched_ctx, agent_db_conn,
                                sgroup_name, agent_id, kernel.requested_slots,
                                extra_conds=agent_query_extra_conds,
                            )
                            candidate_agents = await _list_agents_by_sgroup(agent_db_conn, sgroup_name)

                    await execute_with_retry(_reserve)
                except InstanceNotAvailable:
                    log.debug(log_fmt + 'no-available-instances', *log_args)

                    async def _update() -> None:
                        async with self.db.begin() as kernel_db_conn:
                            await _rollback_predicate_mutations(
                                kernel_db_conn, sched_ctx, sess_ctx,
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

                    await execute_with_retry(_update)
                    raise
                except Exception as e:
                    log.exception(
                        log_fmt + 'unexpected-error, during agent allocation',
                        *log_args,
                    )
                    exc_data = convert_to_status_data(e)

                    async def _update() -> None:
                        async with self.db.begin() as kernel_db_conn:
                            await _rollback_predicate_mutations(
                                kernel_db_conn, sched_ctx, sess_ctx,
                            )
                            query = kernels.update().values({
                                'status_info': "scheduler-error",
                                'status_data': exc_data,
                            }).where(kernels.c.id == kernel.kernel_id)
                            await kernel_db_conn.execute(query)

                    await execute_with_retry(_update)
                    raise
                else:
                    kernel_agent_bindings.append(KernelAgentBinding(kernel, agent_alloc_ctx))

        assert len(kernel_agent_bindings) == len(sess_ctx.kernels)
        # Proceed to PREPARING only when all kernels are successfully scheduled.

        async def _finalize_scheduled() -> None:
            async with self.db.begin() as kernel_db_conn:
                for binding in kernel_agent_bindings:
                    query = kernels.update().values({
                        'agent': binding.agent_alloc_ctx.agent_id,
                        'agent_addr': binding.agent_alloc_ctx.agent_addr,
                        'scaling_group': sgroup_name,
                        'status': KernelStatus.SCHEDULED,
                        'status_info': 'scheduled',
                        'status_data': {},
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == binding.kernel.kernel_id)
                    await kernel_db_conn.execute(query)

        await execute_with_retry(_finalize_scheduled)
        await self.registry.event_producer.produce_event(
            SessionScheduledEvent(sess_ctx.session_id, sess_ctx.session_creation_id)
        )

    async def prepare(
        self,
        context: None,
        source: AgentId,
        event: DoPrepareEvent,
    ) -> None:
        """
        Scan the scheduled sessions and perform the agent RPC calls to begin preparation of them.
        Each RPC calls are done in separate asyncio tasks.

        Session status transition: SCHEDULED -> PREPARING
        """
        known_slot_types = await self.shared_config.get_resource_slots()
        sched_ctx = SchedulingContext(
            self.registry,
            known_slot_types,
        )
        try:
            async with self.db.advisory_lock(AdvisoryLock.LOCKID_PREPARE):
                now = datetime.now(tzutc())

                async def _transition() -> Sequence[PendingSession]:
                    async with self.db.begin() as conn:
                        update_query = (
                            sa.update(kernels)
                            .values({
                                'status': KernelStatus.PREPARING,
                                'status_changed': now,
                                'status_info': "",
                                'status_data': {},
                            })
                            .where(
                                (kernels.c.status == KernelStatus.SCHEDULED)
                            )
                            .returning(kernels.c.id)
                        )
                        rows = (await conn.execute(update_query)).fetchall()
                        if len(rows) == 0:
                            return []
                        target_kernel_ids = [r['id'] for r in rows]
                        select_query = (
                            PendingSession.base_query()
                            .where(
                                kernels.c.id.in_(target_kernel_ids)
                            )
                        )
                        rows = (await conn.execute(select_query)).fetchall()
                        return PendingSession.from_rows(rows)

                scheduled_sessions = await execute_with_retry(_transition)
                log.debug("prepare(): preparing {} session(s)", len(scheduled_sessions))
                async with aiotools.TaskGroup() as tg:
                    for scheduled_session in scheduled_sessions:
                        await self.registry.event_producer.produce_event(
                            SessionPreparingEvent(
                                scheduled_session.session_id,
                                scheduled_session.session_creation_id,
                            )
                        )
                        tg.create_task(self.start_session(
                            sched_ctx,
                            scheduled_session,
                        ))
        except DBAPIError as e:
            if getattr(e.orig, 'pgcode', None) == '55P03':
                log.info("prepare(): cancelled due to advisory lock timeout; "
                         "maybe another prepare() call is still running")
                raise asyncio.CancelledError()
            raise

    async def start_session(
        self,
        sched_ctx: SchedulingContext,
        session: PendingSession,
    ) -> None:
        log_fmt = "prepare(s:{0.session_id}, type:{0.session_type}, name:{0.session_name}, " \
                  "ak:{0.access_key}, cluster_mode:{0.cluster_mode}): "
        log_args = (session, )
        log.debug(log_fmt + 'try-starting', *log_args)
        try:
            assert len(session.kernels) > 0
            await self.registry.start_session(sched_ctx, session)
        except Exception as e:
            status_data = convert_to_status_data(e, self.local_config['debug']['enabled'])
            log.warning(log_fmt + 'failed-starting: {1!r}', *log_args, status_data)
            # TODO: instead of instantly cancelling upon exception, we could mark it as
            #       SCHEDULED and retry within some limit using status_data.

            async def _update() -> None:
                async with self.db.begin() as db_conn:
                    for k in session.kernels:
                        await recalc_agent_resource_occupancy(db_conn, k.agent_id)
                    await _rollback_predicate_mutations(db_conn, sched_ctx, session)
                    now = datetime.now(tzutc())
                    query = kernels.update().values({
                        'status': KernelStatus.CANCELLED,
                        'status_changed': now,
                        'status_info': "failed-to-start",
                        'status_data': status_data,
                        'terminated_at': now,
                    }).where(kernels.c.session_id == session.session_id)
                    await db_conn.execute(query)

            await execute_with_retry(_update)
            await self.registry.event_producer.produce_event(
                SessionCancelledEvent(
                    session.session_id,
                    session.session_creation_id,
                    "failed-to-start",
                )
            )
            log.debug(log_fmt + 'cleanup-start-failure: begin', *log_args)
            try:
                async with self.db.begin_readonly() as db_conn:
                    query = (
                        sa.select([kernels.c.id, kernels.c.container_id])
                        .where(kernels.c.session_id == session.session_id)
                    )
                    rows = (await db_conn.execute(query)).fetchall()
                    cid_map = {row['id']: row['container_id'] for row in rows}
                destroyed_kernels = [
                    {
                        "agent": k.agent_id,
                        "agent_addr": k.agent_addr,
                        "id": k.kernel_id,
                        "container_id": cid_map[k.kernel_id],
                    }
                    for k in session.kernels
                ]
                await self.registry.destroy_session_lowlevel(
                    session.session_id, destroyed_kernels,
                )
            except Exception as destroy_err:
                log.error(log_fmt + 'cleanup-start-failure: error', *log_args, exc_info=destroy_err)
            finally:
                log.debug(log_fmt + 'cleanup-start-failure: done', *log_args)
        else:
            log.info(log_fmt + 'started', *log_args)


async def _list_pending_sessions(
    db_conn: SAConnection,
    sgroup_name: str,
) -> List[PendingSession]:
    query = (
        PendingSession.base_query()
        .where(
            (kernels.c.status == KernelStatus.PENDING) &
            (
                (kernels.c.scaling_group == sgroup_name) |
                (kernels.c.scaling_group.is_(None))
            )
        )
    )
    rows = (await db_conn.execute(query)).fetchall()
    return PendingSession.from_rows(rows)


async def _list_existing_sessions(
    db_conn: SAConnection,
    sgroup: str,
) -> List[ExistingSession]:
    query = (
        ExistingSession.base_query()
        .where(
            (kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES)) &
            (kernels.c.scaling_group == sgroup)
        )
    )
    rows = (await db_conn.execute(query)).fetchall()
    return ExistingSession.from_rows(rows)


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
        .where(agents.c.id == agent_id)
        .with_for_update()
    )
    if extra_conds is not None:
        query = query.where(extra_conds)
    current_occupied_slots = (await db_conn.execute(query)).scalar()
    if current_occupied_slots is None:
        raise RuntimeError(f"No agent matching condition: {extra_conds}")
    update_query = (
        sa.update(agents)
        .values({
            'occupied_slots': current_occupied_slots + requested_slots
        })
        .where(agents.c.id == agent_id)
    )
    await db_conn.execute(update_query)

    # Get the agent address for later RPC calls
    query = (sa.select([agents.c.addr])
               .where(agents.c.id == agent_id))
    agent_addr = await db_conn.scalar(query)
    assert agent_addr is not None

    return AgentAllocationContext(agent_id, agent_addr, scaling_group)


async def _rollback_predicate_mutations(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    session: PendingSession,
) -> None:
    """
    Rollback any changes performed by predicates.

    NOTE: We don't use the DB-level transaction rollback because we need to
    store the "ERROR" status to corresponding rows in the kernels table.
    """

    # Instead of decrementing concurrency_used, we recalculate the access_key's usage,
    # because asynchronous container launch failures and agent failures
    # (especially with multi-node multi-container cluster sessions)
    # may accumulate up multiple subtractions, resulting in
    # negative concurrency_occupied values.
    await recalc_concurrency_used(db_conn, session.access_key)
