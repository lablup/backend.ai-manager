from __future__ import annotations

import asyncio
from datetime import datetime
import itertools
import logging
import pkg_resources
from typing import (
    Any,
    Awaitable,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

from aiopg.sa.connection import SAConnection
import aioredis
import aioredlock
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    aobject,
    AgentId,
    ClusterMode,
    ResourceSlot,
)
if TYPE_CHECKING:
    from ai.backend.gateway.config import LocalConfig, SharedConfig
    from ai.backend.gateway.events import EventDispatcher

from ai.backend.manager.distributed import GlobalTimer
from ...gateway.defs import REDIS_STREAM_DB
from ...gateway.exceptions import InstanceNotAvailable
if TYPE_CHECKING:
    from ..registry import AgentRegistry
from ..models import (
    agents, kernels, keypairs, scaling_groups,
    AgentStatus, KernelStatus,
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
)
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

__all__ = (
    'load_scheduler',
    'SchedulerDispatcher',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))


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


class SchedulerDispatcher(aobject):

    config: LocalConfig
    shared_config: SharedConfig
    registry: AgentRegistry

    lock_manager: aioredlock.Aioredlock
    timer_redis: aioredis.Redis
    event_dispatcher: EventDispatcher
    schedule_timer: GlobalTimer

    def __init__(
        self,
        local_config: LocalConfig,
        shared_config: SharedConfig,
        event_dispatcher: EventDispatcher,
        registry: AgentRegistry,
    ) -> None:
        self.local_config = local_config
        self.shared_config = shared_config
        self.event_dispatcher = event_dispatcher
        self.registry = registry
        self.dbpool = registry.dbpool

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        self.registry.event_dispatcher.consume('session_enqueued', None, self.schedule)
        self.registry.event_dispatcher.consume('session_terminated', None, self.schedule)
        self.registry.event_dispatcher.consume('instance_started', None, self.schedule)
        self.registry.event_dispatcher.consume('do_schedule', None, self.schedule)
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
            self.event_dispatcher,
            'do_schedule',
            interval=10.0,
        )
        await self.schedule_timer.join()

    async def close(self) -> None:
        log.info('Session scheduler stopped')
        await self.schedule_timer.leave()
        self.timer_redis.close()
        await self.timer_redis.wait_closed()

    async def schedule(self, ctx: object, agent_id: AgentId, event_name: str,
                       *args, **kwargs) -> None:
        try:
            lock = await self.lock_manager.lock('manager.scheduler')
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

        log_fmt = 'schedule(s:{}, type:{}, name:{}, ak:{}, cluster_mode:{}): '
        start_task_args: List[Tuple[
            Tuple[Any, ...],
            SchedulingContext,
            Tuple[PendingSession, List[KernelAgentBinding]],
            List[Union[Exception, PredicateResult]],
        ]]
        start_task_args = []

        async def _schedule_in_sgroup(db_conn: SAConnection, sgroup_name: str) -> None:
            async with db_conn.begin():
                scheduler = await self._load_scheduler(db_conn, sgroup_name)
                pending_sessions = await _list_pending_sessions(db_conn, sgroup_name)
                existing_sessions = await _list_existing_sessions(db_conn, sgroup_name)
            log.debug('running scheduler (sgroup:{}, pending:{}, existing:{})',
                      sgroup_name, len(pending_sessions), len(existing_sessions))
            zero = ResourceSlot()
            while len(pending_sessions) > 0:
                async with db_conn.begin():
                    candidate_agents = await _list_agents_by_sgroup(db_conn, sgroup_name)
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

                log_args = (
                    sess_ctx.session_id,
                    sess_ctx.session_type,
                    sess_ctx.session_name,
                    sess_ctx.access_key,
                    sess_ctx.cluster_mode,
                )
                log.debug(log_fmt + 'try-scheduling', *log_args)
                session_agent_binding: Tuple[PendingSession, List[KernelAgentBinding]]

                async with db_conn.begin():
                    predicates: Sequence[Awaitable[PredicateResult]] = [
                        check_reserved_batch_session(db_conn, sched_ctx, sess_ctx),
                        check_concurrency(db_conn, sched_ctx, sess_ctx),
                        check_dependencies(db_conn, sched_ctx, sess_ctx),
                        check_keypair_resource_limit(db_conn, sched_ctx, sess_ctx),
                        check_group_resource_limit(db_conn, sched_ctx, sess_ctx),
                        check_domain_resource_limit(db_conn, sched_ctx, sess_ctx),
                        check_scaling_group(db_conn, sched_ctx, sess_ctx),
                    ]
                    check_results: List[Union[Exception, PredicateResult]] = []
                    for check in predicates:
                        try:
                            check_results.append(await check)
                        except Exception as e:
                            log.exception(log_fmt + 'predicate-error', *log_args)
                            check_results.append(e)
                    has_failure = False
                    for result in check_results:
                        if isinstance(result, Exception):
                            has_failure = True
                            continue
                        if not result.passed:
                            has_failure = True
                    if has_failure:
                        log.debug(log_fmt + 'predicate-checks-failed', *log_args)
                        await _invoke_failure_callbacks(
                            db_conn, sched_ctx, sess_ctx, check_results,
                        )
                        # Predicate failures are *NOT* permanent errors.
                        # We need to retry the scheduling afterwards.
                        continue

                    if sess_ctx.cluster_mode == ClusterMode.SINGLE_NODE:
                        # Assign agent resource per session.
                        try:
                            agent_id = scheduler.assign_agent_for_session(candidate_agents, sess_ctx)
                            if agent_id is None:
                                raise InstanceNotAvailable
                            agent_alloc_ctx = await _reserve_agent(
                                sched_ctx, db_conn, sgroup_name, agent_id, sess_ctx.requested_slots,
                            )
                        except InstanceNotAvailable:
                            log.debug(log_fmt + 'no-available-instances', *log_args)
                            await _invoke_failure_callbacks(
                                db_conn, sched_ctx, sess_ctx, check_results,
                            )
                            raise
                        except Exception:
                            log.exception(log_fmt + 'unexpected-error, during agent allocation',
                                          *log_args)
                            await _invoke_failure_callbacks(
                                db_conn, sched_ctx, sess_ctx, check_results,
                            )
                            raise
                        query = kernels.update().values({
                            'agent': agent_alloc_ctx.agent_id,
                            'agent_addr': agent_alloc_ctx.agent_addr,
                            'scaling_group': sgroup_name,
                            'status': KernelStatus.PREPARING,
                            'status_info': 'scheduled',
                            'status_changed': datetime.now(tzutc()),
                        }).where(kernels.c.session_id == sess_ctx.session_id)
                        await db_conn.execute(query)
                        session_agent_binding = (
                            sess_ctx,
                            [
                                KernelAgentBinding(kernel, agent_alloc_ctx)
                                for kernel in sess_ctx.kernels
                            ],
                        )
                    elif sess_ctx.cluster_mode == ClusterMode.MULTI_NODE:
                        # Assign agent resource per kernel in the session.
                        agent_query_extra_conds = None
                        if len(sess_ctx.kernels) >= 2:
                            # We should use agents that supports overlay networking.
                            agent_query_extra_conds = (agents.c.clusterized)
                        kernel_agent_bindings = []
                        for kernel in sess_ctx.kernels:
                            try:
                                agent_id = scheduler.assign_agent_for_kernel(candidate_agents, kernel)
                                if agent_id is None:
                                    raise InstanceNotAvailable
                                agent_alloc_ctx = await _reserve_agent(
                                    sched_ctx, db_conn, sgroup_name, agent_id, kernel.requested_slots,
                                    extra_conds=agent_query_extra_conds,
                                )
                            except InstanceNotAvailable:
                                log.debug(log_fmt + 'no-available-instances', *log_args)
                                await _invoke_failure_callbacks(
                                    db_conn, sched_ctx, sess_ctx, check_results,
                                )
                                # continue
                                raise
                            except Exception:
                                log.exception(log_fmt + 'unexpected-error, during agent allocation',
                                              *log_args)
                                await _invoke_failure_callbacks(
                                    db_conn, sched_ctx, sess_ctx, check_results,
                                )
                                # continue
                                raise
                            # TODO: if error occurs for one kernel, should we cancel all others?
                            query = kernels.update().values({
                                'agent': agent_alloc_ctx.agent_id,
                                'agent_addr': agent_alloc_ctx.agent_addr,
                                'scaling_group': sgroup_name,
                                'status': KernelStatus.PREPARING,
                                'status_info': 'scheduled',
                                'status_changed': datetime.now(tzutc()),
                            }).where(kernels.c.id == kernel.kernel_id)
                            await db_conn.execute(query)
                            kernel_agent_bindings.append(KernelAgentBinding(kernel, agent_alloc_ctx))
                        session_agent_binding = (sess_ctx, kernel_agent_bindings)
                    start_task_args.append(
                        (
                            log_args,
                            sched_ctx,
                            session_agent_binding,
                            check_results,
                        )
                    )

        # We use short transaction blocks to prevent deadlock timeouts under heavy loads
        # because this scheduling handler will be executed by only one process.
        # It is executed under a globally exclusive context using aioredlock.
        async with self.dbpool.acquire() as db_conn:
            query = (
                sa.select([agents.c.scaling_group])
                .select_from(agents)
                .where(agents.c.status == AgentStatus.ALIVE)
                .group_by(agents.c.scaling_group)
            )
            schedulable_scaling_groups = [
                row.scaling_group async for row in db_conn.execute(query)
            ]
            for sgroup_name in schedulable_scaling_groups:
                await _schedule_in_sgroup(db_conn, sgroup_name)

        async def start_session(
            log_args,
            sched_ctx: SchedulingContext,
            session_agent_binding: Tuple[PendingSession, List[KernelAgentBinding]],
            check_results,
        ) -> None:
            log.debug(log_fmt + 'try-starting', *log_args)
            sess_ctx = session_agent_binding[0]
            await self.registry.event_dispatcher.produce_event(
                'session_scheduled',
                (str(sess_ctx.session_id), ),
            )
            try:
                assert len(session_agent_binding[1]) > 0
                assert len(sess_ctx.kernels) == len(session_agent_binding[1])
                await self.registry.start_session(sched_ctx, session_agent_binding)
            except Exception as e:
                # TODO: handle exception as "multi-error" and rollback only the agents that are affected
                log.error(log_fmt + 'failed-starting', *log_args, exc_info=e)
                async with self.dbpool.acquire(), db_conn.begin():
                    await _unreserve_agent_slots(db_conn, session_agent_binding)
                    await _invoke_failure_callbacks(db_conn, sched_ctx, sess_ctx, check_results)
                    query = kernels.update().values({
                        'status': KernelStatus.CANCELLED,
                        'status_info': 'failed-to-start',
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.session_id == sess_ctx.session_id)
                    await db_conn.execute(query)
                await self.registry.event_dispatcher.produce_event(
                    'session_cancelled',
                    (str(sess_ctx.session_id), 'failed-to-start'),
                )
            else:
                log.info(log_fmt + 'started', *log_args)
                async with self.dbpool.acquire(), db_conn.begin():
                    await _invoke_success_callbacks(db_conn, sched_ctx, sess_ctx, check_results)

        start_coros = []
        for argset in start_task_args:
            start_coros.append(start_session(*argset))
        await asyncio.gather(*start_coros, return_exceptions=True)

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
        scheduler_name = await result.scalar()
        return load_scheduler(scheduler_name, self.shared_config['plugins']['scheduler'])


async def _list_pending_sessions(
    db_conn: SAConnection,
    sgroup_name: str,
) -> List[PendingSession]:
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
    # TODO: extend for multi-container sessions
    items: MutableMapping[str, PendingSession] = {}
    async for row in db_conn.execute(query):
        if _session := items.get(row['session_id']):
            session = _session
        else:
            session = PendingSession(
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
    items: MutableMapping[str, ExistingSession] = {}
    async for row in db_conn.execute(query):
        if _session := items.get(row['session_id']):
            session = _session
        else:
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
        # TODO: support multi-container sessions
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
        ], for_update=True)
        .select_from(agents)
        .where(
            (agents.c.status == AgentStatus.ALIVE) &
            (agents.c.scaling_group == sgroup_name) &
            (agents.c.schedulable == true())
        )
    )
    items = []
    async for row in db_conn.execute(query):
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
        sa.select([agents.c.occupied_slots], for_update=True)
        .select_from(agents)
        .where(agents.c.id == agent_id))
    if extra_conds is not None:
        query = query.where(extra_conds)
    current_occupied_slots = await db_conn.scalar(query)
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
            sa.select([agents.c.occupied_slots], for_update=True)
            .select_from(agents)
            .where(agents.c.id == agent_id))
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
    results: List[Union[Exception, PredicateResult]],
    *,
    use_new_txn: bool = False,
) -> None:
    """
    Give predicates chances to finalize/add DB changes.
    """
    callbacks: List[Awaitable[None]] = []
    for result in results:
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
    results: List[Union[Exception, PredicateResult]],
    *,
    use_new_txn: bool = False,
) -> None:
    """
    Rollback any changes performed by predicates.

    NOTE: We don't use the DB-level transaction rollback because we need to
    store the "ERROR" status to corresponding rows in the kernels table.
    """
    callbacks: List[Awaitable[None]] = []
    for result in results:
        if isinstance(result, Exception):
            # This won't happen but this code is required to pass static check.
            continue
        if result.failure_cb:
            callbacks.append(result.failure_cb(db_conn, sched_ctx, sess_ctx))
    for cb in reversed(callbacks):
        await cb
