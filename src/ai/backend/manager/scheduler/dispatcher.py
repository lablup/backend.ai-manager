from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import pkg_resources
import time
from typing import (
    Any,
    Awaitable,
    List,
    Mapping,
    Sequence,
    Tuple,
    Union,
)

from aiopg.sa.connection import SAConnection
import aioredlock
from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from ai.backend.common import redis
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    aobject,
    AgentId,
    ResourceSlot,
)
from ai.backend.common.identity import get_instance_id
from ...gateway.defs import REDIS_LIVE_DB
from ...gateway.etcd import ConfigServer
from ...gateway.exceptions import InstanceNotAvailable
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


class SchedulerDispatcher(aobject):

    config_server: ConfigServer
    registry: AgentRegistry

    tick_script = '''
    local key_last_sync = KEYS[1]
    local key_schedulers = KEYS[2]
    local member_id = ARGV[1]
    local sync_interval = tonumber(ARGV[2])
    local sync_time = tonumber(redis.call('GET', key_last_sync))
    local current_time = tonumber(redis.call('TIME')[1])
    if sync_time == nil then
      redis.log(redis.LOG_NOTICE, "backend.ai scheduler - init sync_time")
      sync_time = current_time
      redis.call('SET', key_last_sync, sync_time)
    elseif current_time >= sync_time + sync_interval then
      redis.log(redis.LOG_NOTICE, "backend.ai scheduler - update sync_time")
      sync_time = sync_time + sync_interval
      if current_time - sync_time > sync_interval then
        redis.log(redis.LOG_NOTICE, "backend.ai scheduler - detected too old sync_time; resyncing")
        sync_time = current_time + sync_interval
      end
      redis.call('SET', key_last_sync, sync_time)
    end
    redis.call('SADD', key_schedulers, member_id)
    local members = redis.call('SMEMBERS', key_schedulers)
    local member_count = tonumber(redis.call('SCARD', key_schedulers))
    table.sort(members)
    for i, member in ipairs(members) do
      if member == member_id then
        local member_delay = i * (sync_interval / (member_count + 1))
        return {member_delay, sync_time}
      end
    end
    return {0, sync_time}
    '''

    def __init__(
        self,
        config: dict,
        config_server: ConfigServer,
        registry: AgentRegistry,
        pidx: int,
    ) -> None:
        self.config = config
        self.config_server = config_server
        self.registry = registry
        self.dbpool = registry.dbpool
        self.pidx = pidx

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        self.tick_task = asyncio.create_task(self.generate_scheduling_tick())
        self.registry.event_dispatcher.consume('kernel_enqueued', None, self.schedule)
        self.registry.event_dispatcher.consume('kernel_terminated', None, self.schedule)
        self.registry.event_dispatcher.consume('instance_started', None, self.schedule)
        self.registry.event_dispatcher.consume('do_schedule', None, self.schedule)
        # TODO: add events for resource configuration changes and subscribe them here.
        self.lock_manager = aioredlock.Aioredlock([
            {'host': str(self.config['redis']['addr'][0]),
             'port': self.config['redis']['addr'][1],
             'password': self.config['redis']['password'] if self.config['redis']['password'] else None,
             'db': REDIS_LIVE_DB},
        ])

    async def close(self) -> None:
        log.info('Session scheduler stopped')
        self.tick_task.cancel()
        await self.tick_task

    async def generate_scheduling_tick(self) -> None:
        """
        Periodically generate a scheduling event, considering other manager worker instances,
        using a distributed spread-interval clock.  All workers get a chance to dispatch
        the scheduler once in a minute.  Increasing the number of workers shortens the interval
        between each scheduler dispatch.

        This function relies on the wall clock of the Redis server, and is not affected
        by clock skews of the manager instances using differences of monotonic clocks.
        """
        instance_id = await get_instance_id()
        scheduler_id = f"{instance_id}.{self.pidx}"
        base_time = await redis.execute_with_retries(lambda: self.registry.redis_live.time())
        base_mono = time.monotonic()
        epoch_length = 60
        last_sync_time = base_time
        try:
            while True:
                await asyncio.sleep(2)
                now = base_time + (time.monotonic() - base_mono)
                local_sync_delay, next_sync_time = await redis.execute_script(
                    self.registry.redis_live, 'scheduler_tick', self.tick_script,
                    ['_last_scheduler_sync', '_schedulers'],
                    [scheduler_id, str(epoch_length)],
                )
                if now > next_sync_time + local_sync_delay and last_sync_time != next_sync_time:
                    last_sync_time = next_sync_time
                    await self.registry.event_dispatcher.produce_event('do_schedule')
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception('scheduling-tick: unexpected error')

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
        known_slot_types = await self.config_server.get_resource_slots()
        sched_ctx = SchedulingContext(
            registry=self.registry,
            known_slot_types=known_slot_types,
        )

        log_fmt = 'schedule(k:{}, s:{}, ak:{}): '
        start_task_args: List[Tuple[
            Sequence[Any],
            SchedulingContext,
            PendingSession,
            AgentAllocationContext,
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

                picked_kernel_id = scheduler.pick_session(
                    total_capacity,
                    pending_sessions,
                    existing_sessions,
                )
                if picked_kernel_id is None:
                    # no session is picked.
                    # continue to next sgroup.
                    return
                for picked_idx, sess_ctx in enumerate(pending_sessions):
                    if sess_ctx.kernel_id == picked_kernel_id:
                        break
                else:
                    # no matching entry for picked session?
                    raise RuntimeError('should not reach here')
                sess_ctx = pending_sessions.pop(picked_idx)

                log_args = (sess_ctx.kernel_id, sess_ctx.session_name, sess_ctx.access_key)
                log.debug(log_fmt + 'try-scheduling', *log_args)

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

                    try:
                        agent_id = scheduler.assign_agent(candidate_agents, sess_ctx)
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
                        continue
                    except Exception:
                        log.exception(log_fmt + 'unexpected-error, during agent allocation',
                                      *log_args)
                        await _invoke_failure_callbacks(
                            db_conn, sched_ctx, sess_ctx, check_results,
                        )
                        continue

                    query = kernels.update().values({
                        'agent': agent_alloc_ctx.agent_id,
                        'agent_addr': agent_alloc_ctx.agent_addr,
                        'scaling_group': sgroup_name,
                        'status': KernelStatus.PREPARING,
                        'status_info': 'scheduled',
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == sess_ctx.kernel_id)
                    await db_conn.execute(query)
                    start_task_args.append(
                        (
                            log_args, sched_ctx,
                            sess_ctx,
                            agent_alloc_ctx,
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

        async def start_session(log_args, sched_ctx, sess_ctx, agent_alloc_ctx, check_results):
            log.debug(log_fmt + 'try-starting', *log_args)
            try:
                await self.registry.start_session(sched_ctx, sess_ctx, agent_alloc_ctx)
            except Exception as e:
                log.error(log_fmt + 'failed-starting', *log_args, exc_info=e)
                async with self.dbpool.acquire(), db_conn.begin():
                    await _unreserve_agent_slots(db_conn, sess_ctx, agent_alloc_ctx)
                    await _invoke_failure_callbacks(db_conn, sched_ctx, sess_ctx, check_results)
                    query = kernels.update().values({
                        'status': KernelStatus.CANCELLED,
                        'status_info': 'failed-to-start',
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == sess_ctx.kernel_id)
                    await db_conn.execute(query)
                await self.registry.event_dispatcher.produce_event(
                    'kernel_cancelled',
                    (str(sess_ctx.kernel_id), 'failed-to-start'),
                )
            else:
                log.info(log_fmt + 'started', *log_args)
                async with self.dbpool.acquire(), db_conn.begin():
                    await _invoke_success_callbacks(db_conn, sched_ctx, sess_ctx, check_results)

        start_coros = []
        for log_args, sched_ctx, sess_ctx, agent_alloc_ctx, check_results in start_task_args:
            start_coros.append(
                start_session(log_args, sched_ctx, sess_ctx, agent_alloc_ctx, check_results)
            )
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
        return load_scheduler(scheduler_name, self.config['plugins']['scheduler'])


async def _list_pending_sessions(
    db_conn: SAConnection,
    sgroup_name: str,
) -> List[PendingSession]:
    query = (
        sa.select([
            kernels.c.id,
            kernels.c.status,
            kernels.c.image,
            kernels.c.registry,
            kernels.c.sess_type,
            kernels.c.sess_id,
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
    items = []
    async for row in db_conn.execute(query):
        items.append(PendingSession(
            kernel_id=row['id'],
            access_key=row['access_key'],
            session_type=row['sess_type'],
            session_name=row['sess_id'],
            domain_name=row['domain_name'],
            group_id=row['group_id'],
            scaling_group=row['scaling_group'],
            image_ref=ImageRef(row['image'], [row['registry']]),
            resource_policy=row['resource_policy'],
            resource_opts=row['resource_opts'],
            requested_slots=row['occupied_slots'],
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
        ))
    return items


async def _list_existing_sessions(
    db_conn: SAConnection,
    sgroup: str,
) -> List[ExistingSession]:
    query = (
        sa.select([
            kernels.c.id,
            kernels.c.status,
            kernels.c.image,
            kernels.c.registry,
            kernels.c.sess_type,
            kernels.c.sess_id,
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
    items = []
    async for row in db_conn.execute(query):
        items.append(ExistingSession(
            kernel_id=row['id'],
            access_key=row['access_key'],
            session_type=row['sess_type'],
            session_name=row['sess_id'],
            domain_name=row['domain_name'],
            group_id=row['group_id'],
            scaling_group=row['scaling_group'],
            image_ref=ImageRef(row['image'], [row['registry']]),
            occupying_slots=row['occupied_slots'],
        ))
    return items


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
) -> AgentAllocationContext:
    query = (
        sa.select([agents.c.occupied_slots], for_update=True)
        .select_from(agents)
        .where(agents.c.id == agent_id))
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
    sess_ctx: PendingSession,
    agent_ctx: AgentAllocationContext,
) -> None:
    # Un-reserve agent slots, using a separate db txn.
    query = (
        sa.select([agents.c.occupied_slots], for_update=True)
        .select_from(agents)
        .where(agents.c.id == agent_ctx.agent_id))
    current_occupied_slots = await db_conn.scalar(query)
    query = (
        sa.update(agents)
        .values({
            'occupied_slots': current_occupied_slots - sess_ctx.requested_slots
        })
        .where(agents.c.id == agent_ctx.agent_id))
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
