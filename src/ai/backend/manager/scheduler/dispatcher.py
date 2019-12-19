from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import random
from typing import (
    Union,
    Awaitable,
    Type,
    List, Sequence,
)

from dateutil.tz import tzutc
import sqlalchemy as sa
import aioredlock

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    aobject,
    AgentId,
    ResourceSlot,
)
from ..gateway.defs import REDIS_LIVE_DB
from ..gateway.etcd import ConfigServer
from ..gateway.exceptions import InstanceNotAvailable
from ..registry import AgentRegistry
from ..manager.models import (
    agents, kernels, keypairs,
    AgentStatus, KernelStatus,
    RESOURCE_OCCUPYING_KERNEL_STATUSES,
)
from . import (
    PredicateResult,
    PendingSession,
    ExistingSession,
    SchedulingContext,
)
from .predicates import (
    check_concurrency,
    check_dependencies,
    check_keypair_resource_limit,
    check_group_resource_limit,
    check_domain_resource_limit,
    check_scaling_group,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))


class SchedulerDispatcher(aobject):

    config_server: ConfigServer
    registry: AgentRegistry

    def __init__(self, config: dict, config_server: ConfigServer, registry: AgentRegistry) -> None:
        self.config = config
        self.config_server = config_server
        self.registry = registry
        self.dbpool = registry.dbpool

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        self.tick_task = asyncio.create_task(self.generate_scheduling_tick())
        self.registry.event_dispatcher.consume('kernel_enqueued', None, self.schedule)
        self.registry.event_dispatcher.consume('kernel_terminated', None, self.schedule)
        self.registry.event_dispatcher.consume('instance_started', None, self.schedule)
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
        # A fallback for when missing enqueue events
        try:
            await asyncio.sleep(30 * random.uniform(1, 4))
            while True:
                await asyncio.sleep(30 * 2.5)
                await self.registry.event_dispatcher.produce_event(
                    'kernel_enqueued', [None])
        except asyncio.CancelledError:
            pass

    async def schedule(self, ctx: object, agent_id: AgentId, event_name: str,
                       *args, **kwargs) -> None:
        lock = await self.lock_manager.lock('manager.scheduler')
        try:
            async with lock:
                await asyncio.sleep(0.5)
                await self.schedule_impl()
        except aioredlock.LockError as e:
            log.debug('schedule(): temporary locking failure', exc_info=e)
            # The dispatcher will try the next chance.

    async def schedule_impl(self) -> None:
        log.debug('schedule(): triggered')
        known_slot_types = await self.config_server.get_resource_slots()

        async def _invoke_success_callbacks(
                results: List[Union[Exception, PredicateResult]], *,
                use_new_txn: bool = False) -> None:
            conn = None

            async def _inner() -> None:
                nonlocal results, conn
                callbacks: List[Awaitable[None]] = []
                for result in results:
                    if isinstance(result, Exception):
                        # This won't happen but this code is required to pass static check.
                        continue
                    if result.success_cb is not None:
                        callbacks.append(result.success_cb(sched_ctx, sess_ctx, conn))
                for cb in reversed(callbacks):
                    await cb

            if use_new_txn:
                async with self.dbpool.acquire() as conn, conn.begin():
                    await _inner()
            else:
                conn = None
                await _inner()

        async def _invoke_failure_callbacks(
                results: List[Union[Exception, PredicateResult]], *,
                use_new_txn: bool = False) -> None:
            conn = None

            async def _inner() -> None:
                nonlocal results, conn
                callbacks: List[Awaitable[None]] = []
                for result in results:
                    if isinstance(result, Exception):
                        # This won't happen but this code is required to pass static check.
                        continue
                    if result.failure_cb:
                        callbacks.append(result.failure_cb(sched_ctx, sess_ctx, conn))
                for cb in reversed(callbacks):
                    await cb

            # Rollback any changes performed by predicates
            # (NOTE: We don't use the DB-level transaction rollback because we need to
            #  store the "ERROR" status to corresponding rows in the kernels table.)
            if use_new_txn:
                async with self.dbpool.acquire() as conn, conn.begin():
                    await _inner()
            else:
                await _inner()

        # We allow a long database transaction here
        # because this scheduling handler will be executed by only one process.
        # (It's a globally unique singleton.)
        async with self.dbpool.acquire() as db_conn, db_conn.begin():
            sched_ctx = SchedulingContext(
                registry=self.registry,
                db_conn=db_conn,
                known_slot_types=known_slot_types,
            )
            from .fifo import FIFOSlotScheduler
            scheduler = FIFOSlotScheduler()
            zero = ResourceSlot()
            used_scaling_groups = ...  # TODO: implement
            agents_by_sgroups = await self._list_agents_by_sgroups(db_conn)

            for sgroup_id in used_scaling_groups:
                candidate_agents = agents_by_sgroups[sgroup_id]
                pending_sessions = await self._list_pending_sessions(db_conn, sgroup_id)
                existing_sessions = await self._list_existing_sessions(db_conn, sgroup_id)
                total_capacity = sum(
                    (ag.available_slots for ag in candidate_agents),
                    zero)
                while len(pending_sessions) > 0:
                    picked_kernel_id = scheduler.pick_session(
                        total_capacity,
                        pending_sessions,
                        existing_sessions,
                    )
                    if picked_kernel_id is None:
                        # no session is picked.
                        return  # TODO: continue to next sgroup
                    for picked_idx, sess_ctx in enumerate(pending_sessions):
                        if sess_ctx.kernel_id == picked_kernel_id:
                            break
                    else:
                        # no matching entry for picked session?
                        raise RuntimeError('should not reach here')
                    sess_ctx = pending_sessions.pop(picked_idx)

                    log_fmt = 'schedule(k:{}, s:{}, ak:{}): '
                    log_args = (sess_ctx.kernel_id, sess_ctx.sess_id, sess_ctx.access_key)
                    log.debug(log_fmt + 'try-scheduling', *log_args)

                    predicates: Sequence[Awaitable[PredicateResult]] = [
                        check_concurrency(sched_ctx, sess_ctx),
                        check_dependencies(sched_ctx, sess_ctx),
                        check_keypair_resource_limit(sched_ctx, sess_ctx),
                        check_group_resource_limit(sched_ctx, sess_ctx),
                        check_domain_resource_limit(sched_ctx, sess_ctx),
                        check_scaling_group(sched_ctx, sess_ctx),
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
                        await _invoke_failure_callbacks(check_results)
                        # Predicate failures are *NOT* permanent errors.
                        # We need to retry the scheduling afterwards.
                        continue

                    try:
                        agent_id = scheduler.assign_agent(candidate_agents, sess_ctx)
                        agent_ctx = await self._reserve_agent(sched_ctx, agent_id)
                    except InstanceNotAvailable:
                        log.debug(log_fmt + 'no-available-instances', *log_args)
                        await _invoke_failure_callbacks(check_results)
                        continue
                    except Exception:
                        log.exception(log_fmt + 'unexpected-error, during agent allocation',
                                      *log_args)
                        await _invoke_failure_callbacks(check_results)
                        continue

                    query = kernels.update().values({
                        'agent': agent_ctx.agent_id,
                        'agent_addr': agent_ctx.agent_addr,
                        'scaling_group': agent_ctx.scaling_group,
                        'status': KernelStatus.PREPARING,
                        'status_info': 'scheduled',
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == sess_ctx.kernel_id)
                    await db_conn.execute(query)

                    log.debug(log_fmt + 'try-starting', *log_args)
                    task = asyncio.create_task(self.registry.start_session(sched_ctx, sess_ctx, agent_ctx))

                    async def _cb(fut):
                        if fut.exception():
                            log.error(log_fmt + 'failed-starting',
                                      *log_args, exc_info=fut.exception())
                            await self._unreserve_agent_slots(sess_ctx, agent_ctx)
                            await _invoke_failure_callbacks(check_results, use_new_txn=True)
                            async with self.dbpool.acquire() as conn, conn.begin():
                                query = kernels.update().values({
                                    'status': KernelStatus.CANCELLED,
                                    'status_info': 'failed-to-start',
                                    'status_changed': datetime.now(tzutc()),
                                }).where(kernels.c.id == sess_ctx.kernel_id)
                                await conn.execute(query)
                            await self.registry.event_dispatcher.produce_event(
                                'kernel_cancelled',
                                (str(sess_ctx.kernel_id), 'failed-to-start'),
                            )

                        else:
                            log.info(log_fmt + 'started', *log_args)
                            await _invoke_success_callbacks(check_results, use_new_txn=True)

                    task.add_done_callback(lambda fut: asyncio.create_task(_cb(fut)))

    async def _list_pending_sessions(self, db_conn, sgroup) -> List[PendingSession]:
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
            ], for_update=True)
            .select_from(sa.join(
                kernels, keypairs,
                keypairs.c.access_key == kernels.c.access_key
            ))
            .where(
                (kernels.c.status == KernelStatus.PENDING) &
                (kernels.c.scaling_group == sgroup)
            )
            .order_by(kernels.c.created_at)
        )
        items = []
        async for row in db_conn.execute(query):
            items.append(PendingSession(
                kernel_id=row['id'],
                access_key=row['access_key'],
                sess_type=row['sess_type'],
                sess_id=row['sess_id'],
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
                startup_command=row['startup_command'],
            ))
        return items

    async def _list_existing_sessions(self, db_conn, sgroup) -> List[ExistingSession]:
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
            ], for_update=True)
            .select_from(sa.join(
                kernels, keypairs,
                keypairs.c.access_key == kernels.c.access_key
            ))
            .where(
                (kernels.c.status.in_(RESOURCE_OCCUPYING_KERNEL_STATUSES)) &
                (kernels.c.scaling_group == sgroup)
            )
            .order_by(kernels.c.created_at)
        )
        items = []
        async for row in db_conn.execute(query):
            items.append(ExistingSession(
                kernel_id=row['id'],
                access_key=row['access_key'],
                sess_type=row['sess_type'],
                sess_id=row['sess_id'],
                domain_name=row['domain_name'],
                group_id=row['group_id'],
                scaling_group=row['scaling_group'],
                image_ref=ImageRef(row['image'], [row['registry']]),
                occupying_slots=row['occupied_slots'],
            ))
        return items

    async def _reserve_agent(self, sched_ctx: SchedulingContext,
                             agent_id: AgentId,
                             requested_slots: ResourceSlot) \
                             -> AgentAllocationContext:
        '''
        # Fetch all agent available slots and normalize them to "remaining" slots
        possible_agent_slots = []
        query = (
            sa.select([
                agents.c.id,
                agents.c.scaling_group,
                agents.c.available_slots,
                agents.c.occupied_slots,
            ], for_update=True)
            .where(
                (agents.c.status == AgentStatus.ALIVE) &
                (agents.c.scaling_group.in_(sess_ctx.target_sgroup_names))
            )
        )
        async for row in sched_ctx.db_conn.execute(query):
            capacity_slots = row['available_slots']
            occupied_slots = row['occupied_slots']
            log.debug('{} capacity: {!r}', row['id'], capacity_slots)
            log.debug('{} occupied: {!r}', row['id'], occupied_slots)
            try:
                remaining_slots = capacity_slots - occupied_slots

                # Check if: any(remaining >= requested)
                if remaining_slots >= sess_ctx.requested_slots:
                    possible_agent_slots.append((
                        row['id'],
                        remaining_slots,
                        occupied_slots))
            except ValueError:
                # happens when requested_slots have more keys
                # than the agent_slots
                # (e.g., agent does not have accelerators
                #  requested by the client)
                continue

        # Load-balance! (choose the agent with most remaining slots)
        # Here, all items in possible_agent_slots have the same keys,
        # allowing the total ordering property.
        if possible_agent_slots:
            agent_id, _, current_occupied_slots = \
                max(possible_agent_slots, key=lambda s: s[1])
        else:
            raise InstanceNotAvailable
        '''

        # Reserve agent slots
        query = (sa.update(agents)
                   .values({
                       'occupied_slots': current_occupied_slots + requested_slots
                   })
                   .where(agents.c.id == agent_id))
        await sched_ctx.db_conn.execute(query)

        # Get the agent address for later RPC calls
        query = (sa.select([agents.c.addr])
                   .where(agents.c.id == agent_id))
        agent_addr = await sched_ctx.db_conn.scalar(query)
        assert agent_addr is not None

        return AgentAllocationContext(agent_id, agent_addr, row['scaling_group'])

    async def _unreserve_agent_slots(self,
                                     sess_ctx: PendingSession,
                                     agent_ctx: AgentAllocationContext):
        # Un-reserve agent slots, using a separate db txn.
        async with self.dbpool.acquire() as conn, conn.begin():
            query = (
                sa.select([agents.c.occupied_slots], for_update=True)
                .select_from(agents)
                .where(agents.c.id == agent_ctx.agent_id))
            current_occupied_slots = await conn.scalar(query)
            query = (
                sa.update(agents)
                .values({
                    'occupied_slots': current_occupied_slots - sess_ctx.requested_slots
                })
                .where(agents.c.id == agent_ctx.agent_id))
            await conn.execute(query)
