from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime
import logging
import pkg_resources
import random
from typing import (
    Any, Union,
    Awaitable,
    List, Sequence,
    Optional,
    Dict, Mapping, MutableMapping
)

from aiopg.sa.connection import SAConnection
import aioredlock
from dateutil.tz import tzutc
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    aobject,
    AgentId,
    ResourceSlot,
)
from ...gateway.defs import REDIS_LIVE_DB
from ...gateway.etcd import ConfigServer
from ...gateway.exceptions import InstanceNotAvailable
from ..registry import AgentRegistry
from ..models import (
    agents, kernels, keypairs, scaling_groups,
    AgentStatus, KernelStatus,
    RESOURCE_OCCUPYING_KERNEL_STATUSES,
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
    KernelAgentBinding
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


def load_scheduler(name: str, scheduler_configs: Mapping[str, Any]) -> AbstractScheduler:
    entry_prefix = 'backendai_scheduler_v10'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        if entrypoint.name == name:
            log.info('loading scheduler plugin "{}" from {}', name, entrypoint.module_name)
            scheduler_cls = entrypoint.load()
            scheduler_config = scheduler_configs.get(name, {})
            return scheduler_cls(scheduler_config)
    raise ImportError('Cannot load the scheduler plugin', name)


def merge_resource(src: MutableMapping[str, Any], val: MutableMapping[str, Any]):
    for k in val.keys():
        if k in src.keys():
            src[k] += val[k]
        else:
            src[k] = val[k]


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
                sess_ctx: PendingSession,
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
                sess_ctx: PendingSession,
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

        async def _schedule_in_sgroup(db_conn, sgroup_name):
            query = (
                sa.select([scaling_groups.c.scheduler])
                .select_from(scaling_groups)
                .where(scaling_groups.c.name == sgroup_name)
            )
            result = await db_conn.execute(query)
            scheduler_name = await result.scalar()
            scheduler = load_scheduler(scheduler_name, self.config['plugins']['scheduler'])
            candidate_agents = agents_by_sgroups[sgroup_name]
            pending_sessions = await self._list_pending_sessions(db_conn, sgroup_name)
            existing_sessions = await self._list_existing_sessions(db_conn, sgroup_name)
            log.debug('running scheduler (sgroup:{}, pending:{}, existing:{})',
                      sgroup_name, len(pending_sessions), len(existing_sessions))
            zero = ResourceSlot()
            total_capacity = sum(
                (ag.available_slots for ag in candidate_agents),
                zero)
            while len(pending_sessions) > 0:
                picked_sess_id = scheduler.pick_session(
                    total_capacity,
                    pending_sessions,
                    existing_sessions,
                )
                if picked_sess_id is None:
                    # no session is picked.
                    # continue to next sgroup.
                    return
                for picked_idx, sess_ctx in enumerate(pending_sessions):
                    if sess_ctx.sess_id == picked_sess_id:
                        break
                else:
                    # no matching entry for picked session?
                    raise RuntimeError('should not reach here')
                sess_ctx = pending_sessions.pop(picked_idx)
                log_fmt = 'schedule(k:{}, s:{}, ak:{}): '
                log_args = (', '.join(sess_ctx.kernels), sess_ctx.sess_id, sess_ctx.access_key)
                log.debug(log_fmt + 'try-scheduling', *log_args)
                loaded = []

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
                    await _invoke_failure_callbacks(sess_ctx, check_results)
                    # Predicate failures are *NOT* permanent errors.
                    # We need to retry the scheduling afterwards.
                    continue
                
                agent_query_extra_conds = None
                if len(sess_ctx.kernels) >= 2:
                    agent_query_extra_conds = (agents.c.clusterized)
                for kernel in sess_ctx.kernels:
                    try:
                        agent_id = scheduler.assign_agent(candidate_agents, sess_ctx)
                        if agent_id is None:
                            raise InstanceNotAvailable
                        agent_alloc_ctx = await self._reserve_agent(
                            sched_ctx, sgroup_name, agent_id, kernel.requested_slots,
                            extra_conds=agent_query_extra_conds)
                    except InstanceNotAvailable:
                        log.debug(log_fmt + 'no-available-instances', *log_args)
                        await _invoke_failure_callbacks(sess_ctx, check_results)
                        break
                    except Exception:
                        log.exception(log_fmt + 'unexpected-error, during agent allocation',
                                      *log_args)
                        await _invoke_failure_callbacks(sess_ctx, check_results)
                        break

                    query = kernels.update().values({
                        'agent': agent_alloc_ctx.agent_id,
                        'agent_addr': agent_alloc_ctx.agent_addr,
                        'scaling_group': sgroup_name,
                        'status': KernelStatus.PREPARING,
                        'status_info': 'scheduled',
                        'status_changed': datetime.now(tzutc()),
                    }).where(kernels.c.id == kernel.kernel_id)
                    await db_conn.execute(query)

                    log.debug(log_fmt + 'try-starting', *log_args)
                    loaded.append([kernel, agent_alloc_ctx])

                if len(sess_ctx.kernels) == len(loaded):
                    task = asyncio.create_task(
                        self.registry.start_session(sched_ctx, sess_ctx, loaded))

                    async def _cb(fut):
                        if fut.exception():
                            log.error(log_fmt + 'failed-starting',
                                        *log_args, exc_info=fut.exception())
                            for loaded_kernel in loaded:
                                await self._unreserve_agent_slots(sess_ctx, loaded_kernel[0],
                                                                loaded_kernel[1])
                            await _invoke_failure_callbacks(sess_ctx, check_results, use_new_txn=True)
                            async with self.dbpool.acquire() as conn, conn.begin():
                                query = kernels.update().values({
                                    'status': KernelStatus.CANCELLED,
                                    'status_info': 'failed-to-start',
                                    'status_changed': datetime.now(tzutc()),
                                }).where(kernels.c.sess_id == sess_ctx.sess_id)
                                await conn.execute(query)
                            await self.registry.event_dispatcher.produce_event(
                                'kernel_cancelled',
                                (str(sess_ctx.sess_id), 'failed-to-start'),
                            )

                        else:
                            log.info(log_fmt + 'started', *log_args)
                            await _invoke_success_callbacks(sess_ctx, check_results, use_new_txn=True)

                    task.add_done_callback(lambda fut: asyncio.create_task(_cb(fut)))
                else:
                    for loaded_kernel in loaded:
                        await self._unreserve_agent_slots(sess_ctx, loaded_kernel[0],
                                                          loaded_kernel[1])
                        async with self.dbpool.acquire() as conn, conn.begin():
                            query = kernels.update().values({
                                'status': KernelStatus.CANCELLED,
                                'status_info': 'failed-to-start',
                                'status_changed': datetime.now(tzutc()),
                            }).where(kernels.c.sess_id == sess_ctx.sess_id)
                            await conn.execute(query)

        # We allow a long database transaction here
        # because this scheduling handler will be executed by only one process.
        # It is executed under a globally exclusive context using aioredlock.
        async with self.dbpool.acquire() as db_conn:
            sched_ctx = SchedulingContext(
                registry=self.registry,
                db_conn=db_conn,
                known_slot_types=known_slot_types,
            )
            agents_by_sgroups = await self._list_agents_by_sgroups(db_conn)
            all_scaling_groups = [*agents_by_sgroups.keys()]
            for sgroup_name in all_scaling_groups:
                async with db_conn.begin():
                    await _schedule_in_sgroup(db_conn, sgroup_name)

    async def _list_pending_sessions(self, db_conn, sgroup_name) -> List[PendingSession]:
        query = (
            sa.select([
                kernels.c.id,
                kernels.c.status,
                kernels.c.image,
                kernels.c.role,
                kernels.c.idx,
                kernels.c.registry,
                kernels.c.sess_type,
                kernels.c.sess_id,
                kernels.c.sess_uuid,
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
                keypairs.c.resource_policy,
            ], for_update=True)
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
        items: MutableMapping[str, PendingSession] = {}
        async for row in db_conn.execute(query):
            if _session := items.get(row['sess_id']):
                session = _session
            else:
                session = PendingSession(
                    kernels=[],
                    access_key=row['access_key'],
                    sess_type=row['sess_type'],
                    sess_id=row['sess_id'],
                    sess_uuid=row['sess_uuid'],
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
                )
                items[row['sess_id']] = session
            # TODO: Remove `type: ignore` when mypy supports type inference for walrus operator
            # Check https://github.com/python/mypy/issues/7316
            session.kernels.append(KernelInfo(  # type: ignore
                kernel_id=row['id'],
                role=row['role'],
                idx=row['idx'],
                image_ref=ImageRef(row['image'], [row['registry']]),
                bootstrap_script=row['bootstrap_script'],
                startup_command=row['startup_command'],
                resource_opts=row['resource_opts'],
                requested_slots=row['occupied_slots'],
            ))
            session.requested_slots += row['occupied_slots']  # type: ignore
            merge_resource(session.resource_opts, row['resource_opts'])  # type: ignore

        return list(items.values())

    async def _list_existing_sessions(self, db_conn, sgroup) -> List[ExistingSession]:
        query = (
            sa.select([
                kernels.c.id,
                kernels.c.status,
                kernels.c.image,
                kernels.c.registry,
                kernels.c.sess_type,
                kernels.c.sess_id,
                kernels.c.sess_uuid,
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
        items: MutableMapping[str, ExistingSession] = {}
        async for row in db_conn.execute(query):
            if _session := items.get(row['sess_id']):
                session = _session
            else:
                session = ExistingSession(
                    kernels=[],
                    access_key=row['access_key'],
                    sess_type=row['sess_type'],
                    sess_id=row['sess_id'],
                    sess_uuid=row['sess_uuid'],
                    domain_name=row['domain_name'],
                    group_id=row['group_id'],
                    scaling_group=row['scaling_group'],
                    occupying_slots=row['occupied_slots'],
                )
                items[row['sess_id']] = session
            session.kernels.append(KernelInfo(  # type: ignore
                kernel_id=row['id'],
                role=row['role'],
                image_ref=ImageRef(row['image'], [row['registry']]),
                resource_opts={},
                requested_slots=ResourceSlot(),
                bootstrap_script='',
                startup_command=''
            ))

        return list(items.values())

    async def _list_agents_by_sgroups(self, db_conn: SAConnection) \
            -> Mapping[str, Sequence[AgentContext]]:
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
                agents.c.status == AgentStatus.ALIVE
            )
        )
        items_by_sgroup: Dict[str, List[AgentContext]] = defaultdict(list)
        async for row in db_conn.execute(query):
            item = AgentContext(
                row['id'],
                row['addr'],
                row['scaling_group'],
                row['available_slots'],
                row['occupied_slots'],
            )
            items_by_sgroup[row['scaling_group']].append(item)
        return dict(items_by_sgroup)

    async def _reserve_agent(self, sched_ctx: SchedulingContext,
                             scaling_group: str,
                             agent_id: AgentId,
                             requested_slots: ResourceSlot,
                             extra_conds: Optional[Any]) \
                             -> AgentAllocationContext:
        query = (
            sa.select([agents.c.occupied_slots], for_update=True)
            .select_from(agents)
            .where(agents.c.id == agent_id))
        if extra_conds is not None:
            query = query.where(extra_conds)
        current_occupied_slots = await sched_ctx.db_conn.scalar(query)
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

        return AgentAllocationContext(agent_id, agent_addr, scaling_group)

    async def _unreserve_agent_slots(self,
                                     sess_ctx: PendingSession,
                                     kernel_ctx: KernelInfo,
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
                    'occupied_slots': current_occupied_slots - kernel_ctx.requested_slots
                })
                .where(agents.c.id == agent_ctx.agent_id))
            await conn.execute(query)
