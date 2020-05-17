import asyncio
from datetime import datetime
from decimal import Decimal
import logging
import random
from typing import (
    Any, Union,
    Awaitable, Optional,
    Sequence, MutableSequence, List, Tuple,
    Mapping,
)
from typing_extensions import (
    Protocol,
)
import uuid

import attr
from dateutil.tz import tzutc
import sqlalchemy as sa
import aioredlock
from aiopg.sa.connection import SAConnection

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    aobject,
    AgentId, KernelId, SessionTypes,
    AccessKey,
    ResourceSlot, DefaultForUnspecified,
)

from .registry import AgentRegistry
from .models import (
    agents, domains, groups, kernels, keypairs,
    keypair_resource_policies,
    query_allowed_sgroups,
    AgentStatus, KernelStatus,
)
from ..gateway.defs import REDIS_LIVE_DB
from ..gateway.etcd import ConfigServer
from ..gateway.exceptions import (
    InstanceNotAvailable,
)
from ..gateway.utils import current_loop

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))


@attr.s(auto_attribs=True, slots=True)
class SchedulingContext:
    '''
    Context for each scheduling decision.
    '''
    registry: AgentRegistry
    db_conn: SAConnection
    known_slot_types: Mapping[str, str]


@attr.s(auto_attribs=True, slots=True)
class SessionContext:
    '''
    Context for individual session-related information used during scheduling.
    '''
    kernel_id: KernelId
    image_ref: ImageRef
    access_key: AccessKey
    sess_type: SessionTypes
    sess_id: str
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    resource_policy: dict
    resource_opts: Mapping[str, Any]
    requested_slots: ResourceSlot
    target_sgroup_names: MutableSequence[str]
    environ: Mapping[str, str]
    mounts: Sequence[str]
    startup_command: Optional[str]
    internal_data: Optional[Mapping[str, Any]]


@attr.s(auto_attribs=True, slots=True)
class AgentAllocationContext:
    agent_id: AgentId
    agent_addr: str
    scaling_group: str


class PredicateCallback(Protocol):
    async def __call__(self,
                       sched_ctx: SchedulingContext,
                       sess_ctx: SessionContext,
                       db_conn: SAConnection = None) -> None:
        ...


@attr.s(auto_attribs=True, slots=True)
class PredicateResult:
    passed: bool
    message: Optional[str] = None
    success_cb: Optional[PredicateCallback] = None
    failure_cb: Optional[PredicateCallback] = None


class SchedulingPredicate(Protocol):
    async def __call__(self,
                       sched_ctx: SchedulingContext,
                       sess_ctx: SessionContext) \
                       -> PredicateResult:
        ...


async def check_concurrency(sched_ctx: SchedulingContext,
                            sess_ctx: SessionContext) -> PredicateResult:
    query = (
        sa.select([keypair_resource_policies])
        .select_from(keypair_resource_policies)
        .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
    )
    result = await sched_ctx.db_conn.execute(query)
    resource_policy = await result.first()
    query = (sa.select([keypairs.c.concurrency_used], for_update=True)
               .select_from(keypairs)
               .where(keypairs.c.access_key == sess_ctx.access_key))
    concurrency_used = await sched_ctx.db_conn.scalar(query)
    log.debug('access_key: {0} ({1} / {2})',
              sess_ctx.access_key, concurrency_used,
              resource_policy['max_concurrent_sessions'])
    if concurrency_used >= resource_policy['max_concurrent_sessions']:
        return PredicateResult(
            False,
            "You cannot run more than "
            f"{resource_policy['max_concurrent_sessions']} concurrent sessions"
        )
    # Increment concurrency usage of keypair.
    query = (sa.update(keypairs)
               .values(concurrency_used=keypairs.c.concurrency_used + 1)
               .where(keypairs.c.access_key == sess_ctx.access_key))
    await sched_ctx.db_conn.execute(query)

    async def rollback(sched_ctx: SchedulingContext,
                       sess_ctx: SessionContext,
                       db_conn: SAConnection = None) -> None:
        query = (sa.update(keypairs)
                   .values(concurrency_used=keypairs.c.concurrency_used - 1)
                   .where(keypairs.c.access_key == sess_ctx.access_key))
        if db_conn is not None:
            await db_conn.execute(query)
        else:
            await sched_ctx.db_conn.execute(query)

    return PredicateResult(True, failure_cb=rollback)


async def check_dependencies(sched_ctx: SchedulingContext,
                             sess_ctx: SessionContext) -> PredicateResult:
    # TODO: implement
    return PredicateResult(True, 'bypassing because it is not implemented')


async def check_keypair_resource_limit(sched_ctx: SchedulingContext,
                                       sess_ctx: SessionContext) -> PredicateResult:
    query = (
        sa.select([keypair_resource_policies])
        .select_from(keypair_resource_policies)
        .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
    )
    result = await sched_ctx.db_conn.execute(query)
    resource_policy = await result.first()
    total_keypair_allowed = ResourceSlot.from_policy(resource_policy,
                                                     sched_ctx.known_slot_types)
    key_occupied = await sched_ctx.registry.get_keypair_occupancy(
        sess_ctx.access_key, conn=sched_ctx.db_conn)
    log.debug('keypair:{} current-occupancy: {}', sess_ctx.access_key, key_occupied)
    log.debug('keypair:{} total-allowed: {}', sess_ctx.access_key, total_keypair_allowed)
    if not (key_occupied + sess_ctx.requested_slots <= total_keypair_allowed):

        async def update_status_info(sched_ctx: SchedulingContext,
                                     sess_ctx: SessionContext,
                                     db_conn: SAConnection = None) -> None:
            query = (sa.update(kernels)
                       .values(status_info='out-of-resource (keypair resource quota exceeded)')
                       .where(kernels.c.id == sess_ctx.kernel_id))
            if db_conn is not None:
                await db_conn.execute(query)
            else:
                await sched_ctx.db_conn.execute(query)

        return PredicateResult(
            False,
            'Your keypair resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_keypair_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )),
            failure_cb=update_status_info)
    return PredicateResult(True)


async def check_group_resource_limit(sched_ctx: SchedulingContext,
                                     sess_ctx: SessionContext) -> PredicateResult:
    query = (sa.select([groups.c.total_resource_slots])
               .where(groups.c.id == sess_ctx.group_id))
    group_resource_slots = await sched_ctx.db_conn.scalar(query)
    group_resource_policy = {'total_resource_slots': group_resource_slots,
                             'default_for_unspecified': DefaultForUnspecified.UNLIMITED}
    total_group_allowed = ResourceSlot.from_policy(group_resource_policy,
                                                   sched_ctx.known_slot_types)
    group_occupied = await sched_ctx.registry.get_group_occupancy(
        sess_ctx.group_id, conn=sched_ctx.db_conn)
    log.debug('group:{} current-occupancy: {}', sess_ctx.group_id, group_occupied)
    log.debug('group:{} total-allowed: {}', sess_ctx.group_id, total_group_allowed)
    if not (group_occupied + sess_ctx.requested_slots <= total_group_allowed):

        async def update_status_info(sched_ctx: SchedulingContext,
                                     sess_ctx: SessionContext,
                                     db_conn: SAConnection = None) -> None:
            query = (sa.update(kernels)
                       .values(status_info='out-of-resource (group resource quota exceeded)')
                       .where(kernels.c.id == sess_ctx.kernel_id))
            if db_conn is not None:
                await db_conn.execute(query)
            else:
                await sched_ctx.db_conn.execute(query)

        return PredicateResult(
            False,
            'Your group resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_group_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )),
            failure_cb=update_status_info)
    return PredicateResult(True)


async def check_domain_resource_limit(sched_ctx: SchedulingContext,
                                      sess_ctx: SessionContext) -> PredicateResult:
    query = (sa.select([domains.c.total_resource_slots])
               .where(domains.c.name == sess_ctx.domain_name))
    domain_resource_slots = await sched_ctx.db_conn.scalar(query)
    domain_resource_policy = {
        'total_resource_slots': domain_resource_slots,
        'default_for_unspecified': DefaultForUnspecified.UNLIMITED
    }
    total_domain_allowed = ResourceSlot.from_policy(domain_resource_policy,
                                                    sched_ctx.known_slot_types)
    domain_occupied = await sched_ctx.registry.get_domain_occupancy(
        sess_ctx.domain_name, conn=sched_ctx.db_conn)
    log.debug('domain:{} current-occupancy: {}', sess_ctx.domain_name, domain_occupied)
    log.debug('domain:{} total-allowed: {}', sess_ctx.domain_name, total_domain_allowed)
    if not (domain_occupied + sess_ctx.requested_slots <= total_domain_allowed):

        async def update_status_info(sched_ctx: SchedulingContext,
                                     sess_ctx: SessionContext,
                                     db_conn: SAConnection = None) -> None:
            query = (sa.update(kernels)
                       .values(status_info='out-of-resource (domain resource quota exceeded)')
                       .where(kernels.c.id == sess_ctx.kernel_id))
            if db_conn is not None:
                await db_conn.execute(query)
            else:
                await sched_ctx.db_conn.execute(query)

        return PredicateResult(
            False,
            'Your domain resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_domain_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )),
            failure_cb=update_status_info)
    return PredicateResult(True)


async def check_scaling_group(sched_ctx: SchedulingContext,
                              sess_ctx: SessionContext) -> PredicateResult:
    sgroups = await query_allowed_sgroups(
        sched_ctx.db_conn,
        sess_ctx.domain_name,
        sess_ctx.group_id,
        sess_ctx.access_key)
    target_sgroup_names: List[str] = []
    preferred_sgroup_name = sess_ctx.scaling_group
    if preferred_sgroup_name is not None:
        for sgroup in sgroups:
            if preferred_sgroup_name == sgroup['name']:
                break
        else:
            return PredicateResult(
                False,
                'The given preferred scaling group is not available. ({})'
                .format(preferred_sgroup_name)
            )
        # Consider agents only in the preferred scaling group.
        target_sgroup_names = [preferred_sgroup_name]
    else:
        # Consider all agents in all allowed scaling groups.
        target_sgroup_names = [sgroup['name'] for sgroup in sgroups]
    log.debug('considered scaling groups: {}', target_sgroup_names)
    if not target_sgroup_names:

        async def update_status_info(sched_ctx: SchedulingContext,
                                     sess_ctx: SessionContext,
                                     db_conn: SAConnection = None) -> None:
            query = (sa.update(kernels)
                       .values(status_info='out-of-resource (no available resource in scaling groups)')
                       .where(kernels.c.id == sess_ctx.kernel_id))
            if db_conn is not None:
                await db_conn.execute(query)
            else:
                await sched_ctx.db_conn.execute(query)

        return PredicateResult(False, 'No available resource in scaling groups.',
                               failure_cb=update_status_info)
    sess_ctx.target_sgroup_names.extend(target_sgroup_names)
    return PredicateResult(True)


def key_by_requested_slots(
    agent_slots: ResourceSlot,
    requested_slots: ResourceSlot,
) -> Tuple[int, ResourceSlot]:
    unused_slot_keys = set()
    for k, v in requested_slots.items():
        if v == Decimal(0):
            unused_slot_keys.add(k)
    num_extras = 0
    for k, v in agent_slots.items():
        if k in unused_slot_keys and v > Decimal(0):
            num_extras += 1
    # Put back agents with more extra slot types
    # (e.g., accelerators)
    # Also put front agents with exactly required slot types
    return (-num_extras, agent_slots)


class SessionScheduler(aobject):

    config_server: ConfigServer
    registry: AgentRegistry

    def __init__(self, config: dict, config_server: ConfigServer, registry: AgentRegistry) -> None:
        self.config = config
        self.config_server = config_server
        self.registry = registry
        self.dbpool = registry.dbpool

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        loop = current_loop()
        self.tick_task = loop.create_task(self.generate_scheduling_tick())
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
        try:
            lock = await self.lock_manager.lock('manager.scheduler')
            async with lock:
                await asyncio.sleep(0.5)
                await self.schedule_impl()
        except aioredlock.LockError:
            log.debug('schedule(): temporary locking failure; retrying at the next time')

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
        start_task_args = []

        async with self.dbpool.acquire() as db_conn:
            sched_ctx = SchedulingContext(
                registry=self.registry,
                db_conn=db_conn,
                known_slot_types=known_slot_types,
            )

            # First, fetch all pending sessions.
            # For each pending session, check the followings:
            # - all dependent jobs has finished (status=TERMINATED, result=SUCCESS).
            # - target scaling group's resource capacity is sufficient to run the session.
            async with db_conn.begin():
                sess_ctxs = await self._list_pending_sessions(db_conn)
            for sess_ctx in sess_ctxs:
                log_fmt = 'schedule(k:{}, s:{}, ak:{}): '
                log_args = (sess_ctx.kernel_id, sess_ctx.sess_id, sess_ctx.access_key)
                log.debug(log_fmt + 'try-scheduling', *log_args)

                async with db_conn.begin():
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

                    # TODO: allow prioritization of ready-to-start sessions
                    #       using custom algorithms (e.g., DRF)

                    try:
                        agent_ctx = await self._find_and_reserve_agent(sched_ctx, sess_ctx)
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
                    start_task_args.append(
                        (log_args, sched_ctx, sess_ctx, agent_ctx, check_results),
                    )

        # Perform session-start trials after finishing the DB transaction.
        async def _cb(fut_exception, log_args, sess_ctx, agent_ctx, check_results) -> None:
            if fut_exception is not None:
                log.error(log_fmt + 'failed-starting',
                          *log_args, exc_info=fut_exception)
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

        for log_args, sched_ctx, sess_ctx, agent_ctx, check_results in start_task_args:
            log.debug(log_fmt + 'try-starting', *log_args)
            try:
                await self.registry.start_session(sched_ctx, sess_ctx, agent_ctx)
            except Exception as e:
                await _cb(e, log_args, sess_ctx, agent_ctx, check_results)
            else:
                await _cb(None, log_args, sess_ctx, agent_ctx, check_results)

    async def _list_pending_sessions(self, db_conn) -> Sequence[SessionContext]:
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
                kernels.c.startup_command,
                kernels.c.internal_data,
                keypairs.c.resource_policy,
            ])
            .select_from(sa.join(
                kernels, keypairs,
                keypairs.c.access_key == kernels.c.access_key
            ))
            .where(kernels.c.status == KernelStatus.PENDING)
            .order_by(kernels.c.created_at)
        )
        sess_ctxs = []
        async for row in db_conn.execute(query):
            sess_ctxs.append(SessionContext(
                kernel_id=row['id'],
                image_ref=ImageRef(row['image'], [row['registry']]),
                access_key=row['access_key'],
                sess_type=row['sess_type'],
                sess_id=row['sess_id'],
                domain_name=row['domain_name'],
                group_id=row['group_id'],
                scaling_group=row['scaling_group'],
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
                startup_command=row['startup_command'],
            ))
        return sess_ctxs

    async def _find_and_reserve_agent(self, sched_ctx: SchedulingContext,
                                      sess_ctx: SessionContext) \
                                      -> AgentAllocationContext:
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
            log.debug('{} ({}) capacity: {!r}', row['id'], row['scaling_group'], capacity_slots)
            log.debug('{} ({}) occupied: {!r}', row['id'], row['scaling_group'], occupied_slots)
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
                max(possible_agent_slots,
                    key=lambda ag_slots: key_by_requested_slots(ag_slots[1],
                                                                sess_ctx.requested_slots))
        else:
            raise InstanceNotAvailable

        # Reserve agent slots
        query = (sa.update(agents)
                   .values({
                       'occupied_slots': current_occupied_slots + sess_ctx.requested_slots
                   })
                   .where(agents.c.id == agent_id))
        await sched_ctx.db_conn.execute(query)

        # Get the agent address for later RPC calls
        query = (sa.select([agents.c.addr, agents.c.scaling_group])
                   .where(agents.c.id == agent_id))
        result = await sched_ctx.db_conn.execute(query)
        row = await result.first()
        agent_addr = row['addr']
        agent_sgroup = row['scaling_group']
        assert agent_addr is not None

        return AgentAllocationContext(agent_id, agent_addr, agent_sgroup)

    async def _unreserve_agent_slots(self,
                                     sess_ctx: SessionContext,
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
