import asyncio
from datetime import datetime
import logging
from typing import (
    Any, Union,
    Awaitable, Optional,
    Sequence, MutableSequence, List,
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


@attr.s(auto_attribs=True, slots=True)
class AgentAllocationContext:
    agent_id: AgentId
    agent_addr: str


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
        return PredicateResult(False, '')
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
        return PredicateResult(
            False,
            'Your keypair resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_keypair_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )))
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
        return PredicateResult(
            False,
            'Your group resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_group_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )))
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
        return PredicateResult(
            False,
            'Your domain resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_domain_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )))
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
        return PredicateResult(False, 'No available scaling groups.')
    sess_ctx.target_sgroup_names.extend(target_sgroup_names)
    return PredicateResult(True)


class SessionScheduler(aobject):

    config_server: ConfigServer
    registry: AgentRegistry

    def __init__(self, config: dict, config_server: ConfigServer, registry: AgentRegistry) -> None:
        self.config = config
        self.config_server = config_server
        self.registry = registry
        self.dbpool = registry.dbpool
        self.lock_manager = aioredlock.Aioredlock([
            {'host': str(config['redis']['addr'][0]),
             'port': config['redis']['addr'][1],
             'password': config['redis']['password'] if config['redis']['password'] else None,
             'db': REDIS_LIVE_DB},
        ])

    async def __ainit__(self) -> None:
        log.info('Session scheduler started')
        loop = current_loop()
        self.task = loop.create_task(self.scheduling_loop())

    async def close(self) -> None:
        log.info('Session scheduler stopped')
        self.task.cancel()
        await self.task
        await self.lock_manager.destroy()

    async def scheduling_loop(self) -> None:
        # TODO: change to event-driven invocation
        # TODO: adopt Redlock for distributed coordination?
        try:
            while True:
                try:
                    lock = await self.lock_manager.lock('manager.scheduler')
                    async with lock:
                        await asyncio.shield(self.schedule())
                        await asyncio.sleep(1)
                except aioredlock.LockError:
                    await asyncio.sleep(1)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception('unexpected error in scheduler loop')
        except asyncio.CancelledError:
            await asyncio.sleep(1)

    async def schedule(self) -> None:
        # NOTE: This schedule() function is never cancelled.

        if self.config['debug']['log-scheduler-ticks']:
            log.debug('schedule(): tick')
        known_slot_types = await self.config_server.get_resource_slots()

        async def _invoke_success_callbacks(results: List[Union[Exception, PredicateResult]], *,
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

        async def _invoke_failure_callbacks(results: List[Union[Exception, PredicateResult]], *,
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

            # First, fetch all pending sessions.
            # For each pending session, check the followings:
            # - all dependent jobs has finished (status=TERMINATED, result=SUCCESS).
            # - target scaling group's resource capacity is sufficient to run the session.
            for sess_ctx in (await self._list_pending_sessions(db_conn)):
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
                failure_callbacks: List[Awaitable[None]] = []
                for result in check_results:
                    if isinstance(result, Exception):
                        has_failure = True
                        continue
                    if not result.passed:
                        has_failure = True
                        if result.failure_cb is not None:
                            failure_callbacks.append(result.failure_cb(sched_ctx, sess_ctx))
                if has_failure:
                    log.debug(log_fmt + 'predicate-checks-failed', *log_args)
                    # If any one of predicates fails, rollback all changes.
                    for cb in reversed(failure_callbacks):
                        await cb
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
                    'status': KernelStatus.PREPARING,
                    'status_info': 'scheduled',
                    'status_changed': datetime.now(tzutc()),
                }).where(kernels.c.id == sess_ctx.kernel_id)
                await db_conn.execute(query)

                log.debug(log_fmt + 'try-starting', *log_args)
                loop = current_loop()
                task = loop.create_task(self.registry.start_session(sched_ctx, sess_ctx, agent_ctx))

                async def _cb(fut):
                    if fut.exception():
                        log.exception(log_fmt + 'failed-starting-preparation', *log_args)
                        await self._unreserve_agent_slots(sess_ctx, agent_ctx)
                        await _invoke_failure_callbacks(check_results, use_new_txn=True)
                        query = kernels.update().values({
                            'status': KernelStatus.ERROR,
                            'status_info': 'failed to start',
                            'status_changed': datetime.now(tzutc()),
                        }).where(kernels.c.id == sess_ctx.kernel_id)
                        await db_conn.execute(query)
                    else:
                        log.info(log_fmt + 'preparation-started', *log_args)
                        await _invoke_success_callbacks(check_results, use_new_txn=True)

                task.add_done_callback(lambda fut: loop.create_task(_cb(fut)))

    async def _list_pending_sessions(self, db_conn) -> Sequence[SessionContext]:
        query = (
            sa.select([
                kernels.c.id,
                kernels.c.status,
                kernels.c.image,
                kernels.c.registry,
                kernels.c.type,
                kernels.c.sess_id,
                kernels.c.access_key,
                kernels.c.domain_name,
                kernels.c.group_id,
                kernels.c.scaling_group,
                kernels.c.occupied_slots,
                kernels.c.resource_opts,
                kernels.c.environ,
                kernels.c.mounts,
                keypairs.c.resource_policy,
            ], for_update=True)
            .select_from(sa.join(
                kernels, keypairs,
                keypairs.c.access_key == kernels.c.access_key
            ))
            .where(kernels.c.status == KernelStatus.PENDING)
        )
        sess_ctxs = []
        async for row in db_conn.execute(query):
            sess_ctxs.append(SessionContext(
                kernel_id=row['id'],
                image_ref=ImageRef(row['image'], [row['registry']]),
                access_key=row['access_key'],
                sess_type=row['type'],
                sess_id=row['sess_id'],
                domain_name=row['domain_name'],
                group_id=row['group_id'],
                scaling_group=row['scaling_group'],
                resource_policy=row['resource_policy'],
                resource_opts=row['resource_opts'],
                requested_slots=row['occupied_slots'],
                target_sgroup_names=[],
                environ={
                    k: v for k, v
                    in map(lambda s: s.split('=', maxsplit=1), row['environ'])
                },
                mounts=row['mounts'],
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

        # Reserve agent slots
        query = (sa.update(agents)
                   .values({
                       'occupied_slots': current_occupied_slots + sess_ctx.requested_slots
                   })
                   .where(agents.c.id == agent_id))
        await sched_ctx.db_conn.execute(query)

        # Get the agent address for later RPC calls
        query = (sa.select([agents.c.addr])
                   .where(agents.c.id == agent_id))
        agent_addr = await sched_ctx.db_conn.scalar(query)
        assert agent_addr is not None

        return AgentAllocationContext(agent_id, agent_addr)

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
