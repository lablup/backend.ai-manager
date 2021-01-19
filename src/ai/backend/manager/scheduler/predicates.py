from datetime import datetime
import logging
from typing import (
    List,
)

from aiopg.sa.connection import SAConnection
from dateutil.tz import tzutc
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    ResourceSlot, SessionTypes,
)

from ai.backend.manager.models.kernel import recalc_concurrency_used

from ..models import (
    domains, groups, kernels, keypairs,
    keypair_resource_policies,
    query_allowed_sgroups,
    DefaultForUnspecified,
)
from . import (
    SchedulingContext,
    PendingSession,
    PredicateResult,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))


async def check_reserved_batch_session(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    """
    Check if a batch-type session should not be started for a certain amount of time.
    """
    if sess_ctx.session_type == SessionTypes.BATCH:
        query = (
            sa.select([kernels.c.starts_at])
            .select_from(kernels)
            .where(kernels.c.id == sess_ctx.session_id)
        )
        starts_at = await db_conn.scalar(query)
        if starts_at is not None and datetime.now(tzutc()) < starts_at:
            return PredicateResult(
                False,
                'Before start time'
            )
    return PredicateResult(True)


async def check_concurrency(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    query = (
        sa.select([keypair_resource_policies])
        .select_from(keypair_resource_policies)
        .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
    )
    result = await db_conn.execute(query)
    resource_policy = await result.first()
    query = (sa.select([keypairs.c.concurrency_used], for_update=True)
               .select_from(keypairs)
               .where(keypairs.c.access_key == sess_ctx.access_key))
    concurrency_used = await db_conn.scalar(query)
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
    await db_conn.execute(query)

    async def rollback(
        db_conn: SAConnection,
        sched_ctx: SchedulingContext,
        sess_ctx: PendingSession,
    ) -> None:
        # Instead of subtraction, we recalculate the access_key's usage,
        # because asynchronous container launch failures and agent failures
        # (especially with multi-node multi-container cluster sessions)
        # may accumulate up multiple subtractions, resulting in
        # negative concurrency_occupied values.
        await recalc_concurrency_used(db_conn, sess_ctx.access_key)

    return PredicateResult(True, failure_cb=rollback)


async def check_dependencies(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    # TODO: implement
    return PredicateResult(True, 'bypassing because it is not implemented')


async def check_keypair_resource_limit(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    query = (
        sa.select([keypair_resource_policies])
        .select_from(keypair_resource_policies)
        .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
    )
    result = await db_conn.execute(query)
    resource_policy = await result.first()
    if len(sess_ctx.kernels) > resource_policy['max_containers_per_session']:
        return PredicateResult(
            False,
            f"You cannot create session with more than "
            f"{resource_policy['max_containers_per_session']} containers.",
            permanent=True,
        )
    total_keypair_allowed = ResourceSlot.from_policy(resource_policy,
                                                     sched_ctx.known_slot_types)
    key_occupied = await sched_ctx.registry.get_keypair_occupancy(
        sess_ctx.access_key, conn=db_conn)
    log.debug('keypair:{} current-occupancy: {}', sess_ctx.access_key, key_occupied)
    log.debug('keypair:{} total-allowed: {}', sess_ctx.access_key, total_keypair_allowed)
    if not (key_occupied + sess_ctx.requested_slots <= total_keypair_allowed):
        return PredicateResult(
            False,
            "Your keypair resource quota is exceeded. ({})"
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_keypair_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )),
        )
    return PredicateResult(True)


async def check_group_resource_limit(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    query = (sa.select([groups.c.total_resource_slots])
               .where(groups.c.id == sess_ctx.group_id))
    group_resource_slots = await db_conn.scalar(query)
    group_resource_policy = {'total_resource_slots': group_resource_slots,
                             'default_for_unspecified': DefaultForUnspecified.UNLIMITED}
    total_group_allowed = ResourceSlot.from_policy(group_resource_policy,
                                                   sched_ctx.known_slot_types)
    group_occupied = await sched_ctx.registry.get_group_occupancy(
        sess_ctx.group_id, conn=db_conn)
    log.debug('group:{} current-occupancy: {}', sess_ctx.group_id, group_occupied)
    log.debug('group:{} total-allowed: {}', sess_ctx.group_id, total_group_allowed)
    if not (group_occupied + sess_ctx.requested_slots <= total_group_allowed):
        return PredicateResult(
            False,
            "Your group resource quota is exceeded. ({})"
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_group_allowed.to_humanized(sched_ctx.known_slot_types).items()
            ))
        )
    return PredicateResult(True)


async def check_domain_resource_limit(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    query = (sa.select([domains.c.total_resource_slots])
               .where(domains.c.name == sess_ctx.domain_name))
    domain_resource_slots = await db_conn.scalar(query)
    domain_resource_policy = {
        'total_resource_slots': domain_resource_slots,
        'default_for_unspecified': DefaultForUnspecified.UNLIMITED
    }
    total_domain_allowed = ResourceSlot.from_policy(domain_resource_policy,
                                                    sched_ctx.known_slot_types)
    domain_occupied = await sched_ctx.registry.get_domain_occupancy(
        sess_ctx.domain_name, conn=db_conn)
    log.debug('domain:{} current-occupancy: {}', sess_ctx.domain_name, domain_occupied)
    log.debug('domain:{} total-allowed: {}', sess_ctx.domain_name, total_domain_allowed)
    if not (domain_occupied + sess_ctx.requested_slots <= total_domain_allowed):
        return PredicateResult(
            False,
            'Your domain resource quota is exceeded. ({})'
            .format(' '.join(
                f'{k}={v}' for k, v in
                total_domain_allowed.to_humanized(sched_ctx.known_slot_types).items()
            )),
        )
    return PredicateResult(True)


async def check_scaling_group(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:
    sgroups = await query_allowed_sgroups(
        db_conn,
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
                f"The given preferred scaling group is not allowed to use. "
                f"({preferred_sgroup_name})",
                permanent=True,
            )
        # Consider agents only in the preferred scaling group.
        target_sgroup_names = [preferred_sgroup_name]
    else:
        # Consider all agents in all allowed scaling groups.
        target_sgroup_names = [sgroup['name'] for sgroup in sgroups]
    log.debug('considered scaling groups: {}', target_sgroup_names)
    if not target_sgroup_names:
        return PredicateResult(
            False,
            "No available resource in scaling groups.",
        )
    sess_ctx.target_sgroup_names.extend(target_sgroup_names)
    return PredicateResult(True)
