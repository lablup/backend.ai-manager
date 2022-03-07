from datetime import datetime
import logging
from typing import (
    List,
)

from dateutil.tz import tzutc
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    ResourceSlot, SessionTypes,
)

from ..models import (
    domains, groups, kernels,
    keypair_resource_usages,
    keypair_resource_policies,
    query_allowed_sgroups,
    DefaultForUnspecified,
)
from ..models.utils import execute_with_retry, reenter_txn
from .types import (
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
                'Before start time',
            )
    return PredicateResult(True)


async def check_concurrency(
    db_conn: SAConnection,
    sched_ctx: SchedulingContext,
    sess_ctx: PendingSession,
) -> PredicateResult:

    async def _check() -> PredicateResult:
        async with db_conn.begin_nested():
            select_query = (
                sa.select([keypair_resource_policies])
                .select_from(keypair_resource_policies)
                .where(keypair_resource_policies.c.name == sess_ctx.resource_policy)
            )
            result = await db_conn.execute(select_query)
            resource_policy = result.first()
            select_query = (
                sa.select([keypair_resource_usages.c.concurrency_used])
                .select_from(keypair_resource_usages)
                .where(keypair_resource_usages.c.access_key == sess_ctx.access_key)
                .with_for_update()
            )
            concurrency_used = (await db_conn.execute(select_query)).scalar()
            log.debug(
                'access_key: {0} ({1} / {2})',
                sess_ctx.access_key, concurrency_used,
                resource_policy['max_concurrent_sessions'],
            )
            if concurrency_used >= resource_policy['max_concurrent_sessions']:
                return PredicateResult(
                    False,
                    "You cannot run more than "
                    f"{resource_policy['max_concurrent_sessions']} concurrent sessions",
                )

            # Increment concurrency usage of keypair.
            update_query = (
                sa.update(keypair_resource_usages)
                .values(concurrency_used=keypair_resource_usages.c.concurrency_used + 1)
                .where(keypair_resource_usages.c.access_key == sess_ctx.access_key)
            )
            await db_conn.execute(update_query)
            return PredicateResult(True)

    return await execute_with_retry(_check)


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
    resource_policy = result.first()
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
            )),
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
        'default_for_unspecified': DefaultForUnspecified.UNLIMITED,
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

    async def _query():
        async with reenter_txn(sched_ctx.registry.db, db_conn) as _conn:
            return await query_allowed_sgroups(
                _conn,
                sess_ctx.domain_name,
                sess_ctx.group_id,
                sess_ctx.access_key,
            )

    sgroups = await execute_with_retry(_query)
    if not sgroups:
        return PredicateResult(
            False,
            "You do not have any scaling groups allowed to use.",
            permanent=True,
        )
    target_sgroup_names: List[str] = []
    preferred_sgroup_name = sess_ctx.scaling_group
    if preferred_sgroup_name is not None:
        # Consider only the preferred scaling group.
        for sgroup in sgroups:
            if preferred_sgroup_name == sgroup['name']:
                break
        else:
            return PredicateResult(
                False,
                f"You do not have access to the scaling group '{preferred_sgroup_name}'.",
                permanent=True,
            )
        allowed_session_types = sgroup['scheduler_opts']['allowed_session_types']
        if sess_ctx.session_type.value.lower() not in allowed_session_types:
            return PredicateResult(
                False,
                f"The scaling group '{preferred_sgroup_name}' does not accept "
                f"the session type '{sess_ctx.session_type}'. ",
                permanent=True,
            )
        target_sgroup_names = [preferred_sgroup_name]
    else:
        # Consider all allowed scaling groups.
        usable_sgroups = []
        for sgroup in sgroups:
            allowed_session_types = sgroup['scheduler_opts']['allowed_session_types']
            if sess_ctx.session_type.value.lower() in allowed_session_types:
                usable_sgroups.append(sgroup)
        if not usable_sgroups:
            return PredicateResult(
                False,
                f"No scaling groups accept the session type '{sess_ctx.session_type}'.",
                permanent=True,
            )
        target_sgroup_names = [sgroup['name'] for sgroup in usable_sgroups]
    assert target_sgroup_names
    log.debug("scaling groups considered for s:{} are {}", sess_ctx.session_id, target_sgroup_names)
    sess_ctx.target_sgroup_names.extend(target_sgroup_names)
    return PredicateResult(True)
