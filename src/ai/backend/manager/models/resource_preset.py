from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    Sequence,
    Mapping,
    Tuple,
    TYPE_CHECKING,
)
import uuid

from aiohttp import web
import graphene
import sqlalchemy as sa
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection
from decimal import Decimal

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from ..api.exceptions import (
    InvalidAPIParameters,
)
from .base import (
    metadata, BigInt, BinarySize, ResourceSlotColumn,
    simple_db_mutate,
    simple_db_mutate_returning_item,
    set_if_set,
    batch_result,
)
from .agent import (
    agents, AgentStatus,
)
from .user import UserRole
from .group import (
    groups,
    association_groups_users,
)
from .scaling_group import query_allowed_sgroups
from .kernel import (
    AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES,
    kernels,
)
from .domain import domains
if TYPE_CHECKING:
    from .gql import GraphQueryContext

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.models'))

__all__: Sequence[str] = (
    'resource_presets',
    'ResourcePreset',
    'CreateResourcePreset',
    'ModifyResourcePreset',
    'DeleteResourcePreset',
    'get_groups_info_by_row',
    'check_scaling_group_resource',
    'check_group_resource',
)


resource_presets = sa.Table(
    'resource_presets', metadata,
    sa.Column('name', sa.String(length=256), primary_key=True),
    sa.Column('resource_slots', ResourceSlotColumn(), nullable=False),
    sa.Column('shared_memory', sa.BigInteger(), nullable=True),
)


class ResourcePreset(graphene.ObjectType):
    name = graphene.String()
    resource_slots = graphene.JSONString()
    shared_memory = BigInt()

    @classmethod
    def from_row(
        cls,
        ctx: GraphQueryContext,
        row: Row | None
    ) -> ResourcePreset | None:
        if row is None:
            return None
        shared_memory = str(row['shared_memory']) if row['shared_memory'] else None
        return cls(
            name=row['name'],
            resource_slots=row['resource_slots'].to_json(),
            shared_memory=shared_memory,
        )

    @classmethod
    async def load_all(cls, ctx: GraphQueryContext) -> Sequence[ResourcePreset]:
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets)
        )
        async with ctx.db.begin_readonly() as conn:
            return [
                obj async for r in (await conn.stream(query))
                if (obj := cls.from_row(ctx, r)) is not None
            ]

    @classmethod
    async def batch_load_by_name(
        cls,
        ctx: GraphQueryContext,
        names: Sequence[str],
    ) -> Sequence[ResourcePreset | None]:
        query = (
            sa.select([resource_presets])
            .select_from(resource_presets)
            .where(resource_presets.c.name.in_(names))
            .order_by(resource_presets.c.name)
        )
        async with ctx.db.begin_readonly() as conn:
            return await batch_result(
                ctx, conn, query, cls,
                names, lambda row: row['name'],
            )


class CreateResourcePresetInput(graphene.InputObjectType):
    resource_slots = graphene.JSONString(required=True)
    shared_memory = graphene.String(required=False)


class ModifyResourcePresetInput(graphene.InputObjectType):
    resource_slots = graphene.JSONString(required=False)
    shared_memory = graphene.String(required=False)


class CreateResourcePreset(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)
        props = CreateResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    resource_preset = graphene.Field(lambda: ResourcePreset, required=False)

    @classmethod
    async def mutate(
        cls,
        root,
        info: graphene.ResolveInfo,
        name: str,
        props: CreateResourcePresetInput,
    ) -> CreateResourcePreset:
        data = {
            'name': name,
            'resource_slots': ResourceSlot.from_user_input(
                props.resource_slots, None),
            'shared_memory': BinarySize.from_str(props.shared_memory) if props.shared_memory else None,
        }
        insert_query = sa.insert(resource_presets).values(data)
        return await simple_db_mutate_returning_item(
            cls, info.context, insert_query,
            item_cls=ResourcePreset,
        )


class ModifyResourcePreset(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(
        cls,
        root,
        info: graphene.ResolveInfo,
        name: str,
        props: ModifyResourcePresetInput,
    ) -> ModifyResourcePreset:
        data: Dict[str, Any] = {}
        set_if_set(props, data, 'resource_slots',
                   clean_func=lambda v: ResourceSlot.from_user_input(v, None))
        set_if_set(props, data, 'shared_memory',
                   clean_func=lambda v: BinarySize.from_str(v) if v else None)
        update_query = (
            sa.update(resource_presets)
            .values(data)
            .where(resource_presets.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, update_query)


class DeleteResourcePreset(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(
        cls,
        root,
        info: graphene.ResolveInfo,
        name: str,
    ) -> DeleteResourcePreset:
        delete_query = (
            sa.delete(resource_presets)
            .where(resource_presets.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, delete_query)


async def get_groups_info_by_row(
    conn: SAConnection,
    request: web.Request,
    params: Any,
    domain_name: str
) -> Row:
    """
    Returns row that has id and total resource slots in group.
    """
    j = sa.join(
        groups, association_groups_users,
        association_groups_users.c.group_id == groups.c.id,
    )
    query = (
        sa.select([groups.c.id, groups.c.total_resource_slots])
        .select_from(j)
        .where(
            (association_groups_users.c.user_id == request['user']['uuid']) &
            (groups.c.name == params['group']) &
            (domains.c.name == domain_name)
        )
    )
    result = await conn.execute(query)
    row = result.first()

    return row


async def check_scaling_group_resource(
    conn: SAConnection,
    sgroup_name: str,
    known_slot_types: Mapping[str, str],
) -> Tuple[ResourceSlot, ResourceSlot]:
    """
    Returns scaling group resource, scaling group resource using from resource occupying kernels,
    and scaling group resource remaining from agents stats as tuple.
    """
    sgroup_capacity = ResourceSlot({k: Decimal(0) for k in known_slot_types.keys()})
    sgroup_remaining = ResourceSlot({k: Decimal(0) for k in known_slot_types.keys()})
    query = (
        sa.select([agents.c.available_slots, agents.c.occupied_slots])
        .select_from(agents)
        .where(
            (agents.c.status == AgentStatus.ALIVE) &
            (agents.c.scaling_group == sgroup_name)
        )
    )
    async for row in (await conn.stream(query)):
        sgroup_capacity += row['available_slots']
        sgroup_remaining += row['available_slots'] - row['occupied_slots']
    return sgroup_capacity, sgroup_remaining


async def check_group_resource(
    conn: SAConnection,
    group: Row,  # TODO: refactor as ORM-based Group
    known_slot_types: Mapping,
    *,
    group_resource_visibility: bool = True,
) -> Tuple:
    """
    Returns limits, occupied, and remaining status of groups resource as tuple.
    """

    # TODO: work-in-progress

    group_resource_slots = group.total_resource_slots
    if not group_resource_visibility:
        # Hide the group resource config.
        group_limits = ResourceSlot({k: Decimal('NaN') for k in known_slot_types.keys()})
        group_occupied = ResourceSlot({k: Decimal('NaN') for k in known_slot_types.keys()})
        group_remaining = ResourceSlot({k: Decimal('NaN') for k in known_slot_types.keys()})
    else:
        group_resource_policy = {
            'total_resource_slots': group_resource_slots,
            'default_for_unspecified': DefaultForUnspecified.UNLIMITED
        }
        group_limits = ResourceSlot.from_policy(group_resource_policy, known_slot_types)
        group_occupied = await root_ctx.registry.get_group_occupancy(group.id, conn=conn)
        group_remaining = group_limits - group_occupied

    # Per scaling group resource using from resource occupying kernels.
    query = (
        sa.select([kernels.c.occupied_slots, kernels.c.scaling_group])
        .select_from(kernels)
        .where(
            (kernels.c.user_uuid == request['user']['uuid']) &
            (kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES)) &
            (kernels.c.scaling_group.in_(sgroup_names))
        )
    )
    async for row in (await conn.stream(query)):
        per_sgroup[row['scaling_group']]['using'] += row['occupied_slots']

    return group_limits, group_occupied, group_remaining
