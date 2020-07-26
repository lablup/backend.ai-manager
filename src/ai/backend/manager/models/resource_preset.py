import logging
from typing import Any, Mapping, Sequence

import graphene
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from aiopg.sa.result import RowProxy
from .base import (
    metadata, BigInt, BinarySize, ResourceSlotColumn,
    simple_db_mutate,
    simple_db_mutate_returning_item,
    set_if_set,
    batch_result,
)
from .user import UserRole

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.models'))

__all__: Sequence[str] = (
    'resource_presets',
    'ResourcePreset',
    'CreateResourcePreset',
    'ModifyResourcePreset',
    'DeleteResourcePreset',
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
        context: Mapping[str, Any],
        row: RowProxy
    ):
        if row is None:
            return None
        shared_memory = str(row['shared_memory']) if row['shared_memory'] else None
        return cls(
            name=row['name'],
            resource_slots=row['resource_slots'].to_json(),
            shared_memory=shared_memory,
        )

    @classmethod
    async def load_all(cls, context):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([resource_presets])
                       .select_from(resource_presets))
            return [cls.from_row(context, r) async for r in conn.execute(query)]

    @classmethod
    async def batch_load_by_name(cls, context, names):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([resource_presets])
                       .select_from(resource_presets)
                       .where(resource_presets.c.name.in_(names))
                       .order_by(resource_presets.c.name))
            return await batch_result(
                context, conn, query, cls,
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
    async def mutate(cls, root, info, name, props):
        data = {
            'name': name,
            'resource_slots': ResourceSlot.from_user_input(
                props.resource_slots, None),
            'shared_memory': BinarySize.from_str(props.shared_memory) if props.shared_memory else None,
        }
        insert_query = (resource_presets.insert().values(data))
        item_query = (
            resource_presets.select()
            .where(resource_presets.c.name == name))
        return await simple_db_mutate_returning_item(
            cls, info.context, insert_query,
            item_query=item_query, item_cls=ResourcePreset)


class ModifyResourcePreset(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name, props):
        data = {}
        set_if_set(props, data, 'resource_slots',
                   clean_func=lambda v: ResourceSlot.from_user_input(v, None))
        set_if_set(props, data, 'shared_memory',
                   clean_func=lambda v: BinarySize.from_str(v) if v else None)
        update_query = (
            resource_presets.update()
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
    async def mutate(cls, root, info, name):
        delete_query = (
            resource_presets.delete()
            .where(resource_presets.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, delete_query)
