from collections import OrderedDict
import logging
from typing import Sequence

import graphene
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from .base import (
    metadata, ResourceSlotColumn,
    privileged_mutation,
    simple_db_mutate,
    simple_db_mutate_returning_item,
    set_if_set,
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
)


class ResourcePreset(graphene.ObjectType):
    name = graphene.String()
    resource_slots = graphene.JSONString()

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            resource_slots=row['resource_slots'].to_json(),
        )

    @classmethod
    async def load_all(cls, context):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([resource_presets])
                       .select_from(resource_presets))
            result = await conn.execute(query)
            rows = await result.fetchall()
            return [cls.from_row(context, r) for r in rows]

    @classmethod
    async def batch_load_by_name(cls, context, names):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([resource_presets])
                       .select_from(resource_presets)
                       .where(resource_presets.c.name.in_(names))
                       .order_by(resource_presets.c.name))
            objs_per_key = OrderedDict()
            for k in names:
                objs_per_key[k] = None
            async for row in conn.execute(query):
                o = cls.from_row(context, row)
                objs_per_key[row.name] = o
        return tuple(objs_per_key.values())


class CreateResourcePresetInput(graphene.InputObjectType):
    resource_slots = graphene.JSONString(required=True)


class ModifyResourcePresetInput(graphene.InputObjectType):
    resource_slots = graphene.JSONString(required=False)


class CreateResourcePreset(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = CreateResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    resource_preset = graphene.Field(lambda: ResourcePreset)

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, name, props):
        data = {
            'name': name,
            'resource_slots': ResourceSlot.from_user_input(
                props.resource_slots, None),
        }
        insert_query = (resource_presets.insert().values(data))
        item_query = (
            resource_presets.select()
            .where(resource_presets.c.name == name))
        return await simple_db_mutate_returning_item(
            cls, info.context, insert_query,
            item_query=item_query, item_cls=ResourcePreset)


class ModifyResourcePreset(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, name, props):
        data = {}
        set_if_set(props, data, 'resource_slots',
                   clean_func=lambda v: ResourceSlot.from_user_input(v, None))
        update_query = (
            resource_presets.update()
            .values(data)
            .where(resource_presets.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, update_query)


class DeleteResourcePreset(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    @privileged_mutation(UserRole.SUPERADMIN)
    async def mutate(cls, root, info, name):
        delete_query = (
            resource_presets.delete()
            .where(resource_presets.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, delete_query)
