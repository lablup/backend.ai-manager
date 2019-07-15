import asyncio
from collections import OrderedDict
import logging

import graphene
import sqlalchemy as sa
import psycopg2 as pg

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from .base import metadata, ResourceSlotColumn

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.models'))

__all__ = (
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
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {
                'name': name,
                'resource_slots': ResourceSlot.from_user_input(
                    props.resource_slots, None),
            }
            query = (resource_presets.insert().values(data))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    checkq = (
                        resource_presets.select()
                        .where(resource_presets.c.name == name))
                    result = await conn.execute(checkq)
                    o = ResourcePreset.from_row(
                        info.context, await result.first())
                    return cls(ok=True, msg='success', resource_policy=o)
                else:
                    return cls(ok=False, msg='failed to create resource policy',
                               resource_policy=None)
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False, msg=f'integrity error: {e}',
                           resource_policy=None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(
                    ok=False,
                    msg=f'unexpected error ({type(e).__name__}): {e}',
                    resource_policy=None)


class ModifyResourcePreset(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyResourcePresetInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name, props):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            data = {}

            def set_if_set(name, clean=lambda v: v):
                v = getattr(props, name)
                # NOTE: unset optional fields are passed as null.
                if v is not None:
                    data[name] = clean(v)

            def clean_resource_slot(v):
                return ResourceSlot.from_user_input(v, None)

            set_if_set('resource_slots', clean_resource_slot)

            try:
                query = (
                    resource_presets.update()
                    .values(data)
                    .where(resource_presets.c.name == name))
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such resource policy')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')


class DeleteResourcePreset(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        async with info.context['dbpool'].acquire() as conn, conn.begin():
            query = (
                resource_presets.delete()
                .where(resource_presets.c.name == name))
            try:
                result = await conn.execute(query)
                if result.rowcount > 0:
                    return cls(ok=True, msg='success')
                else:
                    return cls(ok=False, msg='no such resource policy')
            except (pg.IntegrityError, sa.exc.IntegrityError) as e:
                return cls(ok=False,
                           msg=f'integrity error: {e}')
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as e:
                return cls(ok=False,
                           msg=f'unexpected error: {e}')
