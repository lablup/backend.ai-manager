import logging
import re
from typing import (
    Sequence,
    List,
    Tuple,
    TypedDict,
    Union,
)

from aiopg.sa.connection import SAConnection
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ResourceSlot
from .base import (
    metadata, ResourceSlotColumn,
    simple_db_mutate,
    simple_db_mutate_returning_item,
    set_if_set,
    batch_result,
)
from .scaling_group import ScalingGroup
from .user import UserRole
from ..defs import RESERVED_DOTFILES
from ai.backend.common import msgpack

log = BraceStyleAdapter(logging.getLogger(__file__))


__all__: Sequence[str] = (
    'domains',
    'Domain', 'DomainInput', 'ModifyDomainInput',
    'CreateDomain', 'ModifyDomain', 'DeleteDomain',
    'DomainDotfile', 'MAXIMUM_DOTFILE_SIZE',
    'query_domain_dotfiles',
    'verify_dotfile_name',
)

MAXIMUM_DOTFILE_SIZE = 64 * 1024  # 61 KiB
_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')

domains = sa.Table(
    'domains', metadata,
    sa.Column('name', sa.String(length=64), primary_key=True),
    sa.Column('description', sa.String(length=512)),
    sa.Column('is_active', sa.Boolean, default=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('modified_at', sa.DateTime(timezone=True),
              server_default=sa.func.now(), onupdate=sa.func.current_timestamp()),
    # TODO: separate resource-related fields with new domain resource policy table when needed.
    sa.Column('total_resource_slots', ResourceSlotColumn(), default='{}'),
    sa.Column('allowed_vfolder_hosts', pgsql.ARRAY(sa.String), nullable=False, default='{}'),
    sa.Column('allowed_docker_registries', pgsql.ARRAY(sa.String), nullable=False, default='{}'),
    #: Field for synchronization with external services.
    sa.Column('integration_id', sa.String(length=512)),
    # dotfiles column, \x90 means empty list in msgpack
    sa.Column('dotfiles', sa.LargeBinary(length=MAXIMUM_DOTFILE_SIZE), nullable=False, default=b'\x90'),
)


class Domain(graphene.ObjectType):
    name = graphene.String()
    description = graphene.String()
    is_active = graphene.Boolean()
    created_at = GQLDateTime()
    modified_at = GQLDateTime()
    total_resource_slots = graphene.JSONString()
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String)
    allowed_docker_registries = graphene.List(lambda: graphene.String)
    integration_id = graphene.String()

    # Dynamic fields.
    scaling_groups = graphene.List(lambda: graphene.String)

    async def resolve_scaling_groups(self, info):
        sgroups = await ScalingGroup.load_by_domain(info.context, self.name)
        return [sg.name for sg in sgroups]

    @classmethod
    def from_row(cls, context, row):
        if row is None:
            return None
        return cls(
            name=row['name'],
            description=row['description'],
            is_active=row['is_active'],
            created_at=row['created_at'],
            modified_at=row['modified_at'],
            total_resource_slots=row['total_resource_slots'].to_json(),
            allowed_vfolder_hosts=row['allowed_vfolder_hosts'],
            allowed_docker_registries=row['allowed_docker_registries'],
            integration_id=row['integration_id'],
        )

    @classmethod
    async def load_all(cls, context, *, is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = sa.select([domains]).select_from(domains)
            if is_active is not None:
                query = query.where(domains.c.is_active == is_active)
            return [
                cls.from_row(context, row) async for row in conn.execute(query)
            ]

    @classmethod
    async def batch_load_by_name(cls, context, names=None, *,
                                 is_active=None):
        async with context['dbpool'].acquire() as conn:
            query = (sa.select([domains])
                       .select_from(domains)
                       .where(domains.c.name.in_(names)))
            return await batch_result(
                context, conn, query, cls,
                names, lambda row: row['name'],
            )


class DomainInput(graphene.InputObjectType):
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False, default=True)
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    allowed_docker_registries = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class ModifyDomainInput(graphene.InputObjectType):
    name = graphene.String(required=False)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    total_resource_slots = graphene.JSONString(required=False)
    allowed_vfolder_hosts = graphene.List(lambda: graphene.String, required=False)
    allowed_docker_registries = graphene.List(lambda: graphene.String, required=False)
    integration_id = graphene.String(required=False)


class CreateDomain(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)
        props = DomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain, required=False)

    @classmethod
    async def mutate(cls, root, info, name, props):
        if _rx_slug.search(name) is None:
            return cls(False, 'invalid name format. slug format required.', None)
        data = {
            'name': name,
            'description': props.description,
            'is_active': props.is_active,
            'total_resource_slots': ResourceSlot.from_user_input(
                props.total_resource_slots, None),
            'allowed_vfolder_hosts': props.allowed_vfolder_hosts,
            'allowed_docker_registries': props.allowed_docker_registries,
            'integration_id': props.integration_id,
        }
        insert_query = (
            domains.insert()
            .values(data)
        )
        item_query = domains.select().where(domains.c.name == name)
        return await simple_db_mutate_returning_item(
            cls, info.context, insert_query,
            item_query=item_query, item_cls=Domain)


class ModifyDomain(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)
        props = ModifyDomainInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    domain = graphene.Field(lambda: Domain, required=False)

    @classmethod
    async def mutate(cls, root, info, name, props):
        data = {}
        set_if_set(props, data, 'name')  # data['name'] is new domain name
        set_if_set(props, data, 'description')
        set_if_set(props, data, 'is_active')
        set_if_set(props, data, 'total_resource_slots',
                   clean_func=lambda v: ResourceSlot.from_user_input(v, None))
        set_if_set(props, data, 'allowed_vfolder_hosts')
        set_if_set(props, data, 'allowed_docker_registries')
        set_if_set(props, data, 'integration_id')
        if 'name' in data and _rx_slug.search(data['name']) is None:
            raise ValueError('invalid name format. slug format required.')
        update_query = (
            domains.update()
            .values(data)
            .where(domains.c.name == name)
        )
        # The name may have changed if set.
        if 'name' in data:
            name = data['name']
        item_query = domains.select().where(domains.c.name == name)
        return await simple_db_mutate_returning_item(
            cls, info.context, update_query,
            item_query=item_query, item_cls=Domain)


class DeleteDomain(graphene.Mutation):
    """
    Instead of deleting the domain, just mark it as inactive.
    """
    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        query = (
            domains.update()
            .values(is_active=False)
            .where(domains.c.name == name)
        )
        return await simple_db_mutate(cls, info.context, query)


class PurgeDomain(graphene.Mutation):
    """
    Completely delete domain from DB.

    Domain-bound kernels will also be all deleted.
    To purge domain, there should be no users and groups in the target domain.
    """
    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        name = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @classmethod
    async def mutate(cls, root, info, name):
        from . import users, groups
        async with info.context['dbpool'].acquire() as conn:
            if await cls.domain_has_active_kernels(conn, name):
                raise RuntimeError('Domain has some active kernels. Terminate them first.')
            query = (
                sa.select([sa.func.count()])
                .where(users.c.domain_name == name)
            )
            user_count = await conn.scalar(query)
            if user_count > 0:
                raise RuntimeError('There are users bound to the domain. Remove users first.')
            query = (
                sa.select([sa.func.count()])
                .where(groups.c.domain_name == name)
            )
            group_count = await conn.scalar(query)
            if group_count > 0:
                raise RuntimeError('There are groups bound to the domain. Remove groups first.')

            await cls.delete_kernels(conn, name)
        query = domains.delete().where(domains.c.name == name)
        return await simple_db_mutate(cls, info.context, query)

    @classmethod
    async def delete_kernels(
        cls,
        conn: SAConnection,
        domain_name: str,
    ) -> int:
        """
        Delete all kernels run from the target domain.

        :param conn: DB connection
        :param domain_name: domain's name to delete kernels

        :return: number of deleted rows
        """
        from . import kernels
        query = (
            kernels.delete()
            .where(kernels.c.domain_name == domain_name)
        )
        result = await conn.execute(query)
        if result.rowcount > 0:
            log.info('deleted {0} domain\'s kernels ({1})', result.rowcount, domain_name)
        return result.rowcount

    @classmethod
    async def domain_has_active_kernels(
        cls,
        conn: SAConnection,
        domain_name: str,
    ) -> bool:
        """
        Check if the domain does not have active kernels.

        :param conn: DB connection
        :param domain_name: domain's name

        :return: True if the domain has some active kernels.
        """
        from . import kernels, AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES
        query = (
            sa.select([sa.func.count()])
            .select_from(kernels)
            .where((kernels.c.domain_name == domain_name) &
                   (kernels.c.status.in_(AGENT_RESOURCE_OCCUPYING_KERNEL_STATUSES)))
        )
        active_kernel_count = await conn.scalar(query)
        return (active_kernel_count > 0)


class DomainDotfile(TypedDict):
    data: str
    path: str
    perm: str


async def query_domain_dotfiles(
    conn: SAConnection,
    name: str,
) -> Tuple[Union[List[DomainDotfile], None], Union[int, None]]:
    query = (sa.select([domains.c.dotfiles])
               .select_from(domains)
               .where(domains.c.name == name))
    packed_dotfile = await conn.scalar(query)
    if packed_dotfile is None:
        return None, None
    rows = msgpack.unpackb(packed_dotfile)
    return rows, MAXIMUM_DOTFILE_SIZE - len(packed_dotfile)


def verify_dotfile_name(dotfile: str) -> bool:
    if dotfile in RESERVED_DOTFILES:
        return False
    return True
