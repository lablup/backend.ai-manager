import asyncio
import inspect
import logging
from typing import Any

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
from graphql.error import GraphQLError, format_error
import trafaret as t

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common import validators as tx

from .manager import GQLMutationUnfrozenRequiredMiddleware
from .exceptions import (
    GenericForbidden,
    InvalidAPIParameters,
    GraphQLError as BackendGQLError
)
from .auth import auth_required
from .utils import check_api_params
from ..manager.models.base import DataLoaderManager, privileged_query
from ..manager.models import (
    Agent, AgentList, Image, RescanImages, AliasImage, DealiasImage,
    Domain, CreateDomain, ModifyDomain, DeleteDomain,
    Group, CreateGroup, ModifyGroup, DeleteGroup,
    User, CreateUser, ModifyUser, DeleteUser, UserRole,
    KeyPair, CreateKeyPair, ModifyKeyPair, DeleteKeyPair,
    ComputeSession, ComputeSessionList, ComputeWorker, KernelStatus,
    VirtualFolder,
    KeyPairResourcePolicy, CreateKeyPairResourcePolicy,
    ModifyKeyPairResourcePolicy, DeleteKeyPairResourcePolicy,
    ResourcePreset,
    CreateResourcePreset, ModifyResourcePreset, DeleteResourcePreset,
    ScalingGroup,
    CreateScalingGroup, ModifyScalingGroup, DeleteScalingGroup,
    AssociateScalingGroupWithDomain, DisassociateScalingGroupWithDomain,
    AssociateScalingGroupWithUserGroup, DisassociateScalingGroupWithUserGroup,
    AssociateScalingGroupWithKeyPair, DisassociateScalingGroupWithKeyPair,
)
from .exceptions import GenericNotFound

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.admin'))


@atomic
@auth_required
@check_api_params(
    t.Dict({
        t.Key('query'): t.String,
        t.Key('variables', default=None): t.Null | t.Mapping(t.String, t.Any),
        tx.AliasedKey(['operation_name', 'operationName'], default=None): t.Null | t.String,
    }))
async def handle_gql(request: web.Request, params: Any) -> web.Response:
    executor = request.app['admin.gql_executor']
    schema = request.app['admin.gql_schema']
    manager_status = await request.app['config_server'].get_manager_status()
    known_slot_types = await request.app['config_server'].get_resource_slots()
    context = {
        'config_server': request.app['config_server'],
        'etcd': request.app['config_server'].etcd,
        'user': request['user'],
        'access_key': request['keypair']['access_key'],
        'dbpool': request.app['dbpool'],
        'redis_stat': request.app['redis_stat'],
        'manager_status': manager_status,
        'known_slot_types': known_slot_types,
    }
    dlmanager = DataLoaderManager(context)
    result = schema.execute(
        params['query'], executor,
        variable_values=params['variables'],
        operation_name=params['operation_name'],
        context_value={
            'dlmgr': dlmanager,
            **context,
        },
        middleware=[GQLMutationUnfrozenRequiredMiddleware()],
        return_promise=True)
    if inspect.isawaitable(result):
        result = await result
    if result.errors:
        errors = []
        for e in result.errors:
            if isinstance(e, GraphQLError):
                errors.append(format_error(e))
            else:
                errors.append({'message': str(e)})
        raise BackendGQLError(extra_data=errors)
    return web.json_response(result.data, status=200)


class Mutations(graphene.ObjectType):
    # super-admin only
    create_domain = CreateDomain.Field()
    modify_domain = ModifyDomain.Field()
    delete_domain = DeleteDomain.Field()

    # admin only
    create_group = CreateGroup.Field()
    modify_group = ModifyGroup.Field()
    delete_group = DeleteGroup.Field()

    # admin only
    create_user = CreateUser.Field()
    modify_user = ModifyUser.Field()
    delete_user = DeleteUser.Field()

    # admin only
    create_keypair = CreateKeyPair.Field()
    modify_keypair = ModifyKeyPair.Field()
    delete_keypair = DeleteKeyPair.Field()

    # admin only
    rescan_images = RescanImages.Field()
    alias_image = AliasImage.Field()
    dealias_image = DealiasImage.Field()

    # super-admin only
    create_keypair_resource_policy = CreateKeyPairResourcePolicy.Field()
    modify_keypair_resource_policy = ModifyKeyPairResourcePolicy.Field()
    delete_keypair_resource_policy = DeleteKeyPairResourcePolicy.Field()

    # super-admin only
    create_resource_preset = CreateResourcePreset.Field()
    modify_resource_preset = ModifyResourcePreset.Field()
    delete_resource_preset = DeleteResourcePreset.Field()

    # super-admin only
    create_scaling_group = CreateScalingGroup.Field()
    modify_scaling_group = ModifyScalingGroup.Field()
    delete_scaling_group = DeleteScalingGroup.Field()
    associate_scaling_group_with_domain     = AssociateScalingGroupWithDomain.Field()
    associate_scaling_group_with_user_group = AssociateScalingGroupWithUserGroup.Field()
    associate_scaling_group_with_keypair    = AssociateScalingGroupWithKeyPair.Field()
    disassociate_scaling_group_with_domain     = DisassociateScalingGroupWithDomain.Field()
    disassociate_scaling_group_with_user_group = DisassociateScalingGroupWithUserGroup.Field()
    disassociate_scaling_group_with_keypair    = DisassociateScalingGroupWithKeyPair.Field()


class Queries(graphene.ObjectType):
    '''
    Available GraphQL queries for the admin privilege.
    It allows use of any access keys regardless of the one specified in the
    authorization header as well as querying the keypair information of all
    users.
    '''

    # super-admin only
    agent = graphene.Field(
        Agent,
        agent_id=graphene.String(required=True))

    # super-admin only
    agent_list = graphene.Field(
        AgentList,
        limit=graphene.Int(required=True),
        offset=graphene.Int(required=True),
        status=graphene.String())

    # super-admin only
    agents = graphene.List(
        Agent,
        status=graphene.String())

    domain = graphene.Field(
        Domain,
        name=graphene.String())

    # super-admin only
    domains = graphene.List(
        Domain,
        is_active=graphene.Boolean())

    group = graphene.Field(
        Group,
        id=graphene.String(required=True))

    groups = graphene.List(
        Group,
        domain_name=graphene.String(),
        all=graphene.Boolean(),
        is_active=graphene.Boolean())

    image = graphene.Field(
        Image,
        reference=graphene.String(required=True))

    images = graphene.List(
        Image,
    )

    user = graphene.Field(
        User,
        email=graphene.String())  # must be empty for user requests

    users = graphene.List(
        User,
        is_active=graphene.Boolean())

    keypair = graphene.Field(
        KeyPair,
        access_key=graphene.String())  # must be empty for user requests

    keypairs = graphene.List(
        KeyPair,
        user_id=graphene.String(),
        is_active=graphene.Boolean())

    keypair_resource_policy = graphene.Field(
        KeyPairResourcePolicy,
        name=graphene.String())

    keypair_resource_policies = graphene.List(
        KeyPairResourcePolicy)

    resource_preset = graphene.Field(
        ResourcePreset,
        name=graphene.String())

    resource_presets = graphene.List(
        ResourcePreset)

    # super-admin only
    scaling_group = graphene.Field(
        ScalingGroup,
        name=graphene.String())

    # super-admin only
    scaling_groups = graphene.List(
        ScalingGroup,
        name=graphene.String(),
        is_active=graphene.Boolean())

    vfolders = graphene.List(
        VirtualFolder,
        access_key=graphene.String())  # must be empty for user requests

    compute_session_list = graphene.Field(
        ComputeSessionList,
        limit=graphene.Int(required=True),
        offset=graphene.Int(required=True),
        access_key=graphene.String(),
        status=graphene.String(),
        group_id=graphene.String())

    compute_sessions = graphene.List(  # legacy non-paginated list
        ComputeSession,
        access_key=graphene.String(),
        status=graphene.String(),
        group_id=graphene.String())

    compute_session = graphene.Field(
        ComputeSession,
        sess_id=graphene.String())

    compute_workers = graphene.List(  # legacy non-paginated list
        ComputeWorker,
        sess_id=graphene.String(required=True),
        status=graphene.String())

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_agent(executor, info, agent_id):
        manager = info.context['dlmgr']
        loader = manager.get_loader('Agent', status=None)
        return await loader.load(agent_id)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_agents(executor, info, status=None):
        return await Agent.load_all(info.context, status=status)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_agent_list(executor, info, limit, offset, status=None):
        total_count = await Agent.load_count(info.context, status)
        agent_list = await Agent.load_slice(info.context, limit, offset, status)
        return AgentList(agent_list, total_count)

    @staticmethod
    async def resolve_domain(executor, info, name=None):
        manager = info.context['dlmgr']
        name = info.context['user']['domain_name'] if name is None else name
        if info.context['user']['role'] != UserRole.SUPERADMIN:
            if name != info.context['user']['domain_name']:
                # prevent querying other domains if not superadmin
                raise GenericNotFound('no such domain')
        loader = manager.get_loader('Domain.by_name')
        return await loader.load(name)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_domains(executor, info, is_active=None):
        return await Domain.load_all(info.context, is_active=is_active)

    @staticmethod
    async def resolve_group(executor, info, id):
        client_role = info.context['user']['role']
        client_domain_name = info.context['user']['domain_name']
        manager = info.context['dlmgr']
        loader = manager.get_loader('Group.by_id')
        group = await loader.load(id)
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            if group.domain_name != client_domain_name:
                raise GenericForbidden
        elif client_role == UserRole.USER:
            # TODO: check client is member of this group
            raise NotImplementedError
        else:
            raise InvalidAPIParameters('Unknown client role')
        return group

    @staticmethod
    async def resolve_groups(executor, info, domain_name=None, is_active=None, all=False):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            domain_name = client_domain if domain_name is None else domain_name
        elif client_role == UserRole.ADMIN:
            if domain_name is not None and domain_name != client_domain:
                raise GenericForbidden
            domain_name = client_domain
        elif client_role == UserRole.USER:
            # TODO: query groups that have the client as their member
            raise NotImplementedError
        else:
            raise InvalidAPIParameters('Unknown client role')
        return await Group.load_all(info.context, domain_name, is_active=is_active, all=all)

    @staticmethod
    async def resolve_image(executor, info, reference):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        item = await Image.load_item(info.context, reference)
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role in (UserRole.ADMIN, UserRole.USER):
            # TODO: filter only images from registries allowed for the current domain
            raise NotImplementedError
        else:
            raise InvalidAPIParameters('Unknown client role')
        return item

    @staticmethod
    async def resolve_images(executor, info):
        # TODO: filter only images from registries allowed for the current domain
        return await Image.load_all(info.context)

    @staticmethod
    async def resolve_user(executor, info, email=None):
        client_role = info.context['user']['role']
        client_email = info.context['user']['email']
        if client_role == UserRole.SUPERADMIN:
            if email is None:
                email = client_email
        elif client_role == UserRole.ADMIN:
            if email is not None:
                # TODO: check if the given email is in another domain.
                raise GenericForbidden
            email = client_email
        elif client_role == UserRole.USER:
            if email is not None and email != client_email:
                raise GenericForbidden
            email = client_email
        else:
            raise InvalidAPIParameters('Unknown client role')
        manager = info.context['dlmgr']
        loader = manager.get_loader('User.by_email')
        return await loader.load(email)

    @staticmethod
    async def resolve_users(executor, info, is_active=None):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            # TODO: filter only users in the client domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            raise GenericForbidden
        else:
            raise InvalidAPIParameters('Unknown client role')
        return await User.load_all(info.context, is_active=is_active)

    @staticmethod
    async def resolve_keypair(executor, info, access_key=None):
        manager = info.context['dlmgr']
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        if client_role == UserRole.SUPERADMIN:
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            # TODO: check if the keypair is in the current domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        loader = manager.get_loader('KeyPair.by_ak')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_keypairs(executor, info, user_id=None, is_active=None):
        manager = info.context['dlmgr']
        client_role = info.context['user']['role']
        client_user_id = info.context['user']['uuid']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            # TODO: filter only the keypairs in the current domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            if user_id is not None and user_id != client_user_id:
                raise GenericForbidden
            # Normal users can only query their own keypairs.
            user_id = client_user_id
        else:
            raise InvalidAPIParameters('Unknown client role')
        if user_id is None:
            return await KeyPair.load_all(info.context, is_active=is_active)
        else:
            loader = manager.get_loader('KeyPair.by_uid', is_active=is_active)
            return await loader.load(user_id)

    @staticmethod
    async def resolve_keypair_resource_policy(executor, info, name=None):
        manager = info.context['dlmgr']
        client_access_key = info.context['access_key']
        if name is None:
            loader = manager.get_loader('KeyPairResourcePolicy.by_ak')
            return await loader.load(client_access_key)
        else:
            loader = manager.get_loader('KeyPairResourcePolicy.by_name')
            return await loader.load(name)

    @staticmethod
    async def resolve_keypair_resource_policies(executor, info):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        if client_role == UserRole.SUPERADMIN:
            return await KeyPairResourcePolicy.load_all(info.context)
        elif client_role == UserRole.ADMIN:
            # TODO: filter resource policies by domains?
            return await KeyPairResourcePolicy.load_all(info.context)
        elif client_role == UserRole.USER:
            return await KeyPairResourcePolicy.load_all_user(info.context, client_access_key)
        else:
            raise InvalidAPIParameters('Unknown client role')

    @staticmethod
    async def resolve_resource_preset(executor, info, name):
        manager = info.context['dlmgr']
        loader = manager.get_loader('ResourcePreset.by_name')
        return await loader.load(name)

    @staticmethod
    async def resolve_resource_presets(executor, info):
        return await ResourcePreset.load_all(info.context)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_scaling_group(executor, info, name):
        manager = info.context['dlmgr']
        loader = manager.get_loader('ScalingGroup.by_name')
        return await loader.load(name)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_scaling_groups(executor, info, is_active=None):
        return await ScalingGroup.load_all(info.context, is_active=is_active)

    @staticmethod
    async def resolve_vfolders(executor, info, access_key=None):
        manager = info.context['dlmgr']
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        if client_role == UserRole.SUPERADMIN:
            # TODO: group vfolders?
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            # TODO: filter only vfolders in the client domain
            # TODO: group vfolders?
            raise NotImplementedError
        elif client_role == UserRole.USER:
            # TODO: filter only sessions in the client groups
            # TODO: group vfolders?
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        loader = manager.get_loader('VirtualFolder')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_session_list(executor, info, limit, offset,
                                           access_key=None, status=None,
                                           group_id=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        if client_role == UserRole.SUPERADMIN:
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            # TODO: filter only sessions in the client domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            # TODO: filter only sessions in the client groups
            # TODO: if group_id is given, check if the client is a member of it
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        total_count = await ComputeSession.load_count(
            info.context, access_key, status, group_id)
        items = await ComputeSession.load_slice(
            info.context, limit, offset, access_key, status, group_id)
        return ComputeSessionList(items, total_count)

    @staticmethod
    async def resolve_compute_sessions(executor, info, access_key=None,
                                       status=None, group_id=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        if client_role == UserRole.SUPERADMIN:
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            # TODO: filter only sessions in the client domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            # TODO: filter only sessions in the client groups
            # TODO: if group_id is given, check if the client is a member of it
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        # TODO: make status a proper graphene.Enum type
        #       (https://github.com/graphql-python/graphene/issues/544)
        if status is not None:
            status = KernelStatus[status]
        if access_key is not None:
            manager = info.context['dlmgr']
            loader = manager.get_loader('ComputeSession', status=status, group_id=group_id)
            return await loader.load(access_key)
        else:
            return await ComputeSession.load_all(info.context, status=status,
                                                 group_id=group_id)

    @staticmethod
    async def resolve_compute_session(executor, info, sess_id, status=None):
        client_role = info.context['user']['role']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            # TODO: check if session is in the client domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            # TODO: check if session is owned by the client
            pass
        else:
            raise InvalidAPIParameters('Unknown client role')
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession.detail', status=status)
        return await loader.load(sess_id)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id, status=None):
        client_role = info.context['user']['role']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            # TODO: check if session is in the client domain
            raise NotImplementedError
        elif client_role == UserRole.USER:
            # TODO: check if session is owned by the client
            pass
        else:
            raise InvalidAPIParameters('Unknown client role')
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeWorker', status=status)
        return await loader.load(sess_id)


async def init(app: web.Application):
    loop = asyncio.get_event_loop()
    app['admin.gql_executor'] = AsyncioExecutor(loop=loop)
    app['admin.gql_schema'] = graphene.Schema(
        query=Queries,
        mutation=Mutations,
        auto_camelcase=False)


async def shutdown(app: web.Application):
    pass


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (2, 3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('POST', r'/graphql', handle_gql))
    return app, []


if __name__ == '__main__':
    # If executed as a main program, print all GraphQL schemas.
    # (graphene transforms our object model into a textual representation)
    # This is useful for writing documentation!
    schema = graphene.Schema(
        query=Queries,
        mutation=Mutations,
        auto_camelcase=False)
    print('======== GraphQL API Schema ========')
    print(str(schema))
