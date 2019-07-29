import graphene

from .base import privileged_query
from .agent import (
    Agent, AgentList,
)
from .domain import (
    Domain,
    CreateDomain, ModifyDomain, DeleteDomain,
)
from .group import (
    Group,
    CreateGroup, ModifyGroup, DeleteGroup,
)
from .image import (
    Image,
    RescanImages, AliasImage, DealiasImage,
)
from .kernel import (
    ComputeSession, ComputeSessionList,
    ComputeWorker, KernelStatus,
)
from .keypair import (
    KeyPair,
    CreateKeyPair, ModifyKeyPair, DeleteKeyPair,
)
from .resource_policy import (
    KeyPairResourcePolicy,
    CreateKeyPairResourcePolicy, ModifyKeyPairResourcePolicy, DeleteKeyPairResourcePolicy,
)
from .resource_preset import (
    ResourcePreset,
    CreateResourcePreset, ModifyResourcePreset, DeleteResourcePreset,
)
from .scaling_group import (
    ScalingGroup,
    CreateScalingGroup, ModifyScalingGroup, DeleteScalingGroup,
    AssociateScalingGroupWithDomain,    DisassociateScalingGroupWithDomain,
    AssociateScalingGroupWithUserGroup, DisassociateScalingGroupWithUserGroup,
    AssociateScalingGroupWithKeyPair,   DisassociateScalingGroupWithKeyPair,
)
from .user import (
    User, UserRole,
    CreateUser, ModifyUser, DeleteUser,
)
from .vfolder import (
    VirtualFolder,
)
from ...gateway.exceptions import (
    GenericNotFound,
    GenericForbidden,
    InvalidAPIParameters,
)


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

    # super-admin only
    scaling_groups_for_domain = graphene.List(
        ScalingGroup,
        domain=graphene.String(required=True),
        is_active=graphene.Boolean())

    # super-admin only
    scaling_groups_for_user_group = graphene.List(
        ScalingGroup,
        user_group=graphene.String(required=True),
        is_active=graphene.Boolean())

    # super-admin only
    scaling_groups_for_keypair = graphene.List(
        ScalingGroup,
        access_key=graphene.String(required=True),
        is_active=graphene.Boolean())

    vfolders = graphene.List(
        VirtualFolder,
        access_key=graphene.String())  # must be empty for user requests

    compute_session_list = graphene.Field(
        ComputeSessionList,
        limit=graphene.Int(required=True),
        offset=graphene.Int(required=True),
        domain_name=graphene.String(),
        group_id=graphene.String(),
        access_key=graphene.String(),
        status=graphene.String(),
    )

    compute_sessions = graphene.List(  # legacy non-paginated list
        ComputeSession,
        domain_name=graphene.String(),
        group_id=graphene.String(),
        access_key=graphene.String(),
        status=graphene.String(),
    )

    compute_session = graphene.Field(
        ComputeSession,
        sess_id=graphene.String(required=True),
        domain_name=graphene.String(),
        group_id=graphene.String(),
    )

    compute_workers = graphene.List(  # legacy non-paginated list
        ComputeWorker,
        sess_id=graphene.String(required=True),
        domain_name=graphene.String(),
        group_id=graphene.String(),
        status=graphene.String(),
    )

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
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_scaling_groups_for_domain(executor, info, domain, is_active=None):
        return await ScalingGroup.load_by_domain(
            info.context, domain, is_active=is_active)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_scaling_groups_for_group(executor, info, user_group, is_active=None):
        return await ScalingGroup.load_by_group(
            info.context, user_group, is_active=is_active)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_scaling_groups_for_keypair(executor, info, access_key, is_active=None):
        return await ScalingGroup.load_by_keypair(
            info.context, access_key, is_active=is_active)

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
                                           domain_name=None, access_key=None, status=None,
                                           group_id=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            if domain_name is None:
                domain_name = client_domain
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            if domain_name is not None and domain_name != client_domain:
                raise GenericForbidden
            domain_name = client_domain
            if group_id is not None:
                # TODO: check if the group is a member of the domain
                pass
            if access_key is not None:
                # TODO: check if the client is a member of the domain
                pass
        elif client_role == UserRole.USER:
            if domain_name is not None and domain_name != client_domain:
                raise GenericForbidden
            domain_name = client_domain
            if group_id is not None:
                # TODO: check if the group is a member of the domain
                # TODO: check if the client is a member of the group
                pass
            if access_key is not None:
                # TODO: check if the client is a member of the domain
                pass
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        total_count = await ComputeSession.load_count(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            access_key=access_key,
            status=status)
        items = await ComputeSession.load_slice(
            info.context, limit, offset,
            domain_name=domain_name,
            group_id=group_id,
            access_key=access_key,
            status=status)
        return ComputeSessionList(items, total_count)

    @staticmethod
    async def resolve_compute_sessions(executor, info,
                                       domain_name=None, group_id=None, access_key=None,
                                       status=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            if domain_name is None:
                domain_name = client_domain
            if access_key is None:
                access_key = client_access_key
        elif client_role == UserRole.ADMIN:
            if domain_name is not None and domain_name != client_domain:
                raise GenericForbidden
            domain_name = client_domain
            if group_id is not None:
                # TODO: check if the group is a member of the domain
                pass
            if access_key is not None:
                # TODO: check if the client is a member of the domain
                pass
        elif client_role == UserRole.USER:
            if domain_name is not None and domain_name != client_domain:
                raise GenericForbidden
            domain_name = client_domain
            if group_id is not None:
                # TODO: check if the group is a member of the domain
                # TODO: check if the client is a member of the group
                pass
            if access_key is not None:
                # TODO: check if the client is a member of the domain
                pass
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        # TODO: make status a proper graphene.Enum type
        #       (https://github.com/graphql-python/graphene/issues/544)
        if status is not None:
            status = KernelStatus[status]
        return await ComputeSession.load_all(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            access_key=access_key,
            status=status)

    @staticmethod
    async def resolve_compute_session(executor, info, sess_id,
                                      domain_name=None, access_key=None,
                                      status=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        client_domain = info.context['user']['domain_name']
        # We need to check the group membership of the designated kernel,
        # but practically a user cannot guess the IDs of kernels launched
        # by other users and in other groups.
        # Let's just protect the domain/user boundary here.
        if client_role == UserRole.SUPERADMIN:
            domain_name = None
        elif client_role == UserRole.ADMIN:
            domain_name = client_domain
        elif client_role == UserRole.USER:
            domain_name = client_domain
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader(
            'ComputeSession.detail',
            domain_name=domain_name,
            access_key=access_key,
            status=status)
        return await loader.load(sess_id)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id,
                                      domain_name=None, access_key=None,
                                      status=None):
        client_role = info.context['user']['role']
        client_access_key = info.context['access_key']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            domain_name = None
        elif client_role == UserRole.ADMIN:
            domain_name = client_domain
        elif client_role == UserRole.USER:
            domain_name = client_domain
            if access_key is not None and access_key != client_access_key:
                raise GenericForbidden
            access_key = client_access_key
        else:
            raise InvalidAPIParameters('Unknown client role')
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader(
            'ComputeWorker',
            domain_name=domain_name,
            access_key=access_key,
            status=status)
        return await loader.load(sess_id)
