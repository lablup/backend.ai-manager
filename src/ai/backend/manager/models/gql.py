import graphene

from .base import privileged_query, scoped_query
from .agent import (
    Agent, AgentList,
)
from .domain import (
    Domain,
    CreateDomain, ModifyDomain, DeleteDomain, PurgeDomain,
)
from .group import (
    Group,
    CreateGroup, ModifyGroup, DeleteGroup, PurgeGroup,
)
from .image import (
    Image,
    RescanImages, ForgetImage, AliasImage, DealiasImage,
)
from .kernel import (
    ComputeSession, ComputeSessionList,
    ComputeWorker,
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
    DisassociateAllScalingGroupsWithDomain,
    AssociateScalingGroupWithUserGroup, DisassociateScalingGroupWithUserGroup,
    DisassociateAllScalingGroupsWithGroup,
    AssociateScalingGroupWithKeyPair,   DisassociateScalingGroupWithKeyPair,
)
from .user import (
    User,
    CreateUser, ModifyUser, DeleteUser, PurgeUser,
    UserRole,
)
from .vfolder import (
    VirtualFolder, VirtualFolderList,
)
from ...gateway.exceptions import (
    GenericNotFound,
    ImageNotFound,
    InsufficientPrivilege,
    InvalidAPIParameters,
    TooManyKernelsFound,
)


class Mutations(graphene.ObjectType):
    '''
    All available GraphQL mutations.
    '''

    # super-admin only
    create_domain = CreateDomain.Field()
    modify_domain = ModifyDomain.Field()
    delete_domain = DeleteDomain.Field()
    purge_domain = PurgeDomain.Field()

    # admin only
    create_group = CreateGroup.Field()
    modify_group = ModifyGroup.Field()
    delete_group = DeleteGroup.Field()
    purge_group = PurgeGroup.Field()

    # super-admin only
    create_user = CreateUser.Field()
    modify_user = ModifyUser.Field()
    delete_user = DeleteUser.Field()
    purge_user = PurgeUser.Field()

    # admin only
    create_keypair = CreateKeyPair.Field()
    modify_keypair = ModifyKeyPair.Field()
    delete_keypair = DeleteKeyPair.Field()

    # admin only
    rescan_images = RescanImages.Field()
    forget_image = ForgetImage.Field()
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
    disassociate_all_scaling_groups_with_domain = DisassociateAllScalingGroupsWithDomain.Field()
    disassociate_all_scaling_groups_with_group = DisassociateAllScalingGroupsWithGroup.Field()


class Queries(graphene.ObjectType):
    '''
    All available GraphQL queries.
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
        # ordering customization
        order_key=graphene.String(),
        order_asc=graphene.Boolean(),
        # filters
        scaling_group=graphene.String(),
        status=graphene.String(),
    )

    # super-admin only
    agents = graphene.List(  # legacy non-paginated list
        Agent,
        scaling_group=graphene.String(),
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
        is_active=graphene.Boolean())

    image = graphene.Field(
        Image,
        reference=graphene.String(required=True))

    images = graphene.List(
        Image,
        is_installed=graphene.Boolean(),
        is_operation=graphene.Boolean(),
    )

    user = graphene.Field(
        User,
        domain_name=graphene.String(),
        email=graphene.String())

    user_from_uuid = graphene.Field(
        User,
        domain_name=graphene.String(),
        user_id=graphene.String())

    users = graphene.List(
        User,
        domain_name=graphene.String(),
        group_id=graphene.String(),
        is_active=graphene.Boolean())

    keypair = graphene.Field(
        KeyPair,
        domain_name=graphene.String(),
        access_key=graphene.String())

    keypairs = graphene.List(
        KeyPair,
        domain_name=graphene.String(),
        email=graphene.String(),
        is_active=graphene.Boolean())

    # NOTE: maybe add keypairs_from_user_id?

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

    vfolder_list = graphene.Field(  # legacy non-paginated list
        VirtualFolderList,
        limit=graphene.Int(required=True),
        offset=graphene.Int(required=True),
        # ordering customization
        order_key=graphene.String(),
        order_asc=graphene.Boolean(),
        # filters
        domain_name=graphene.String(),
        group_id=graphene.String(),
        access_key=graphene.String())  # must be empty for user requests

    vfolders = graphene.List(  # legacy non-paginated list
        VirtualFolder,
        domain_name=graphene.String(),
        group_id=graphene.String(),
        access_key=graphene.String())  # must be empty for user requests

    compute_session_list = graphene.Field(
        ComputeSessionList,
        limit=graphene.Int(required=True),
        offset=graphene.Int(required=True),
        # ordering customization
        order_key=graphene.String(),
        order_asc=graphene.Boolean(),
        # filters
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
        access_key=graphene.String(),
    )

    compute_workers = graphene.List(  # legacy non-paginated list
        ComputeWorker,
        sess_id=graphene.String(required=True),
        domain_name=graphene.String(),
        access_key=graphene.String(),
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
    async def resolve_agents(executor, info, *,
                             scaling_group=None,
                             status=None):
        return await Agent.load_all(
            info.context,
            scaling_group=scaling_group,
            status=status)

    @staticmethod
    @privileged_query(UserRole.SUPERADMIN)
    async def resolve_agent_list(executor, info, limit, offset, *,
                                 scaling_group=None,
                                 status=None,
                                 order_key=None, order_asc=None):
        total_count = await Agent.load_count(
            info.context,
            scaling_group=scaling_group,
            status=status)
        agent_list = await Agent.load_slice(
            info.context, limit, offset,
            scaling_group=scaling_group,
            status=status,
            order_key=order_key,
            order_asc=order_asc)
        return AgentList(agent_list, total_count)

    @staticmethod
    async def resolve_domain(executor, info, *, name=None):
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
    async def resolve_domains(executor, info, *, is_active=None):
        return await Domain.load_all(info.context, is_active=is_active)

    @staticmethod
    async def resolve_group(executor, info, id):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        client_user_id = info.context['user']['uuid']
        manager = info.context['dlmgr']
        loader = manager.get_loader('Group.by_id')
        group = await loader.load(id)
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            if group.domain_name != client_domain:
                raise InsufficientPrivilege
        elif client_role == UserRole.USER:
            client_groups = await Group.get_groups_for_user(info.context, client_user_id)
            if group.id not in (g.id for g in client_groups):
                raise InsufficientPrivilege
        else:
            raise InvalidAPIParameters('Unknown client role')
        return group

    @staticmethod
    async def resolve_groups(executor, info, *, domain_name=None, is_active=None):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        client_user_id = info.context['user']['uuid']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            if domain_name is not None and domain_name != client_domain:
                raise InsufficientPrivilege
            domain_name = client_domain
        elif client_role == UserRole.USER:
            return await Group.get_groups_for_user(info.context, client_user_id)
        else:
            raise InvalidAPIParameters('Unknown client role')
        return await Group.load_all(
            info.context,
            domain_name=domain_name,
            is_active=is_active)

    @staticmethod
    async def resolve_image(executor, info, reference):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        item = await Image.load_item(info.context, reference)
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role in (UserRole.ADMIN, UserRole.USER):
            items = await Image.filter_allowed(info.context, [item], client_domain)
            if not items:
                raise ImageNotFound
            item = items[0]
        else:
            raise InvalidAPIParameters('Unknown client role')
        return item

    @staticmethod
    async def resolve_images(executor, info, is_installed=None, is_operation=False):
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        items = await Image.load_all(info.context,
                                     is_installed=is_installed,
                                     is_operation=is_operation)
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role in (UserRole.ADMIN, UserRole.USER):
            items = await Image.filter_allowed(
                info.context, items, client_domain,
                is_installed=is_installed, is_operation=is_operation)
        else:
            raise InvalidAPIParameters('Unknown client role')
        return items

    @staticmethod
    @scoped_query(autofill_user=True, user_key='email')
    async def resolve_user(executor, info, *,
                           domain_name=None, email=None):
        manager = info.context['dlmgr']
        loader = manager.get_loader('User.by_email', domain_name=domain_name)
        return await loader.load(email)

    @staticmethod
    @scoped_query(autofill_user=True, user_key='user_id')
    async def resolve_user_from_uuid(executor, info, *,
                                     domain_name=None, user_id=None):
        manager = info.context['dlmgr']
        loader = manager.get_loader('User.by_uuid', domain_name=domain_name)
        return await loader.load(user_id)

    @staticmethod
    async def resolve_users(executor, info, *,
                            domain_name=None, group_id=None,
                            is_active=None):
        from .user import UserRole
        client_role = info.context['user']['role']
        client_domain = info.context['user']['domain_name']
        if client_role == UserRole.SUPERADMIN:
            pass
        elif client_role == UserRole.ADMIN:
            if domain_name is not None and domain_name != client_domain:
                raise InsufficientPrivilege
            domain_name = client_domain
        elif client_role == UserRole.USER:
            # Users cannot query other users.
            raise InsufficientPrivilege()
        else:
            raise InvalidAPIParameters('Unknown client role')
        return await User.load_all(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            is_active=is_active)

    @staticmethod
    @scoped_query(autofill_user=True, user_key='access_key')
    async def resolve_keypair(executor, info, *,
                              domain_name=None, access_key=None):
        manager = info.context['dlmgr']
        loader = manager.get_loader('KeyPair.by_ak', domain_name=domain_name)
        return await loader.load(access_key)

    @staticmethod
    @scoped_query(autofill_user=False, user_key='email')
    async def resolve_keypairs(executor, info, *,
                               domain_name=None, email=None,
                               is_active=None):
        # Here, user_id corresponds to user email.
        # fetch keypairs from each user_id
        if email is None:
            return await KeyPair.load_all(
                info.context,
                domain_name=domain_name,
                is_active=is_active)
        else:
            manager = info.context['dlmgr']
            loader = manager.get_loader('KeyPair.by_email',
                                        domain_name=domain_name,
                                        is_active=is_active)
            return await loader.load(email)

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
    @scoped_query(autofill_user=False, user_key='user_id')
    async def resolve_vfolder_list(executor, info, limit, offset, *,
                                   domain_name=None, group_id=None, user_id=None,
                                   order_key=None, order_asc=None):
        total_count = await VirtualFolder.load_count(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            user_id=user_id)
        items = await VirtualFolder.load_slice(
            info.context, limit, offset,
            domain_name=domain_name,
            group_id=group_id,
            user_id=user_id,
            order_key=order_key,
            order_asc=order_asc)
        return VirtualFolderList(items, total_count)

    @staticmethod
    @scoped_query(autofill_user=False, user_key='user_id')
    async def resolve_vfolders(executor, info, *,
                               domain_name=None, group_id=None, user_id=None):
        return await VirtualFolder.load_all(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            user_id=user_id)

    @staticmethod
    @scoped_query(autofill_user=False, user_key='access_key')
    async def resolve_compute_session_list(executor, info, limit, offset, *,
                                           domain_name=None, group_id=None, access_key=None,
                                           status=None,
                                           order_key=None, order_asc=None):
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
            status=status,
            order_key=order_key,
            order_asc=order_asc)
        return ComputeSessionList(items, total_count)

    @staticmethod
    @scoped_query(autofill_user=False, user_key='access_key')
    async def resolve_compute_sessions(executor, info, *,
                                       domain_name=None, group_id=None, access_key=None,
                                       status=None):
        return await ComputeSession.load_all(
            info.context,
            domain_name=domain_name,
            group_id=group_id,
            access_key=access_key,
            status=status)

    @staticmethod
    @scoped_query(autofill_user=False, user_key='access_key')
    async def resolve_compute_session(executor, info, sess_id, *,
                                      domain_name=None, access_key=None,
                                      status=None):
        # We need to check the group membership of the designated kernel,
        # but practically a user cannot guess the IDs of kernels launched
        # by other users and in other groups.
        # Let's just protect the domain/user boundary here.
        manager = info.context['dlmgr']
        loader = manager.get_loader(
            'ComputeSession.detail',
            domain_name=domain_name,
            access_key=access_key,
            status=status)
        matches = await loader.load(sess_id)
        if len(matches) == 0:
            return None
        elif len(matches) == 1:
            return matches[0]
        else:
            raise TooManyKernelsFound

    @staticmethod
    @scoped_query(autofill_user=False, user_key='access_key')
    async def resolve_compute_workers(executor, info, sess_id, *,
                                      domain_name=None, access_key=None,
                                      status=None):
        manager = info.context['dlmgr']
        loader = manager.get_loader(
            'ComputeWorker',
            domain_name=domain_name,
            access_key=access_key,
            status=status)
        return await loader.load(sess_id)


class GQLMutationPrivilegeCheckMiddleware:

    def resolve(self, next, root, info, **args):
        if info.operation.operation == 'mutation' and len(info.path) == 1:
            mutation_cls = getattr(Mutations, info.path[0]).type
            # default is allow nobody.
            allowed_roles = getattr(mutation_cls, 'allowed_roles', [])
            if info.context['user']['role'] not in allowed_roles:
                return mutation_cls(False, f"no permission to execute {info.path[0]}")
        return next(root, info, **args)
