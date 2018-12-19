import asyncio
import inspect
import json
import logging
import traceback
from typing import Mapping

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
from graphql.error.located_error import GraphQLLocatedError

from ai.backend.common.logging import BraceStyleAdapter

from .manager import GQLMutationUnfrozenRequiredMiddleware
from .exceptions import InvalidAPIParameters, BackendError
from .auth import auth_required
from ..manager.models.base import DataLoaderManager
from ..manager.models import (
    Agent, Image,
    KeyPair, CreateKeyPair, ModifyKeyPair, DeleteKeyPair,
    ComputeSession, ComputeWorker, KernelStatus,
    VirtualFolder,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.admin'))


@auth_required
@atomic
async def handle_gql(request: web.Request) -> web.Response:
    executor = request.app['admin.gql_executor']
    if request['is_admin']:
        schema = request.app['admin.gql_schema_admin']
    else:
        schema = request.app['admin.gql_schema_user']
    try:
        body = await request.json(loads=json.loads)
    except (asyncio.TimeoutError, json.decoder.JSONDecodeError):
        raise InvalidAPIParameters('Malformed request body.')
    try:
        assert 'query' in body, \
               'The request must have "query" JSON field.'
        assert isinstance(body['query'], str), \
               'The "query" field must be a JSON string.'
        if 'variables' in body:
            assert (body['variables'] is None or
                    isinstance(body['variables'], Mapping)), \
                   'The "variables" field must be an JSON object or null.'
        else:
            body['variables'] = None
    except AssertionError as e:
        raise InvalidAPIParameters(e.args[0])

    manager_status = await request.app['config_server'].get_manager_status()
    dlmanager = DataLoaderManager(request.app['dbpool'])
    result = schema.execute(
        body['query'], executor,
        variable_values=body['variables'],
        context_value={
            'dlmgr': dlmanager,
            'etcd': request.app['config_server'].etcd,
            'access_key': request['keypair']['access_key'],
            'dbpool': request.app['dbpool'],
            'redis_stat': request.app['redis_stat'],
            'manager_status': manager_status,
        },
        middleware=[GQLMutationUnfrozenRequiredMiddleware()],
        return_promise=True)
    if inspect.isawaitable(result):
        result = await result
    if result.errors:
        has_internal_errors = False
        for e in result.errors:
            if isinstance(e, GraphQLLocatedError):
                exc_info = (type(e.original_error),
                            e.original_error,
                            e.original_error.__traceback__)
                tb_text = ''.join(traceback.format_exception(*exc_info))
                log.error('GraphQL located error:\n{0}', tb_text)
                request.app['error_monitor'].capture_exception(exc_info)
                has_internal_errors = True
        if has_internal_errors:
            raise BackendError(str(result.errors[0]))
        raise InvalidAPIParameters(str(result.errors[0]))
    else:
        return web.json_response(result.data, status=200, dumps=json.dumps)


class MutationForAdmin(graphene.ObjectType):
    create_keypair = CreateKeyPair.Field()
    modify_keypair = ModifyKeyPair.Field()
    delete_keypair = DeleteKeyPair.Field()


# Nothing yet!
# class MutationForUser(graphene.ObjectType):
#     pass


class QueryForAdmin(graphene.ObjectType):
    '''
    Available GraphQL queries for the admin privilege.
    It allows use of any access keys regardless of the one specified in the
    authorization header as well as querying the keypair information of all
    users.
    '''

    agent = graphene.Field(
        Agent,
        agent_id=graphene.String())

    agents = graphene.List(
        Agent,
        status=graphene.String())

    images = graphene.List(
        Image,
    )

    keypair = graphene.Field(
        KeyPair,
        access_key=graphene.String())

    keypairs = graphene.List(
        KeyPair,
        user_id=graphene.String(),
        is_active=graphene.Boolean())

    vfolders = graphene.List(
        VirtualFolder,
        access_key=graphene.String())

    compute_sessions = graphene.List(
        ComputeSession,
        access_key=graphene.String(),
        status=graphene.String())

    compute_session = graphene.Field(
        ComputeSession,
        sess_id=graphene.String())

    compute_workers = graphene.List(
        ComputeWorker,
        sess_id=graphene.String(required=True),
        status=graphene.String())

    @staticmethod
    async def resolve_agent(executor, info, agent_id):
        manager = info.context['dlmgr']
        rs = info.context['redis_stat']
        loader = manager.get_loader('Agent', status=None)
        agent = await loader.load(agent_id)
        cpu_pct, mem_cur_bytes = await rs.hmget(
            str(agent.id),
            'cpu_pct', 'mem_cur_bytes',
        )
        agent.cpu_cur_pct = cpu_pct
        agent.mem_cur_bytes = mem_cur_bytes
        return agent

    @staticmethod
    async def resolve_agents(executor, info, status=None):
        dbpool = info.context['dbpool']
        rs = info.context['redis_stat']
        agent_list = await Agent.load_all(dbpool, status=status)
        for agent in agent_list:
            cpu_pct, mem_cur_bytes = await rs.hmget(
                str(agent.id),
                'cpu_pct', 'mem_cur_bytes',
            )
            agent.cpu_cur_pct = cpu_pct
            agent.mem_cur_bytes = mem_cur_bytes
        return agent_list

    @staticmethod
    async def resolve_images(executor, info):
        etcd = info.context['etcd']
        return await Image.load_all(etcd)

    @staticmethod
    async def resolve_keypair(executor, info, access_key=None):
        manager = info.context['dlmgr']
        if access_key is None:
            access_key = info.context['access_key']
        loader = manager.get_loader('KeyPair.by_ak')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_keypairs(executor, info, user_id=None, is_active=None):
        manager = info.context['dlmgr']
        dbpool = info.context['dbpool']
        if user_id is None:
            return await KeyPair.load_all(dbpool, is_active=is_active)
        else:
            loader = manager.get_loader('KeyPair.by_uid', is_active=is_active)
            return await loader.load(user_id)

    @staticmethod
    async def resolve_vfolders(executor, info, access_key=None):
        manager = info.context['dlmgr']
        if access_key is None:
            access_key = info.context['access_key']
        loader = manager.get_loader('VirtualFolder')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_sessions(executor, info, access_key=None, status=None):
        manager = info.context['dlmgr']
        # TODO: make status a proper graphene.Enum type
        #       (https://github.com/graphql-python/graphene/issues/544)
        if access_key is None:
            access_key = info.context['access_key']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession', status=status)
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_session(executor, info, sess_id, status=None):
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession.detail', status=status)
        return await loader.load(sess_id)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id, status=None):
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeWorker', status=status)
        return await loader.load(sess_id)


class QueryForUser(graphene.ObjectType):
    '''
    Available GraphQL queries for the user priveilege.
    It only allows use of the access key specified in the authorization header.
    '''

    images = graphene.List(
        Image,
    )

    keypair = graphene.Field(lambda: KeyPair)

    vfolders = graphene.List(VirtualFolder)

    compute_sessions = graphene.List(
        ComputeSession,
        status=graphene.String())

    compute_session = graphene.Field(
        ComputeSession,
        sess_id=graphene.String())

    compute_workers = graphene.List(
        ComputeWorker,
        sess_id=graphene.String(required=True),
        status=graphene.String())

    @staticmethod
    async def resolve_images(executor, info):
        etcd = info.context['etcd']
        return await Image.load_all(etcd)

    @staticmethod
    async def resolve_keypair(executor, info):
        manager = info.context['dlmgr']
        access_key = info.context['access_key']
        loader = manager.get_loader('KeyPair.by_ak')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_vfolders(executor, info):
        manager = info.context['dlmgr']
        access_key = info.context['access_key']
        loader = manager.get_loader('VirtualFolder')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_sessions(executor, info, status=None):
        manager = info.context['dlmgr']
        access_key = info.context['access_key']
        # TODO: make status a proper graphene.Enum type
        #       (https://github.com/graphql-python/graphene/issues/544)
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession', status=status)
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_session(executor, info, sess_id, status=None):
        manager = info.context['dlmgr']
        access_key = info.context['access_key']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession.detail', access_key=access_key,
                                    status=status)
        return await loader.load(sess_id)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id, status=None):
        manager = info.context['dlmgr']
        access_key = info.context['access_key']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader(
            'ComputeWorker', status=status, access_key=access_key)
        return await loader.load(sess_id)


async def init(app):
    loop = asyncio.get_event_loop()
    app['admin.gql_executor'] = AsyncioExecutor(loop=loop)
    app['admin.gql_schema_admin'] = graphene.Schema(
        query=QueryForAdmin,
        mutation=MutationForAdmin,
        auto_camelcase=False)
    app['admin.gql_schema_user'] = graphene.Schema(
        query=QueryForUser,
        mutation=None,
        auto_camelcase=False)


async def shutdown(app):
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
    admin_schema = graphene.Schema(
        query=QueryForAdmin,
        mutation=MutationForAdmin,
        auto_camelcase=False)
    user_schema = graphene.Schema(
        query=QueryForUser,
        mutation=None,
        auto_camelcase=False)
    print('======== Admin Schema ========')
    print(str(admin_schema))
    print('======== User Schema ========')
    print(str(user_schema))
