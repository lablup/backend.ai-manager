import asyncio
import base64
import functools
import inspect
import logging
import secrets
import typing

from aiohttp import web
import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
from graphql.error.located_error import GraphQLLocatedError
import simplejson as json
import sqlalchemy as sa

from .exceptions import InvalidAPIParameters, SornaError
from ..manager.models.base import DataLoaderManager
from ..manager.models import (
    KeyPair, CreateKeyPair, ModifyKeyPair, DeleteKeyPair,
    ComputeSession, ComputeWorker, KernelStatus,
    VirtualFolder,
)

log = logging.getLogger('sorna.gateway.admin')


#@auth_required
async def handle_gql(request):
    executor = request.app['admin.gql_executor']
    schema = request.app['admin.gql_schema']
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
                    isinstance(body['variables'], typing.Mapping)), \
                   'The "variables" field must be an JSON object or null.'
        else:
            body['variables'] = None
    except AssertionError as e:
        raise InvalidAPIParameters(e.args[0])
    text = await request.text()
    log.debug(f'handle_gql: processing request\n{text}')
    dlmanager = DataLoaderManager(request.app['dbpool'])
    result = schema.execute(
        body['query'], executor,
        variable_values=body['variables'],
        context_value={
            'dlmgr': dlmanager,
            'dbpool': request.app['dbpool'],
        },
        return_promise=True)
    if inspect.isawaitable(result):
        result = await result
    if result.errors:
        has_internal_errors = False
        for e in result.errors:
            if isinstance(e, GraphQLLocatedError):
                exc_info = (type(e.original_error),
                            e.original_error.args,
                            e.original_error.__traceback__)
                request.app['sentry'].captureException(exc_info)
                has_internal_errors = True
        if has_internal_errors:
            raise SornaError(str(result.errors[0]))
        raise InvalidAPIParameters(str(result.errors[0]))
    else:
        return web.json_response(result.data, status=200, dumps=json.dumps)


class Mutation(graphene.ObjectType):
    create_keypair = CreateKeyPair.Field()
    modify_keypair = ModifyKeyPair.Field()
    delete_keypair = DeleteKeyPair.Field()


class Query(graphene.ObjectType):

    keypairs = graphene.List(KeyPair,
        user_id=graphene.Int(required=True),
        is_active=graphene.Boolean())

    vfolders = graphene.List(VirtualFolder,
        access_key=graphene.String(required=True))

    compute_sessions = graphene.List(ComputeSession,
        access_key=graphene.String(required=True),
        status=graphene.String())

    compute_workers = graphene.List(ComputeWorker,
        sess_id=graphene.String(required=True),
        status=graphene.String())

    @staticmethod
    async def resolve_keypairs(executor, info, user_id, is_active=None):
        manager = info.context['dlmgr']
        loader = manager.get_loader('KeyPair', is_active=is_active)
        return await loader.load(user_id)

    @staticmethod
    async def resolve_vfolders(executor, info, access_key):
        manager = info.context['dlmgr']
        loader = manager.get_loader('VirtualFolder')
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_sessions(executor, info, access_key, status=None):
        manager = info.context['dlmgr']
        # TODO: make status a proper graphene.Enum type
        #       (https://github.com/graphql-python/graphene/issues/544)
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeSession', status=status)
        return await loader.load(access_key)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id, status=None):
        manager = info.context['dlmgr']
        if status is not None:
            status = KernelStatus[status]
        loader = manager.get_loader('ComputeWorker', status=status)
        return await loader.load(sess_id)


async def init(app):
    loop = asyncio.get_event_loop()
    app.router.add_route('POST', r'/v{version:\d+}/admin/graphql', handle_gql)
    app['admin.gql_executor'] = AsyncioExecutor(loop=loop)
    app['admin.gql_schema'] = graphene.Schema(
        query=Query,
        mutation=Mutation,
        auto_camelcase=False)


async def shutdown(app):
    pass
