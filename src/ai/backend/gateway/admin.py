import inspect
import logging
from typing import Any
import re

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
from .exceptions import GraphQLError as BackendGQLError
from .auth import auth_required
from .utils import check_api_params
from ..manager.models.base import DataLoaderManager
from ..manager.models.gql import (
    Mutations, Queries,
    GQLMutationPrivilegeCheckMiddleware,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.admin'))

_rx_qname = re.compile(r'{\s*(\w+)\b')


class GQLLoggingMiddleware:

    def resolve(self, next, root, info, **args):
        if len(info.path) == 1:
            log.info('ADMIN.GQL (ak:{}, {}:{}, op:{})',
                     info.context['access_key'],
                     info.operation.operation,
                     info.field_name,
                     info.operation.name)
        return next(root, info, **args)


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
        'config': request.app['config'],
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
        middleware=[
            GQLLoggingMiddleware(),
            GQLMutationUnfrozenRequiredMiddleware(),
            GQLMutationPrivilegeCheckMiddleware(),
        ],
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


async def init(app: web.Application):
    app['admin.gql_executor'] = AsyncioExecutor()
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
