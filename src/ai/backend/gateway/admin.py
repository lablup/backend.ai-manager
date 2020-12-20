import inspect
import logging
from typing import (
    Any,
    Iterable,
    Tuple,
)
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
from .types import CORSOptions, WebMiddleware
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
    manager_status = await request.app['shared_config'].get_manager_status()
    known_slot_types = await request.app['shared_config'].get_resource_slots()
    context = {
        'local_config': request.app['local_config'],
        'shared_config': request.app['shared_config'],
        'etcd': request.app['shared_config'].etcd,
        'user': request['user'],
        'access_key': request['keypair']['access_key'],
        'dbpool': request.app['dbpool'],
        'redis_stat': request.app['redis_stat'],
        'manager_status': manager_status,
        'known_slot_types': known_slot_types,
        'background_task_manager': request.app['background_task_manager'],
        'storage_manager': request.app['storage_manager'],
        'registry': request.app['registry'],
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
                errmsg = format_error(e)
                errors.append(errmsg)
            else:
                errmsg = {'message': str(e)}
                errors.append(errmsg)
            log.error('ADMIN.GQL Exception: {}', errmsg)
        raise BackendGQLError(extra_data=errors)
    return web.json_response(result.data, status=200)


async def init(app: web.Application) -> None:
    app['admin.gql_executor'] = AsyncioExecutor()
    app['admin.gql_schema'] = graphene.Schema(
        query=Queries,
        mutation=Mutations,
        auto_camelcase=False)


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
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
