from __future__ import annotations

import inspect
import logging
import re
from typing import (
    Any,
    Iterable,
    TYPE_CHECKING,
    Tuple,
)

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import attr
import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
from graphql.error import GraphQLError, format_error
import trafaret as t

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common import validators as tx

from ai.backend.manager.models.utils import execute_with_retry

from ..models.base import DataLoaderManager
from ..models.gql import (
    Mutations, Queries,
    GraphQueryContext,
    GQLMutationPrivilegeCheckMiddleware,
)
from .manager import GQLMutationUnfrozenRequiredMiddleware
from .exceptions import GraphQLError as BackendGQLError
from .auth import auth_required
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params
if TYPE_CHECKING:
    from .context import RootContext

log = BraceStyleAdapter(logging.getLogger(__name__))

_rx_mutation_hdr = re.compile(r"^mutation(\s+\w+)?\s*(\(|{|@)", re.M)


class GQLLoggingMiddleware:

    def resolve(self, next, root, info: graphene.ResolveInfo, **args) -> Any:
        graph_ctx: GraphQueryContext = info.context
        if len(info.path) == 1:
            log.info('ADMIN.GQL (ak:{}, {}:{}, op:{})',
                     graph_ctx.access_key,
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
    root_ctx: RootContext = request.app['_root.context']
    app_ctx: PrivateContext = request.app['admin.context']
    manager_status = await root_ctx.shared_config.get_manager_status()
    known_slot_types = await root_ctx.shared_config.get_resource_slots()

    async def _process() -> Any:
        async with root_ctx.db.connect() as db_conn:
            # A simple heuristic to detect read-only queries
            # ref) https://spec.graphql.org/draft/#sec-Document-Syntax
            if _rx_mutation_hdr.search(params['query']) is None:
                await db_conn.execution_options(postgresql_readonly=True)
            async with db_conn.begin():
                gql_ctx = GraphQueryContext(
                    dataloader_manager=DataLoaderManager(),
                    local_config=root_ctx.local_config,
                    shared_config=root_ctx.shared_config,
                    etcd=root_ctx.shared_config.etcd,
                    user=request['user'],
                    access_key=request['keypair']['access_key'],
                    db=root_ctx.db,
                    db_conn=db_conn,
                    redis_stat=root_ctx.redis_stat,
                    redis_image=root_ctx.redis_image,
                    manager_status=manager_status,
                    known_slot_types=known_slot_types,
                    background_task_manager=root_ctx.background_task_manager,
                    storage_manager=root_ctx.storage_manager,
                    registry=root_ctx.registry,
                )
                result = app_ctx.gql_schema.execute(
                    params['query'],
                    app_ctx.gql_executor,
                    variable_values=params['variables'],
                    operation_name=params['operation_name'],
                    context_value=gql_ctx,
                    middleware=[
                        GQLLoggingMiddleware(),
                        GQLMutationUnfrozenRequiredMiddleware(),
                        GQLMutationPrivilegeCheckMiddleware(),
                    ],
                    return_promise=True)
                if inspect.isawaitable(result):
                    result = await result
                return result

    result = await execute_with_retry(_process)
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


@attr.s(auto_attribs=True, slots=True, init=False)
class PrivateContext:
    gql_executor: AsyncioExecutor
    gql_schema: graphene.Schema


async def init(app: web.Application) -> None:
    app_ctx: PrivateContext = app['admin.context']
    app_ctx.gql_executor = AsyncioExecutor()
    app_ctx.gql_schema = graphene.Schema(
        query=Queries,
        mutation=Mutations,
        auto_camelcase=False,
    )


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (2, 3, 4)
    app['admin.context'] = PrivateContext()
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
