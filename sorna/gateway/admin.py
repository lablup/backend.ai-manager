import asyncio
import base64
import functools
import inspect
import logging
import secrets
import typing

from aiohttp import web
from aiotools import apartial
from aiodataloader import DataLoader
import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
from graphql.error.located_error import GraphQLLocatedError
import simplejson as json
import sqlalchemy as sa

from .exceptions import InvalidAPIParameters, SornaError
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
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        vfloader = DataLoader(apartial(VirtualFolder.batch_load, conn))
        csloader = DataLoader(apartial(ComputeSession.batch_load, conn))
        cwloader = DataLoader(apartial(ComputeWorker.batch_load, conn))
        # TODO: better way to distinguish differently filtered dataloaders?
        kploader = DataLoader(apartial(KeyPair.batch_load, conn, is_active=None))
        kpiloader = DataLoader(apartial(KeyPair.batch_load, conn, is_active=False))
        kpaloader = DataLoader(apartial(KeyPair.batch_load, conn, is_active=True))
        result = schema.execute(
            body['query'], executor,
            variable_values=body['variables'],
            context_value={
                'conn': conn,
                'vfloader': vfloader,
                'csloader': csloader,
                'cwloader': cwloader,
                'kploader': kploader,
                'kpiloader': kpiloader,
                'kpaloader': kpaloader,
            },
            return_promise=True)
        if inspect.isawaitable(result):
            result = await result
    if result.errors:
        has_internal_errors = any(
            isinstance(e, GraphQLLocatedError)
            for e in result.errors
        )
        if has_internal_errors:
            raise SornaError(result.errors[0].args[0])
        raise InvalidAPIParameters(result.errors[0].args[0])
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
        )#status=graphene.Enum.from_enum(KernelStatus))
    compute_workers = graphene.List(ComputeWorker,
        sess_id=graphene.String(required=True),
        )#status=graphene.Enum.from_enum(KernelStatus))

    @staticmethod
    async def resolve_keypairs(executor, info, user_id, is_active=None):
        if is_active is None:
            kploader = info.context['kploader']
        elif is_active:
            kploader = info.context['kpaloader']
        else:
            kploader = info.context['kpiloader']
        return kploader.load(user_id)

    @staticmethod
    async def resolve_vfolders(executor, info, access_key):
        loader = info.context['vfloader']
        return loader.load(access_key)

    @staticmethod
    async def resolve_compute_sessions(executor, info, access_key, status=None):
        loader = info.context['csloader']
        return loader.load(access_key)

    @staticmethod
    async def resolve_compute_workers(executor, info, sess_id, status=None):
        loader = info.context['cwloader']
        return loader.load(sess_id)


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
