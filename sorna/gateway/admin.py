import asyncio
import base64
from collections import OrderedDict
import functools
import inspect
import logging
import secrets
import typing

from aiohttp import web
from aiotools import apartial
from aiodataloader import DataLoader
import graphene
from graphene.types.datetime import DateTime as GQLDateTime
from graphql.execution.executors.asyncio import AsyncioExecutor
import simplejson as json
import sqlalchemy as sa

from .exceptions import InvalidAPIParameters, SornaError
from ..manager.models import keypairs, vfolders

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
        result = schema.execute(
            body['query'], executor,
            variable_values=body['variables'],
            context_value={'conn': conn, 'vfloader': vfloader},
            return_promise=True)
        if inspect.isawaitable(result):
            result = await result
    if result.errors:
        # Here the errors are mostly about validation of types and GraphQL syntaxes.
        # TODO: There may be "GraphQLLocatedError" which is raised if resolve()
        # methods raises an arbitrary exception.
        raise InvalidAPIParameters(result.errors[0].args[0])
    else:
        return web.json_response(result.data, status=200, dumps=json.dumps)


class KeyPairInput(graphene.InputObjectType):
    is_active = graphene.Boolean()
    billing_plan = graphene.String()
    concurrency_limit = graphene.Int()
    rate_limit = graphene.Int()


class CreateKeyPair(graphene.Mutation):

    class Arguments:
        user_id = graphene.Int(required=True)
        props = KeyPairInput(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    keypair = graphene.Field(lambda: KeyPair)

    @staticmethod
    async def mutate(root, info, user_id, props):
        conn = info.context['conn']
        ak = 'AKIA' + base64.b32encode(secrets.token_bytes(10)).decode('ascii')
        sk = secrets.token_urlsafe(30)
        data = {
            'user_id': user_id,
            'access_key': ak,
            'secret_key': sk,
            'is_active': props.is_active,
            'billing_plan': props.billing_plan,
            'concurrency_limit': props.concurrency_limit,
            'concurrency_used': 0,
            'rate_limit': props.rate_limit,
            'num_queries': 0,
        }
        query = (keypairs.insert().values(data))
        result = await conn.execute(query)
        if result.rowcount == 1:
            o = KeyPair(**data)
            return CreateKeyPair(ok=True, msg='success', keypair=o)
        else:
            return CreateKeyPair(ok=False, msg='failed to create keypair', keypair=None)


class KeyPair(graphene.ObjectType):
    access_key = graphene.String()
    secret_key = graphene.String()
    is_active = graphene.Boolean()
    billing_plan = graphene.String()
    created_at = GQLDateTime()
    last_used = GQLDateTime()
    concurrency_limit = graphene.Int()
    concurrency_used = graphene.Int()
    rate_limit = graphene.Int()
    num_queries = graphene.Int()

    vfolders = graphene.List(lambda: VirtualFolder)

    @classmethod
    async def to_obj(cls, row):
        return cls(
            access_key=row.access_key,
            secret_key=row.secret_key,
            is_active=row.is_active,
            billing_plan=row.billing_plan,
            created_at=row.created_at,
            last_used=row.last_used,
            concurrency_limit=row.concurrency_limit,
            concurrency_used=row.concurrency_used,
            rate_limit=row.rate_limit,
            num_queries=row.num_queries,
        )

    async def resolve_vfolders(self, info):
        conn = info.context['conn']
        vfloader = info.context['vfloader']
        # Use dataloader for automatic batching
        result = await vfloader.load(self.access_key)
        return result


class VirtualFolder(graphene.ObjectType):
    id = graphene.UUID()
    host = graphene.String()
    name = graphene.String()
    max_files = graphene.Int()
    max_size = graphene.Int()
    num_files = graphene.Int()
    cur_size = graphene.Int()  # virtual value
    created_at = GQLDateTime()
    last_used = GQLDateTime()

    @classmethod
    async def to_obj(cls, row):
        return cls(
            id=row.id,
            host=row.host,
            name=row.name,
            max_files=row.max_files,
            max_size=row.max_size,    # in KiB
            num_files=row.num_files,  # TODO: measure on-the-fly?
            cur_size=1234,            # TODO: measure on-the-fly
            created_at=row.created_at,
            last_used=row.last_used,
        )

    @staticmethod
    async def batch_load(conn, access_keys):
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where(vfolders.c.belongs_to.in_(access_keys)))
        objs_per_key = OrderedDict()
        for k in access_keys:
            objs_per_key[k] = list()
        async for row in conn.execute(query):
            o = await to_obj(row)
            objs_per_key[row.belongs_to].append(o)
        return tuple(objs_per_key.values())


class Mutation(graphene.ObjectType):
    create_keypair = CreateKeyPair.Field()


class Query(graphene.ObjectType):
    keypairs = graphene.List(KeyPair, user_id=graphene.Int(required=True))
    vfolders = graphene.List(VirtualFolder,
                             access_key=graphene.String(required=True))

    @staticmethod
    async def resolve_keypairs(executor, info, user_id):
        conn = info.context['conn']
        query = (sa.select('*')
                   .select_from(keypairs)
                   .where(keypairs.c.user_id == user_id))
        objects = []
        async for row in conn.execute(query):
            o = await KeyPair.to_obj(row)
            objects.append(o)
        return objects

    @staticmethod
    async def resolve_vfolders(executor, info, access_key):
        conn = info.context['conn']
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where(vfolders.c.belongs_to == access_key))
        objects = []
        async for row in conn.execute(query):
            o = await VirtualFolder.to_obj(row)
            objects.append(o)
        return objects


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
