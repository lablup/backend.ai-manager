import asyncio
import base64
import inspect
import logging
import secrets
import typing

from aiohttp import web
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
    async with request.app['dbpool'].acquire() as conn, conn.begin():
        result = schema.execute(
            body['query'], executor,
            variable_values=body['variables'],
            context_value={'conn': conn},
            return_promise=True)
        if inspect.isawaitable(result):
            result = await result
    if result.errors:
        log.error(result.errors)
        raise SornaError(result.errors[0].args[0])
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
    user_id = graphene.Int()
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


class VirtualFolder(graphene.ObjectType):
    id = graphene.UUID()
    host = graphene.String()
    name = graphene.String()
    max_files = graphene.Int()
    max_size = graphene.Int()
    cur_size = graphene.Int()  # virtual value
    num_files = graphene.Int()
    created_at = GQLDateTime()
    last_used = GQLDateTime()
    belongs_to = graphene.String()  # fk:access_key


class Mutation(graphene.ObjectType):
    create_keypair = CreateKeyPair.Field()


class Query(graphene.ObjectType):
    keypairs = graphene.List(KeyPair, user_id=graphene.Int(required=True))
    vfolders = graphene.List(VirtualFolder, access_key=graphene.String(required=True))

    async def resolve_keypairs(self, info, user_id):
        conn = info.context['conn']
        query = (sa.select('*')
                   .select_from(keypairs)
                   .where(keypairs.c.user_id == user_id))
        objects = []
        async for row in conn.execute(query):
            o = KeyPair(**row)
            objects.append(o)
        return objects

    async def resolve_vfolders(self, info, access_key):
        conn = info.context['conn']
        query = (sa.select('*')
                   .select_from(vfolders)
                   .where(vfolders.c.belongs_to == access_key))
        objects = []
        async for row in conn.execute(query):
            # TODO: implement - read the actual size from NAS/EFS
            cur_size = 1234
            o = VirtualFolder(
                cur_size=cur_size,
                **row)
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
