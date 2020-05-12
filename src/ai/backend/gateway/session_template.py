import json
import logging
import uuid

from aiohttp import web
import aiohttp_cors
import sqlalchemy as sa
import trafaret as t
from typing import Any, Tuple
import yaml

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import SessionTypes

from .auth import auth_required
from .exceptions import InvalidAPIParameters, TaskTemplateNotFound
from .manager import READ_ALLOWED, server_status_required
from .types import CORSOptions, Iterable, WebMiddleware
from .utils import check_api_params, get_access_key_scopes

from ..manager.models import (
    association_groups_users as agus, domains,
    groups, session_templates, keypairs, users, UserRole,
    query_accessible_session_templates, TemplateType,
    verify_vfolder_name
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.session_template'))


task_template_v1 = t.Dict({
    tx.AliasedKey(['api_version', 'apiVersion']): t.String,
    t.Key('kind'): t.Enum('taskTemplate', 'task_template'),
    t.Key('metadata'): t.Dict({
        t.Key('name'): t.String,
        t.Key('tag', default=None): t.Null | t.String,
    }),
    t.Key('spec'): t.Dict({
        tx.AliasedKey(['type', 'sessionType'],
                      default='interactive') >> 'session_type': tx.Enum(SessionTypes),
        t.Key('kernel'): t.Dict({
            t.Key('image'): t.String,
            t.Key('environ', default={}): t.Null | t.Mapping(t.String, t.String),
            t.Key('run', default=None): t.Null | t.Dict({
                t.Key('bootstrap', default=None): t.Null | t.String,
                tx.AliasedKey(['startup', 'startup_command', 'startupCommand'],
                              default=None) >> 'startup_command': t.Null | t.String
            }),
            t.Key('git', default=None): t.Null | t.Dict({
                t.Key('repository'): t.String,
                t.Key('commit', default=None): t.Null | t.String,
                t.Key('branch', default=None): t.Null | t.String,
                t.Key('credential', default=None): t.Null | t.Dict({
                    t.Key('username'): t.String,
                    t.Key('password'): t.String
                }),
                tx.AliasedKey(['destination_dir', 'destinationDir'],
                              default=None) >> 'dest_dir': t.Null | t.String
            })
        }),
        t.Key('mounts', default=None): t.Null | t.Mapping(t.String, t.Any),
        t.Key('resources', default=None): t.Null | t.Mapping(t.String, t.Any)
    })
}).allow_extra('*')


@server_status_required(READ_ALLOWED)
@auth_required
@check_api_params(t.Dict(
    {
        tx.AliasedKey(['group', 'groupName', 'group_name'], default='default'): t.String,
        tx.AliasedKey(['domain', 'domainName', 'domain_name'], default='default'): t.String,
        t.Key('owner_access_key', default=None): t.Null | t.String,
        t.Key('payload'): t.String,
        t.Key('type') >> 'template_type': tx.Enum(TemplateType)
    }
))
async def create(request: web.Request, params: Any) -> web.Response:
    if params['domain'] is None:
        params['domain'] = request['user']['domain_name']
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    requester_uuid = request['user']['uuid']
    log.info('CREATE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')
    user_uuid = request['user']['uuid']

    dbpool = request.app['dbpool']

    async with dbpool.acquire() as conn, conn.begin():
        if requester_access_key != owner_access_key:
            # Admin or superadmin is creating sessions for another user.
            # The check for admin privileges is already done in get_access_key_scope().
            query = (
                sa.select([keypairs.c.user, users.c.role, users.c.domain_name])
                .select_from(sa.join(keypairs, users, keypairs.c.user == users.c.uuid))
                .where(keypairs.c.access_key == owner_access_key)
            )
            result = await conn.execute(query)
            row = await result.fetchone()
            owner_domain = row['domain_name']
            owner_uuid = row['user']
            owner_role = row['role']
        else:
            # Normal case when the user is creating her/his own session.
            owner_domain = request['user']['domain_name']
            owner_uuid = requester_uuid
            owner_role = UserRole.USER

        query = (
            sa.select([domains.c.name])
            .select_from(domains)
            .where(
                (domains.c.name == owner_domain) &
                (domains.c.is_active)
            )
        )
        qresult = await conn.execute(query)
        domain_name = await qresult.scalar()
        if domain_name is None:
            raise InvalidAPIParameters('Invalid domain')

        if owner_role == UserRole.SUPERADMIN:
            # superadmin can spawn container in any designated domain/group.
            query = (
                sa.select([groups.c.id])
                .select_from(groups)
                .where(
                    (groups.c.domain_name == params['domain']) &
                    (groups.c.name == params['group']) &
                    (groups.c.is_active)
                ))
            qresult = await conn.execute(query)
            group_id = await qresult.scalar()
        elif owner_role == UserRole.ADMIN:
            # domain-admin can spawn container in any group in the same domain.
            if params['domain'] != owner_domain:
                raise InvalidAPIParameters("You can only set the domain to the owner's domain.")
            query = (
                sa.select([groups.c.id])
                .select_from(groups)
                .where(
                    (groups.c.domain_name == owner_domain) &
                    (groups.c.name == params['group']) &
                    (groups.c.is_active)
                ))
            qresult = await conn.execute(query)
            group_id = await qresult.scalar()
        else:
            # normal users can spawn containers in their group and domain.
            if params['domain'] != owner_domain:
                raise InvalidAPIParameters("You can only set the domain to your domain.")
            query = (
                sa.select([agus.c.group_id])
                .select_from(agus.join(groups, agus.c.group_id == groups.c.id))
                .where(
                    (agus.c.user_id == owner_uuid) &
                    (groups.c.domain_name == owner_domain) &
                    (groups.c.name == params['group']) &
                    (groups.c.is_active)
                ))
            qresult = await conn.execute(query)
            group_id = await qresult.scalar()
        if group_id is None:
            raise InvalidAPIParameters('Invalid group')

        log.debug('Params: {0}', params)
        try:
            body = json.loads(params['payload'])
        except json.JSONDecodeError:
            try:
                body = yaml.load(params['payload'], Loader=yaml.BaseLoader)
            except (yaml.YAMLError, yaml.MarkedYAMLError):
                raise InvalidAPIParameters('Malformed payload')
        body = task_template_v1.check(body)
        if mounts := body['spec'].get('mounts'):
            for p in mounts.values():
                if p is None:
                    continue
                if not p.startswith('/home/work/'):
                    raise InvalidAPIParameters(f'Path {p} should start with /home/work/')
                if not verify_vfolder_name(p.replace('/home/work/', '')):
                    raise InvalidAPIParameters(f'Path {p} is reserved for internal operations.')

        template_id = uuid.uuid4().hex
        resp = {
            'id': template_id,
            'user': user_uuid.hex,
        }
        query = session_templates.insert().values({
            'id': template_id,
            'domain_name': params['domain'],
            'group_id': group_id,
            'user_uuid': user_uuid,
            'name': body['metadata']['name'],
            'template': body
        })
        result = await conn.execute(query)
        assert result.rowcount == 1
    return web.json_response(resp)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('all', default=False): t.ToBool,
        tx.AliasedKey(['group_id', 'groupId'], default=None): tx.UUID | t.String | t.Null,
    }),
)
async def list_template(request: web.Request, params: Any) -> web.Response:
    resp = []
    dbpool = request.app['dbpool']
    access_key = request['keypair']['access_key']
    domain_name = request['user']['domain_name']
    user_role = request['user']['role']
    user_uuid = request['user']['uuid']

    log.info('LIST (ak:{})', access_key)
    async with dbpool.acquire() as conn:
        if request['is_superadmin'] and params['all']:
            j = (session_templates
                    .join(users, session_templates.c.user_uuid == users.c.uuid, isouter=True)
                    .join(groups, session_templates.c.group_id == groups.c.id, isouter=True))
            query = (sa.select([session_templates, users.c.email, groups.c.name], use_labels=True)
                       .select_from(j)
                       .where(session_templates.c.is_active))
            result = await conn.execute(query)
            entries = []
            async for row in result:
                is_owner = True if row.session_templates_user == user_uuid else False
                entries.append({
                    'name': row.session_templates_name,
                    'id': row.session_templates_id,
                    'created_at': row.session_templates_created_at,
                    'is_owner': is_owner,
                    'user': (str(row.session_templates_user_uuid)
                             if row.session_templates_user_uuid else None),
                    'group': (str(row.session_templates_group_id)
                              if row.session_templates_group_id else None),
                    'user_email': row.users_email,
                    'group_name': row.groups_name,
                })
        else:
            extra_conds = None
            if params['group_id'] is not None:
                extra_conds = ((session_templates.c.group_id == params['group_id']))
            entries = await query_accessible_session_templates(
                        conn, user_uuid, TemplateType.TASK,
                        user_role=user_role, domain_name=domain_name,
                        allowed_types=['user', 'group'], extra_conds=extra_conds)

        for entry in entries:
            resp.append({
                'name': entry['name'],
                'id': entry['id'].hex,
                'created_at': str(entry['created_at']),
                'is_owner': entry['is_owner'],
                'user': str(entry['user']),
                'group': str(entry['group']),
                'user_email': entry['user_email'],
                'group_name': entry['group_name'],
                'type': 'user' if entry['user'] is not None else 'group',
            })
        return web.json_response(resp)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('format', default='yaml'): t.Null | t.Enum('yaml', 'json'),
        t.Key('owner_access_key', default=None): t.Null | t.String,
    })
)
async def get(request: web.Request, params: Any) -> web.Response:
    if params['format'] not in ['yaml', 'json']:
        raise InvalidAPIParameters('format should be "yaml" or "json"')
    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('GET (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    template_id = request.match_info['template_id']
    dbpool = request.app['dbpool']

    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select([session_templates.c.template])
                   .select_from(session_templates)
                   .where((session_templates.c.id == template_id) &
                          (session_templates.c.is_active)
                          ))
        template = await conn.scalar(query)
        if not template:
            raise TaskTemplateNotFound
    template = json.loads(template)
    if params['format'] == 'yaml':
        body = yaml.dump(template)
        return web.Response(text=body, content_type='text/yaml')
    else:
        return web.json_response(template)


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('payload'): t.String,
        t.Key('owner_access_key', default=None): t.Null | t.String,
    })
)
async def put(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    template_id = request.match_info['template_id']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('PUT (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select([session_templates.c.id])
                   .select_from(session_templates)
                   .where((session_templates.c.id == template_id) &
                          (session_templates.c.is_active)
                          ))
        result = await conn.scalar(query)
        if not result:
            raise TaskTemplateNotFound
        try:
            body = json.loads(params['payload'])
        except json.JSONDecodeError:
            body = yaml.load(params['payload'], Loader=yaml.BaseLoader)
        except (yaml.YAMLError, yaml.MarkedYAMLError):
            raise InvalidAPIParameters('Malformed payload')
        body = task_template_v1.check(body)
        query = (sa.update(session_templates)
                   .values(template=body, name=body['metadata']['name'])
                   .where((session_templates.c.id == template_id)))
        result = await conn.execute(query)
        assert result.rowcount == 1

        return web.json_response({'success': True})


@auth_required
@server_status_required(READ_ALLOWED)
@check_api_params(
    t.Dict({
        t.Key('owner_access_key', default=None): t.Null | t.String,
    })
)
async def delete(request: web.Request, params: Any) -> web.Response:
    dbpool = request.app['dbpool']
    template_id = request.match_info['template_id']

    requester_access_key, owner_access_key = await get_access_key_scopes(request, params)
    log.info('DELETE (ak:{0}/{1})',
             requester_access_key, owner_access_key if owner_access_key != requester_access_key else '*')

    async with dbpool.acquire() as conn, conn.begin():
        query = (sa.select([session_templates.c.id])
                   .select_from(session_templates)
                   .where((session_templates.c.id == template_id) &
                          (session_templates.c.is_active)
                          ))
        result = await conn.scalar(query)
        if not result:
            raise TaskTemplateNotFound

        query = (sa.update(session_templates)
                   .values(is_active=False)
                   .where((session_templates.c.id == template_id)))
        result = await conn.execute(query)
        assert result.rowcount == 1

        return web.json_response({'success': True})


async def init(app: web.Application) -> None:
    pass


async def shutdown(app: web.Application) -> None:
    pass


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['api_versions'] = (4, 5)
    app['prefix'] = 'template/session'
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('POST', '', create))
    cors.add(app.router.add_route('GET', '', list_template))
    template_resource = cors.add(app.router.add_resource(r'/{template_id}'))
    cors.add(template_resource.add_route('GET', get))
    cors.add(template_resource.add_route('PUT', put))
    cors.add(template_resource.add_route('DELETE', delete))

    return app, []
