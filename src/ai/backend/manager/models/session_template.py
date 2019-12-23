import enum
from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from .base import metadata, GUID, IDColumn, EnumType
from .user import UserRole

__all__: Sequence[str] = (
    'TemplateType', 'session_templates', 'query_accessible_session_templates'
)


class TemplateType(str, enum.Enum):
    TASK = 'task'
    CLUSTER = 'cluster'


session_templates = sa.Table(
    'session_templates', metadata,
    IDColumn('id'),
    sa.Column('created_at', sa.DateTime(timezone=True), index=True),
    sa.Column('is_active', sa.Boolean, default=True),

    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False),
    sa.Column('group_id', GUID, sa.ForeignKey('groups.id'), nullable=True),
    sa.Column('user_uuid', GUID, sa.ForeignKey('users.uuid'), index=True, nullable=False),
    sa.Column('type',
              EnumType(TemplateType),
              nullable=False,
              server_default='TASK',
              index=True
              ),

    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('template', pgsql.JSONB(), nullable=False)
)


async def query_accessible_session_templates(conn, user_uuid, template_type: TemplateType, *,
                                             user_role=None, domain_name=None,
                                             allowed_types=['user'],
                                             extra_conds=None):
    from ai.backend.manager.models import groups, users, association_groups_users as agus
    entries = []
    if 'user' in allowed_types:
        # Query user templates
        j = (session_templates.join(users, session_templates.c.user_uuid == users.c.uuid))
        query = (sa.select([
                        session_templates.c.name,
                        session_templates.c.id,
                        session_templates.c.created_at,
                        session_templates.c.user_uuid,
                        session_templates.c.group_id,
                        users.c.email
                    ])
                    .select_from(j)
                    .where((session_templates.c.user_uuid == user_uuid) &
                           session_templates.c.is_active &
                           (session_templates.c.type == TemplateType.TASK)))
        if extra_conds is not None:
            query = query.where(extra_conds)
        result = await conn.execute(query)
        async for row in result:
            entries.append({
                'name': row.name,
                'id': row.id,
                'created_at': row.created_at,
                'is_owner': True,
                'user': str(row.user_uuid) if row.user_uuid else None,
                'group': str(row.group_id) if row.group_id else None,
                'user_email': row.email,
                'group_name': None,
            })
    if 'group' in allowed_types:
        # Query group session_templates
        if user_role == UserRole.ADMIN or user_role == 'admin':
            query = (sa.select([groups.c.id])
                        .select_from(groups)
                        .where(groups.c.domain_name == domain_name))
            result = await conn.execute(query)
            grps = await result.fetchall()
            group_ids = [g.id for g in grps]
        else:
            j = sa.join(agus, users, agus.c.user_id == users.c.uuid)
            query = (sa.select([agus.c.group_id])
                        .select_from(j)
                        .where(agus.c.user_id == user_uuid))
            result = await conn.execute(query)
            grps = await result.fetchall()
            group_ids = [g.group_id for g in grps]
        j = (session_templates.join(groups, session_templates.c.group_id == groups.c.id))
        query = (sa.select([
                        session_templates.c.name,
                        session_templates.c.id,
                        session_templates.c.created_at,
                        session_templates.c.user_uuid,
                        session_templates.c.group_id,
                        groups.c.name
                    ], use_labels=True)
                    .where(session_templates.c.group_id.in_(group_ids) &
                           session_templates.c.is_active &
                           (session_templates.c.type == TemplateType.TASK)))
        if extra_conds is not None:
            query = query.where(extra_conds)
        if 'user' in allowed_types:
            query = query.where(session_templates.c.user_uuid != user_uuid)
        result = await conn.execute(query)
        is_owner = (user_role == UserRole.ADMIN or user_role == 'admin')
        async for row in result:
            entries.append({
                'name': row.session_templates_name,
                'id': row.session_templates_id,
                'created_at': row.session_templates_created_at,
                'is_owner': is_owner,
                'user': (str(row.session_templates_user_uuid) if row.session_templates_user_uuid
                         else None),
                'group': str(row.session_templates_group_id) if row.session_templates_group_id else None,
                'user_email': None,
                'group_name': row.groups_name,
            })
    return entries
