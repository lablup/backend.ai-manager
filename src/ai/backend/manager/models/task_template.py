from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql

from .base import metadata, GUID
from .user import UserRole

__all__: Sequence[str] = (
    'task_templates', 'query_accessible_task_templates'
)


task_templates = sa.Table(
    'task_templates', metadata,
    sa.Column('id', GUID, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), index=True),
    sa.Column('is_active', sa.Boolean, default=True),

    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False),
    sa.Column('group_id', GUID, sa.ForeignKey('groups.id'), nullable=True),
    sa.Column('user_uuid', GUID, sa.ForeignKey('users.uuid'), nullable=False),

    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('template', pgsql.JSONB(), nullable=False)
)


async def query_accessible_task_templates(conn, user_uuid, *,
                                          user_role=None, domain_name=None,
                                          allowed_types=['user'],
                                          extra_conds=None):
    from ai.backend.manager.models import groups, users, association_groups_users as agus
    entries = []
    if 'user' in allowed_types:
        # Query user templates
        j = (task_templates.join(users, task_templates.c.user_uuid == users.c.uuid))
        query = (sa.select([
                        task_templates.c.name,
                        task_templates.c.id,
                        task_templates.c.created_at,
                        task_templates.c.user_uuid,
                        task_templates.c.group_id,
                        users.c.email
                    ])
                    .select_from(j)
                    .where(task_templates.c.user_uuid == user_uuid)
                    .where(task_templates.c.is_active))
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
        # Query group templates
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
        j = (task_templates.join(groups, task_templates.c.group_id == groups.c.id))
        query = (sa.select([
                        task_templates.c.name,
                        task_templates.c.id,
                        task_templates.c.created_at,
                        task_templates.c.user_uuid,
                        task_templates.c.group_id,
                        groups.c.name
                    ], use_labels=True)
                    .select_from(j)
                    .where(task_templates.c.group_id.in_(group_ids))
                    .where(task_templates.c.is_active))
        if extra_conds is not None:
            query = query.where(extra_conds)
        if 'user' in allowed_types:
            query = query.where(task_templates.c.user_uuid != user_uuid)
        result = await conn.execute(query)
        is_owner = (user_role == UserRole.ADMIN or user_role == 'admin')
        async for row in result:
            entries.append({
                'name': row.task_templates_name,
                'id': row.task_templates_id,
                'created_at': row.task_templates_created_at,
                'is_owner': is_owner,
                'user': str(row.task_templates_user_uuid) if row.task_templates_user_uuid else None,
                'group': str(row.task_templates_group_id) if row.task_templates_group_id else None,
                'user_email': None,
                'group_name': row.groups_name,
            })
    return entries
