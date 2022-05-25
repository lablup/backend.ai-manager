"""split session and kernel table

Revision ID: ab6ac93efd6c
Revises: 81c264528f20
Create Date: 2022-05-24 15:27:18.773174

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from sqlalchemy.orm import relationship, registry
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.ext.declarative import declarative_base

from ai.backend.manager.models.base import (
    GUID, KernelIDColumn, convention
)
from ai.backend.manager.models import ImageRow

# revision identifiers, used by Alembic.
revision = 'ab6ac93efd6c'
down_revision = '81c264528f20'
branch_labels = None
depends_on = None

KernelStatus = (
    'PENDING', 'SCHEDULED', 'PREPARING', 'BUILDING',
    'PULLING', 'RUNNING', 'RESTARTING', 'RESIZING',
    'SUSPENDED', 'TERMINATING', 'TERMINATED', 'ERROR', 'CANCELLED',
)

SessionStatus = (
    'PENDING', 'SCHEDULED', 'PREPARING', 'BUILDING',
    'PULLING', 'RUNNING', 'RESTARTING', 'RESIZING',
    'SUSPENDED', 'TERMINATING', 'TERMINATED', 'ERROR', 'CANCELLED',
)

metadata = sa.MetaData(naming_convention=convention)
mapper_registry = registry(metadata=metadata)
Base = mapper_registry.generate_base()

def default_hostname(context) -> str:
    params = context.get_current_parameters()
    return f"{params['cluster_role']}{params['cluster_idx']}"


kernels = sa.Table(
    'kernels', metadata,
    KernelIDColumn(),
    sa.Column('session_id', GUID, nullable=False),
    sa.Column('session_creation_id', sa.String(length=32), unique=False),
    sa.Column('session_name', sa.String(length=64), unique=False),
    sa.Column('session_type', pgsql.ENUM('INTERACTIVE', 'BATCH'), nullable=False,
                default='INTERACTIVE', server_default='INTERACTIVE'),
    sa.Column('cluster_mode', sa.String(length=16), nullable=False,
                default='SINGLE_NODE', server_default='SINGLE_NODE'),
    sa.Column('cluster_size', sa.Integer, nullable=False, default=1),

    # Resource ownership
    sa.Column('scaling_group', sa.ForeignKey('scaling_groups.name'), nullable=True),
    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False),
    sa.Column('group_id', GUID, sa.ForeignKey('groups.id'), nullable=False),
    sa.Column('user_uuid', GUID, sa.ForeignKey('users.uuid'), nullable=False),
    sa.Column('access_key', sa.String(length=20), sa.ForeignKey('keypairs.access_key')),
    sa.Column('image', sa.String(length=512)),
    sa.Column('tag', sa.String(length=64), nullable=True),

    # Resource occupation
    sa.Column('occupied_slots', pgsql.JSONB(), nullable=False),
    sa.Column('vfolder_mounts', pgsql.JSONB(), nullable=True),
    sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
            server_default=sa.func.now()),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
            nullable=True, default=sa.null()),
    sa.Column('starts_at', sa.DateTime(timezone=True),
            nullable=True, default=sa.null()),
    sa.Column('status', sa.Enum(*KernelStatus),
            default='PENDING',
            server_default='PENDING',
            nullable=False),
    sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True),
    sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null()),
    sa.Column('status_data', pgsql.JSONB(), nullable=True, default=sa.null()),
    sa.Column('callback_url', sa.types.UnicodeText, nullable=True, default=sa.null()),

    sa.Column('startup_command', sa.Text, nullable=True),
    sa.Column('result', sa.Enum('UNDEFINED', 'SUCCESS', 'FAILURE'),
            default='UNDEFINED',
            server_default='UNDEFINED',
            nullable=False),

    # Resource metrics measured upon termination
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('last_stat', pgsql.JSONB(), nullable=True, default=sa.null()),
)

class KernelRow:
    pass

mapper_registry.map_imperatively(KernelRow, kernels)


class SessionRow(Base):
    __tablename__ = 'sessions'
    id = sa.Column('id', GUID(), server_default=sa.text('uuid_generate_v4()'), nullable=False, primary_key=True)
    creation_id = sa.Column('creation_id', sa.String(length=32), unique=False)
    name = sa.Column('name', sa.String(length=64), unique=False)
    session_type = sa.Column('session_type', pgsql.ENUM('INTERACTIVE', 'BATCH', name='sessiontypes', create_type=False), server_default='INTERACTIVE', nullable=False)
    cluster_mode = sa.Column('cluster_mode', sa.String(length=16), server_default='SINGLE_NODE', nullable=False)
    cluster_size = sa.Column('cluster_size', sa.Integer(), nullable=False)
    
    # Resource ownership
    scaling_group_name = sa.Column('scaling_group_name', sa.String(length=64), nullable=True)
    domain_name = sa.Column('domain_name', sa.String(length=64), nullable=False)
    group_id = sa.Column('group_id', GUID(), nullable=False)
    user_uuid = sa.Column('user_uuid', GUID(), nullable=False)
    kp_access_key = sa.Column('kp_access_key', sa.String(length=20), nullable=True)

    # if image_id is null, should find a image field from related kernel row.
    image_id = sa.Column('image_id', GUID(), nullable=True)
    tag = sa.Column('tag', sa.String(length=64), nullable=True)

    # Resource occupation
    occupying_slots = sa.Column('occupying_slots', pgsql.JSONB(), nullable=False)
    requested_slots = sa.Column('requested_slots', pgsql.JSONB(), nullable=False)
    vfolder_mounts = sa.Column('vfolder_mounts', pgsql.JSONB(), nullable=True)
    resource_opts = sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={})
    bootstrap_script= sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True)

    # Lifecycle
    created_at = sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True)
    terminated_at = sa.Column('terminated_at', sa.DateTime(timezone=True), nullable=True)
    starts_at = sa.Column('starts_at', sa.DateTime(timezone=True), nullable=True)
    status = sa.Column(
        'status',
        pgsql.ENUM(
            'PENDING', 'SCHEDULED', 'PREPARING', 'BUILDING', 'PULLING',
            'RUNNING', 'RESTARTING', 'RESIZING', 'SUSPENDED', 'TERMINATING',
            'TERMINATED', 'ERROR', 'CANCELLED', name='sessionstatus', create_type=False,
         ), server_default='PENDING', nullable=False)
    status_changed = sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True)
    status_info = sa.Column('status_info', sa.Unicode(), nullable=True)
    status_data = sa.Column('status_data', pgsql.JSONB(astext_type=sa.Text()), nullable=True)
    callback_url = sa.Column('callback_url', sa.types.UnicodeText(), nullable=True)

    startup_command = sa.Column('startup_command', sa.Text(), nullable=True)
    result = sa.Column('result', pgsql.ENUM('UNDEFINED', 'SUCCESS', 'FAILURE', name='sessionresults', create_type=False), server_default='UNDEFINED', nullable=False)

    # Resource metrics measured upon termination
    num_queries = sa.Column('num_queries', sa.BigInteger(), nullable=True)
    last_stat = sa.Column('last_stat', pgsql.JSONB(astext_type=sa.Text()), nullable=True)


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    connection = op.get_bind()
    db_session = Session(connection)

    SessionRow.__table__.create(connection)
    op.create_foreign_key(op.f('fk_sessions_access_key_keypairs'), 'sessions', 'keypairs', ['kp_access_key'], ['access_key'])
    op.create_foreign_key(op.f('fk_sessions_domain_name_domains'), 'sessions', 'domains', ['domain_name'], ['name'])
    op.create_foreign_key(op.f('fk_sessions_group_id_groups'), 'sessions', 'groups', ['group_id'], ['id'])
    op.create_foreign_key(op.f('fk_sessions_image_images'), 'sessions', 'images', ['image_id'], ['id'])
    op.create_foreign_key(op.f('fk_sessions_scaling_group_scaling_groups'), 'sessions', 'scaling_groups', ['scaling_group_name'], ['name'])
    op.create_foreign_key(op.f('fk_sessions_user_uuid_users'), 'sessions', 'users', ['user_uuid'], ['uuid'])
    op.create_index(op.f('ix_sessions_created_at'), 'sessions', ['created_at'], unique=False)
    op.create_index(op.f('ix_sessions_name'), 'sessions', ['name'], unique=False)
    op.create_index(op.f('ix_sessions_result'), 'sessions', ['result'], unique=False)
    op.create_index(op.f('ix_sessions_scaling_group'), 'sessions', ['scaling_group_name'], unique=False)
    op.create_index(op.f('ix_sessions_session_type'), 'sessions', ['session_type'], unique=False)
    op.create_index(op.f('ix_sessions_status'), 'sessions', ['status'], unique=False)
    op.create_index(op.f('ix_sessions_status_changed'), 'sessions', ['status_changed'], unique=False)
    op.create_index(op.f('ix_sessions_terminated_at'), 'sessions', ['terminated_at'], unique=False)

    query = (
        sa.select([kernels])
        .select_from(kernels)
        .order_by(kernels.c.created_at)
    )
    kernel_rows = connection.execute(query).fetchall()

    imgs = db_session.query(ImageRow).all()
    img_map = {img.name: img.id for img in imgs}

    all_kernel_sessions = {}
    for row in kernel_rows:
        sess_id = row['session_id']
        if sess_id not in all_kernel_sessions:
            sess = {
                'id': sess_id,
                'creation_id': row['session_creation_id'],
                'name': row['session_name'],
                'session_type': row['session_type'],
                'cluster_mode': row['cluster_mode'],
                'cluster_size': row['cluster_size'],
                'scaling_group_name': row['scaling_group'],
                'domain_name': row['domain_name'],
                'group_id': row['group_id'],
                'user_uuid': row['user_uuid'],
                'kp_access_key': row['access_key'],
                'image_id': img_map[row['image']],
                'tag': row['tag'],
                'occupying_slots': row['occupied_slots'],
                'requested_slots': row['occupied_slots'],
                'vfolder_mounts': row['vfolder_mounts'],
                'resource_opts': row['resource_opts'],
                'bootstrap_script': row['bootstrap_script'],
                'created_at': row['created_at'],
                'terminated_at': row['terminated_at'],
                'starts_at': row['starts_at'],
                'status': row['status'],
                'status_changed': row['status_changed'],
                'status_info': row['status_info'],
                'status_data': row['status_data'],
                'callback_url': row['callback_url'],
                'startup_command': row['startup_command'],
                'result': row['result'],
                'num_queries': row['num_queries'],
                'last_stat': row['last_stat'],
            }
            all_kernel_sessions[sess_id] = sess
        else:
            sess = all_kernel_sessions[sess_id]
            st_change = sess['status_changed']

            if sess['created_at'] > row['created_at']:
                sess['created_at'] = row['created_at']
                st_change = row['st_change']
            
            if sess['starts_at'] > row['starts_at']:
                sess['starts_at'] = row['starts_at']
                st_change = row['st_change']
            
            if str(sess['status']) not in ('ERROR', 'CANCELLED'):
                if KernelStatus.index(str(sess['status'])) > KernelStatus.index(str(row['status'])):
                    sess['status'] = row['status']
                    st_change = row['st_change']

            if sess['terminated_at'] is not None and row['terminated_at'] is not None:
                if sess['terminated_at'] < row['terminated_at']:
                    sess['terminated_at'] = row['terminated_at']
                    st_change = row['status_changed']
            elif sess['terminated_at'] is None:
                pass
            elif row['terminated_at'] is None:
                sess['terminated_at'] = None
            sess['status_changed'] = st_change

            # if sess['status_info'] != row['status_info']:
            #     import json
            #     sinfo = sess['status_info']
            #     try:
            #         sinfo = json.loads(sinfo)
            #     except json.decoder.JSONDecodeError:
            #         sinfo = {sess['id']: sinfo}
            #     sinfo = {**sinfo, row['id']: row['status_info']}
            #     sess['status_info'] = json.dumps(sinfo)
        
    creates = list(all_kernel_sessions.values())
    if creates:
        connection.execute(SessionRow.__table__.insert(), creates)

    # Session dependency table
    op.create_foreign_key(op.f('fk_session_dependencies_session_id_sessions'), 'session_dependencies', 'sessions', ['session_id'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.create_foreign_key(op.f('fk_session_dependencies_depends_on_sessions'), 'session_dependencies', 'sessions', ['depends_on'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.drop_constraint('fk_session_dependencies_depends_on_kernels', 'session_dependencies', type_='foreignkey')
    op.drop_constraint('fk_session_dependencies_session_id_kernels', 'session_dependencies', type_='foreignkey')

    # Kernel table
    op.create_foreign_key(op.f('fk_kernels_session_id_sessions'), 'kernels', 'sessions', ['session_id'], ['id'])
    op.drop_index('ix_kernels_sess_id_role', table_name='kernels')
    op.drop_index('ix_kernels_session_name', table_name='kernels')
    op.drop_index('ix_kernels_session_type', table_name='kernels')
    op.drop_index('ix_kernels_status_role', table_name='kernels')
    op.drop_index('ix_kernels_unique_sess_token', table_name='kernels')
    op.drop_column('kernels', 'session_type')
    op.drop_column('kernels', 'cluster_mode')
    op.drop_column('kernels', 'session_name')
    op.drop_column('kernels', 'cluster_size')
    op.drop_column('kernels', 'session_creation_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    connection = op.get_bind()

    # Kernel table
    op.add_column('kernels', sa.Column('session_creation_id', sa.String(length=32), nullable=True, autoincrement=False))
    op.add_column('kernels', sa.Column('cluster_size', sa.Integer, nullable=False, autoincrement=False,
                                       default=1, server_default=sa.text('1')))
    op.add_column('kernels', sa.Column('session_name', sa.VARCHAR(length=64), autoincrement=False, nullable=True))
    op.add_column('kernels', sa.Column('cluster_mode', sa.VARCHAR(length=16), server_default='SINGLE_NODE', autoincrement=False, nullable=False))
    op.add_column('kernels', sa.Column('session_type', pgsql.ENUM('INTERACTIVE', 'BATCH', name='sessiontypes', create_type=False), server_default='INTERACTIVE', autoincrement=False, nullable=False))
    op.drop_constraint(op.f('fk_kernels_session_id_sessions'), 'kernels', type_='foreignkey')
    op.create_index('ix_kernels_unique_sess_token', 'kernels', ['access_key', 'session_name'], unique=False)
    op.create_index('ix_kernels_status_role', 'kernels', ['status', 'cluster_role'], unique=False)
    op.create_index('ix_kernels_session_type', 'kernels', ['session_type'], unique=False)
    op.create_index('ix_kernels_session_name', 'kernels', ['session_name'], unique=False)
    op.create_index('ix_kernels_sess_id_role', 'kernels', ['session_id', 'cluster_role'], unique=False)

    query = (
        sa.select([
            SessionRow.id,
            SessionRow.creation_id,
            SessionRow.cluster_size,
            SessionRow.name,
            SessionRow.cluster_mode,
            SessionRow.session_type,
        ])
        .select_from(SessionRow)
    )
    session_rows = connection.execute(query).fetchall()
    sess_map = {str(sess['id']): sess for sess in session_rows}

    query = (
        sa.select([
            kernels.c.id,
            kernels.c.session_id,
        ])
        .select_from(kernels)
        .order_by(kernels.c.created_at)
    )
    kernel_rows = connection.execute(query).fetchall()
    updates = []

    for row in kernel_rows:
        sess = sess_map[str(row['session_id'])]
        kernel = {
            'row_id': row['id'],
            'session_creation_id': sess['creation_id'],
            'cluster_size': sess['cluster_size'],
            'session_name': sess['name'],
            'cluster_mode': sess['cluster_mode'],
            'session_type': sess['session_type'],
        }
        updates.append(kernel)

    if updates:
        query = (sa.update(kernels)
                   .values({
                       'session_creation_id': bindparam('session_creation_id'),
                       'cluster_size': bindparam('cluster_size'),
                       'session_name': bindparam('session_name'),
                       'cluster_mode': bindparam('cluster_mode'),
                       'session_type': bindparam('session_type'),
                   })
                   .where(kernels.c.id == bindparam('row_id')))
        connection.execute(query, updates)

    # Session dependency table
    op.create_foreign_key('fk_session_dependencies_session_id_kernels', 'session_dependencies', 'kernels', ['session_id'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.create_foreign_key('fk_session_dependencies_depends_on_kernels', 'session_dependencies', 'kernels', ['depends_on'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.drop_constraint(op.f('fk_session_dependencies_depends_on_sessions'), 'session_dependencies', type_='foreignkey')
    op.drop_constraint(op.f('fk_session_dependencies_session_id_sessions'), 'session_dependencies', type_='foreignkey')

    # Session table
    op.drop_index(op.f('ix_sessions_terminated_at'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_status_changed'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_status'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_session_type'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_scaling_group'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_result'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_name'), table_name='sessions')
    op.drop_index(op.f('ix_sessions_created_at'), table_name='sessions')
    op.drop_table('sessions')
    # ### end Alembic commands ###
