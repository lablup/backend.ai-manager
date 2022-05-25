"""split session and kernel table

Revision ID: ab6ac93efd6c
Revises: 81c264528f20
Create Date: 2022-05-24 15:27:18.773174

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import bindparam
from ai.backend.manager.models.base import (
    EnumType, GUID, KernelIDColumn, mapper_registry, Base, IDColumn,
)

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

def default_hostname(context) -> str:
    params = context.get_current_parameters()
    return f"{params['cluster_role']}{params['cluster_idx']}"


class ImageRow(Base):
    __tablename__ = 'images'
    id = IDColumn('id')
    name = sa.Column('name', sa.String, nullable=False, index=True)
    image = sa.Column('image', sa.String, nullable=False, index=True)
    created_at = sa.Column(
        'created_at', sa.DateTime(timezone=True),
        server_default=sa.func.now(), index=True,
    )
    tag = sa.Column('tag', sa.TEXT)
    registry = sa.Column('registry', sa.String, nullable=False, index=True)
    architecture = sa.Column('architecture', sa.String, nullable=False, index=True, default='x86_64')
    config_digest = sa.Column('config_digest', sa.CHAR(length=72), nullable=False)
    size_bytes = sa.Column('size_bytes', sa.BigInteger, nullable=False)
    type = sa.Column('type', sa.Enum('COMPUTE', 'SYSTEM', 'SERVICE'), nullable=False)
    accelerators = sa.Column('accelerators', sa.String)
    labels = sa.Column('labels', sa.JSON, nullable=False)
    resources = sa.Column('resources', pgsql.JSONB(), nullable=False)

kernels = sa.Table(
    'kernels', mapper_registry.metadata,
    # The Backend.AI-side UUID for each kernel
    # (mapped to a container in the docker backend and a pod in the k8s backend)
    KernelIDColumn(),
    # session_id == id when the kernel is the main container in a multi-container session or a
    # single-container session.
    # Otherwise, it refers the kernel ID of the main contaienr of the belonged multi-container session.
    sa.Column('session_id', GUID, index=True, nullable=False),
    sa.Column('session_creation_id', sa.String(length=32), unique=False, index=False),
    sa.Column('session_name', sa.String(length=64), unique=False, index=True),     # previously sess_id
    sa.Column('session_type', EnumType('INTERACTIVE', 'BATCH'), index=True, nullable=False,  # previously sess_type
                default='INTERACTIVE', server_default='INTERACTIVE'),
    sa.Column('cluster_mode', sa.String(length=16), nullable=False,
                default='SINGLE_NODE', server_default='SINGLE_NODE'),
    sa.Column('cluster_size', sa.Integer, nullable=False, default=1),
    sa.Column('cluster_role', sa.String(length=16), nullable=False, default='main', index=True),
    sa.Column('cluster_idx', sa.Integer, nullable=False, default=0),
    sa.Column('cluster_hostname', sa.String(length=64), nullable=False, default=default_hostname),

    # Resource ownership
    sa.Column('scaling_group', sa.ForeignKey('scaling_groups.name'), index=True, nullable=True),
    sa.Column('agent', sa.String(length=64), sa.ForeignKey('agents.id'), nullable=True),
    sa.Column('agent_addr', sa.String(length=128), nullable=True),
    sa.Column('domain_name', sa.String(length=64), sa.ForeignKey('domains.name'), nullable=False),
    sa.Column('group_id', GUID, sa.ForeignKey('groups.id'), nullable=False),
    sa.Column('user_uuid', GUID, sa.ForeignKey('users.uuid'), nullable=False),
    sa.Column('access_key', sa.String(length=20), sa.ForeignKey('keypairs.access_key')),
    sa.Column('image', sa.String(length=512)),
    sa.Column('architecture', sa.String(length=32), default='x86_64'),
    sa.Column('registry', sa.String(length=512)),
    sa.Column('tag', sa.String(length=64), nullable=True),

    # Resource occupation
    sa.Column('occupied_slots', pgsql.JSONB(), nullable=False),
    sa.Column('occupied_shares', pgsql.JSONB(), nullable=False, default={}),  # legacy
    sa.Column('environ', sa.ARRAY(sa.String), nullable=True),
    sa.Column('mounts', sa.ARRAY(sa.String), nullable=True),  # list of list; legacy since 22.03
    sa.Column('mount_map', pgsql.JSONB(), nullable=True, default={}),  # legacy since 22.03
    sa.Column('vfolder_mounts', pgsql.JSONB(), nullable=True),
    sa.Column('attached_devices', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={}),
    sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True),

    # Port mappings
    # If kernel_host is NULL, it is assumed to be same to the agent host or IP.
    sa.Column('kernel_host', sa.String(length=128), nullable=True),
    sa.Column('repl_in_port', sa.Integer(), nullable=False),
    sa.Column('repl_out_port', sa.Integer(), nullable=False),
    sa.Column('stdin_port', sa.Integer(), nullable=False),   # legacy for stream_pty
    sa.Column('stdout_port', sa.Integer(), nullable=False),  # legacy for stream_pty
    sa.Column('service_ports', pgsql.JSONB(), nullable=True),
    sa.Column('preopen_ports', sa.ARRAY(sa.Integer), nullable=True),

    # Lifecycle
    sa.Column('created_at', sa.DateTime(timezone=True),
            server_default=sa.func.now(), index=True),
    sa.Column('terminated_at', sa.DateTime(timezone=True),
            nullable=True, default=sa.null(), index=True),
    sa.Column('starts_at', sa.DateTime(timezone=True),
            nullable=True, default=sa.null()),
    sa.Column('status', EnumType(*KernelStatus),
            default='PENDING',
            server_default='PENDING',
            nullable=False, index=True),
    sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True, index=True),
    sa.Column('status_info', sa.Unicode(), nullable=True, default=sa.null()),
    # status_info contains a kebab-cased string that expresses a summary of the last status change.
    # Examples: "user-requested", "self-terminated", "predicate-checks-failed", "no-available-instances"

    sa.Column('status_data', pgsql.JSONB(), nullable=True, default=sa.null()),
    sa.Column('callback_url', sa.types.UnicodeText, nullable=True, default=sa.null()),

    sa.Column('startup_command', sa.Text, nullable=True),
    sa.Column('result', EnumType('undefined', 'success', 'failure'),
            default='undefined',
            server_default='undefined',
            nullable=False, index=True),
    sa.Column('internal_data', pgsql.JSONB(), nullable=True),
    sa.Column('container_log', sa.LargeBinary(), nullable=True),
    # Resource metrics measured upon termination
    sa.Column('num_queries', sa.BigInteger(), default=0),
    sa.Column('last_stat', pgsql.JSONB(), nullable=True, default=sa.null()),
)


class SessionRow(Base):
    __tablename__ = 'sessions'
    id = sa.Column('id', GUID(), server_default=sa.text('uuid_generate_v4()'), nullable=False)
    creation_id = sa.Column('creation_id', sa.String(length=32), unique=False, index=False)
    name = sa.Column('name', sa.String(length=64), unique=False, index=True)
    session_type = sa.Column('session_type', EnumType('INTERACTIVE', 'BATCH'), server_default='INTERACTIVE', nullable=False)

    cluster_mode = sa.Column('cluster_mode', sa.String(length=16), server_default='SINGLE_NODE', nullable=False)
    cluster_size = sa.Column('cluster_size', sa.Integer(), nullable=False)
    kernel = relationship('KernelRow', back_populates='session')
    
    # Resource ownership
    scaling_group_name = sa.Column('scaling_group', sa.String(length=64), nullable=True)
    scaling_group = relationship('ScalingGroupRow', back_populates='sessions')
    domain_name = sa.Column('domain_name', sa.String(length=64), nullable=False)
    domain = relationship('DomainRow', back_populates='sessions')
    group_id = sa.Column('group_id', GUID(), nullable=False)
    group = relationship('GroupRow', back_populates='sessions')
    user_uuid = sa.Column('user_uuid', GUID(), nullable=False)
    user = relationship('UserRow', back_populates='sessions')
    kp_access_key = sa.Column('access_key', sa.String(length=20), nullable=True)
    access_key = relationship('KeyPairRow', back_populates='sessions')

    # if image_id is null, should find a image field from related kernel row.
    image_id = sa.Column('image', GUID(), nullable=True)
    image = relationship('ImageRow', back_populates='sessions')
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
    status = sa.Column('status', EnumType('PENDING', 'SCHEDULED', 'PREPARING', 'BUILDING', 'PULLING', 'RUNNING', 'RESTARTING', 'RESIZING', 'SUSPENDED', 'TERMINATING', 'TERMINATED', 'ERROR', 'CANCELLED'), server_default='PENDING', nullable=False)
    status_changed = sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True)
    status_info = sa.Column('status_info', sa.Unicode(), nullable=True)
    status_data = sa.Column('status_data', pgsql.JSONB(astext_type=sa.Text()), nullable=True)
    callback_url = sa.Column('callback_url', sa.types.UnicodeText(), nullable=True)

    startup_command = sa.Column('startup_command', sa.Text(), nullable=True)
    result = sa.Column('result', EnumType('UNDEFINED', 'SUCCESS', 'FAILURE'), server_default='UNDEFINED', nullable=False)

    # Resource metrics measured upon termination
    num_queries = sa.Column('num_queries', sa.BigInteger(), nullable=True)
    last_stat = sa.Column('last_stat', pgsql.JSONB(astext_type=sa.Text()), nullable=True)



def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    connection = op.get_bind()
    db_session = Session(connection)

    # Session table
    # op.create_table('sessions',
    #     sa.Column('id', GUID(), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    #     sa.Column('creation_id', sa.String(length=32), nullable=True),
    #     sa.Column('name', sa.String(length=64), nullable=True),
    #     sa.Column('session_type', EnumType('INTERACTIVE', 'BATCH'), server_default='INTERACTIVE', nullable=False),
    #     sa.Column('cluster_mode', sa.String(length=16), server_default='SINGLE_NODE', nullable=False),
    #     sa.Column('cluster_size', sa.Integer(), nullable=False),
    #     sa.Column('scaling_group', sa.String(length=64), nullable=True),
    #     sa.Column('domain_name', sa.String(length=64), nullable=False),
    #     sa.Column('group_id', GUID(), nullable=False),
    #     sa.Column('user_uuid', GUID(), nullable=False),
    #     sa.Column('access_key', sa.String(length=20), nullable=True),
    #     sa.Column('image', GUID(), nullable=True),
    #     sa.Column('tag', sa.String(length=64), nullable=True),
    #     sa.Column('occupying_slots', pgsql.JSONB(), nullable=False),
    #     sa.Column('requested_slots', pgsql.JSONB(), nullable=False),
    #     sa.Column('vfolder_mounts', pgsql.JSONB(), nullable=True),
    #     sa.Column('resource_opts', pgsql.JSONB(), nullable=True, default={}),
    #     sa.Column('bootstrap_script', sa.String(length=16 * 1024), nullable=True),
    #     sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    #     sa.Column('terminated_at', sa.DateTime(timezone=True), nullable=True),
    #     sa.Column('starts_at', sa.DateTime(timezone=True), nullable=True),
    #     sa.Column('status', EnumType('PENDING', 'SCHEDULED', 'PREPARING', 'BUILDING', 'PULLING', 'RUNNING', 'RESTARTING', 'RESIZING', 'SUSPENDED', 'TERMINATING', 'TERMINATED', 'ERROR', 'CANCELLED'), server_default='PENDING', nullable=False),
    #     sa.Column('status_changed', sa.DateTime(timezone=True), nullable=True),
    #     sa.Column('status_info', sa.Unicode(), nullable=True),
    #     sa.Column('status_data', pgsql.JSONB(astext_type=sa.Text()), nullable=True),
    #     sa.Column('callback_url', sa.types.UnicodeText(), nullable=True),
    #     sa.Column('startup_command', sa.Text(), nullable=True),
    #     sa.Column('result', EnumType('UNDEFINED', 'SUCCESS', 'FAILURE'), server_default='UNDEFINED', nullable=False),
    #     sa.Column('num_queries', sa.BigInteger(), nullable=True),
    #     sa.Column('last_stat', pgsql.JSONB(astext_type=sa.Text()), nullable=True),
    #     sa.ForeignKeyConstraint(['access_key'], ['keypairs.access_key'], name=op.f('fk_sessions_access_key_keypairs')),
    #     sa.ForeignKeyConstraint(['domain_name'], ['domains.name'], name=op.f('fk_sessions_domain_name_domains')),
    #     sa.ForeignKeyConstraint(['group_id'], ['groups.id'], name=op.f('fk_sessions_group_id_groups')),
    #     sa.ForeignKeyConstraint(['image'], ['images.id'], name=op.f('fk_sessions_image_images')),
    #     sa.ForeignKeyConstraint(['scaling_group'], ['scaling_groups.name'], name=op.f('fk_sessions_scaling_group_scaling_groups')),
    #     sa.ForeignKeyConstraint(['user_uuid'], ['users.uuid'], name=op.f('fk_sessions_user_uuid_users')),
    #     sa.PrimaryKeyConstraint('id', name=op.f('pk_sessions'))
    # )
    SessionRow.__table__.create(connection)
    op.create_index(op.f('ix_sessions_created_at'), 'sessions', ['created_at'], unique=False)
    op.create_index(op.f('ix_sessions_name'), 'sessions', ['name'], unique=False)
    op.create_index(op.f('ix_sessions_result'), 'sessions', ['result'], unique=False)
    op.create_index(op.f('ix_sessions_scaling_group'), 'sessions', ['scaling_group'], unique=False)
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
            db_session.add_all(creates)
            db_session.commit()


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
    db_session = Session(connection)

    # Kernel table
    op.add_column('kernels', sa.Column('session_creation_id', sa.VARCHAR(length=32), autoincrement=False, nullable=True))
    op.add_column('kernels', sa.Column('cluster_size', sa.INTEGER(), autoincrement=False, nullable=False))
    op.add_column('kernels', sa.Column('session_name', sa.VARCHAR(length=64), autoincrement=False, nullable=True))
    op.add_column('kernels', sa.Column('cluster_mode', sa.VARCHAR(length=16), server_default=sa.text("'SINGLE_NODE'::character varying"), autoincrement=False, nullable=False))
    op.add_column('kernels', sa.Column('session_type', pgsql.ENUM('INTERACTIVE', 'BATCH', name='sessiontypes'), server_default=sa.text("'INTERACTIVE'::sessiontypes"), autoincrement=False, nullable=False))
    op.drop_constraint(op.f('fk_kernels_session_id_sessions'), 'kernels', type_='foreignkey')
    op.create_index('ix_kernels_unique_sess_token', 'kernels', ['access_key', 'session_name'], unique=False)
    op.create_index('ix_kernels_status_role', 'kernels', ['status', 'cluster_role'], unique=False)
    op.create_index('ix_kernels_session_type', 'kernels', ['session_type'], unique=False)
    op.create_index('ix_kernels_session_name', 'kernels', ['session_name'], unique=False)
    op.create_index('ix_kernels_sess_id_role', 'kernels', ['session_id', 'cluster_role'], unique=False)

    session_rows = db_session.query(SessionRow).all()
    sess_map = {str(sess.id): sess for sess in session_rows}

    query = (
        sa.select([kernels])
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
