"""update vfolder attachment table

Revision ID: 32b133be36cc
Revises: 8679d0a7e22b
Create Date: 2021-04-13 15:20:19.452063

"""
import uuid
import re
import textwrap
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from ai.backend.manager.models import VFolderPermission
from ai.backend.manager.models.base import (
    convention, EnumValueType, IDColumn, GUID
)

# revision identifiers, used by Alembic.
revision = '32b133be36cc'
down_revision = '8679d0a7e22b'
branch_labels = None
depends_on = None


def upgrade():
    metadata = sa.MetaData(bind=op.get_bind())
    vfolder_attachment = sa.Table(
        'vfolder_attachment', metadata,
        sa.Column('vfolder', GUID,
              sa.ForeignKey('vfolders.id', onupdate='CASCADE', ondelete='CASCADE'),
              nullable=False),
        sa.Column('kernel', GUID,
                sa.ForeignKey('kernels.id', onupdate='CASCADE', ondelete='CASCADE'),
                nullable=False),
        sa.PrimaryKeyConstraint('vfolder', 'kernel'),
    )

    op.add_column('vfolder_attachment', sa.Column("host", sa.String(length=128), nullable=False,))
    op.add_column('vfolder_attachment', sa.Column("permission",
                EnumValueType(VFolderPermission),
                default=VFolderPermission.READ_WRITE,
                nullable=False,
            ))
    op.add_column('vfolder_attachment', sa.Column("mountspec", sa.String(length=512), nullable=False,))
    op.add_column('vfolder_attachment', sa.Column("mount_map", pgsql.JSONB(), nullable=True, default={},))

    # insert kernels not in 'TERMINATED' or 'CANCELLED' status with mounts info.
    query = """
    SELECT id, mounts, mount_map
    FROM kernels
    WHERE status NOT IN ('TERMINATED', 'CANCELLED')
    """


    connection = op.get_bind()
    results = connection.execute(query)
    mounts_info = []
    for row in results:
        kernel_id = row['id']
        mount_map = row['mount_map']
        for mount in row['mounts']:
            host, vfolder, permission, mountspec = mount[1:]
            # if mounts id contains slash('/') then refine it
            if '/' in vfolder:
                # apply mountspec if value of mountspec is empty
                mountspec = mountspec if mountspec else vfolder
                legacy_path = ''.join(vfolder.split('/')[-3:])
                vfolder = uuid.UUID(legacy_path)
            mounts_info.append(
                {
                    'vfolder': vfolder,
                    'kernel': kernel_id,
                    'host': host,
                    'permission': permission,
                    'mountspec': mountspec,
                    'mount_map': mount_map
                }
            )
        if not mounts_info:
            connection.execute(vfolder_attachment.insert(), mounts_info)


    # delete kernels in 'TERMINATED' or 'CANCELLED' status with mounts info.
    query = """
    SELECT id
    FROM kernels
    WHERE status IN ('TERMINATED', 'CANCELLED')
    """

    connection = op.get_bind()
    results = connection.execute(query)
    terminated_kernels = []
    if results is not None:
        for row in results:
            terminated_kernels.append(row['id'])

        query = (sa.delete(vfolder_attachment)
                   .where(vfolder_attachment.c.kernel.in_(terminated_kernels)))
        connection.execute(query)
    # ### end Alembic commands ###


def downgrade():
    op.drop_column('vfolder_attachment', 'host')
    op.drop_column('vfolder_attachment', 'permission')
    op.drop_column('vfolder_attachment', 'mountspec')
    op.drop_column('vfolder_attachment', 'mount_map')
    # ### end Alembic commands ###
