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
from sqlalchemy.dialects import postgresql
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


    # insert kernels not in 'TERMINATED' or 'CANCELLED' status with mounts info.
    query = """
    SELECT id, mounts
    FROM kernels
    WHERE status NOT IN ('TERMINATED', 'CANCELLED')
    """


    connection = op.get_bind()
    results = connection.execute(query)
    mounts_info = []
    for row in results:
        mounts_id_list = [mount[2] for mount in row['mounts']]
        kernel_id = row['id']
        
        for mounts_id in mounts_id_list:
            # if mounts id contains slash('/') then refine it
            if '/' in mounts_id:
                legacy_path = mounts.split('/')
                path_list = legacy_path[-3:] # path rule
                legacy_path = ''.join(path_list)
                mounts_id = uuid.UUID(legacy_path)    
            if len(mounts_id) > 0 : # if kernel mounts at least one vfolder
                mounts_info.append(
                    {
                        'vfolder': mounts_id,
                        'kernel': kernel_id
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
    pass
    # ### end Alembic commands ###
