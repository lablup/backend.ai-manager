from abc import abstractmethod

import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata


'''
Some design sketches:

 - Agents will report which scaling group they come from.
 - When spawned, agents will have the following additional instance metadata tags:

   {
     "backend.ai/role": "agent",
     "backend.ai/namespace": "<config.namespace>",
     "backend.ai/scaling-group": "<name>",
   }

 - TODO: How to utilize spot??
'''


class AbstractScalingDriver:

    @abstractmethod
    def allocate(self, available_slots, requested_slots):
        '''
        Allocate agents to run the requested resource.
        '''
        return []

    @abstractmethod
    def init(self, root_app, name, params):
        pass

    @abstractmethod
    def shutdown(self):
        pass


class DefaultFixedScalingDriver(AbstractScalingDriver):

    def allocate(self, available_slots, requested_slots):
        assert requested_slots.num_instances == 1, \
               'Multi-container sessions are not supported'
        agent_id = (max(available_slots,
                        key=lambda s: (s.gpu, s.mem, s.cpu))).id
        return [agent_id]

    def init(self, root_app, name, params):
        pass

    def shutdown(self):
        pass


scaling_groups = sa.Table(
    'scaling_groups', metadata,
    sa.Column('name', sa.String(length=64), unique=True, index=True),
    sa.Column('is_active', sa.Boolean, index=True,
              server_default=true()),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('driver', sa.String(length=64), nullable=False),
    sa.Column('params', sa.JSONB(), nullable=False),
)


def ensure_default():
    pass
