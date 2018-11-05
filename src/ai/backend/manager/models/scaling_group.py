from abc import ABCMeta, abstractmethod
import enum
from typing import Iterable

import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata
from .kernel import SessionCreationRequest


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


class ScalingEventType(enum.Enum):
    SESSION_CREATED = 1
    SESSION_TERMINATED = 2
    AGENT_JOINED = 3
    AGENT_LEFT = 4


@attr.s(auto_atrribs=True, slots=True)
class ScalingEvent:
    type: ScalingEventType
    session_id: str = None
    agent_id: str = None


class ScalingGroup:

    def __init__(self, config, scaling_driver, job_scheduler):
        self.config = config
        self.scaling_driver = scaling_driver
        self.job_scheduler = job_scheduler

    async def current_available_shares(self):
        raise NotImplementedError  # noqa

    async def minimum_prepared_shares(self):
        raise NotImplementedError  # noqa

    async def schedule(self):
        raise NotImplementedError  # noqa


class AbstractScalingDriver(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def scale(self, event: ScalingEvent,
                    pending_requests: Iterable[SessionCreationRequest],
                    scaling_group: ScalingGroup):
        '''
        This callback method is invoked.
        '''
        raise NotImplementedError  # noqa


class AWSDefaultScalingDriver(AbstractScalingDriver):

    def __init__(self, config_server):
        super().__init__(config_server)
        self.config = config_server

        # prevent race-condition of asynchronous agent launches
        # and scale-up decisions *during* scaling

        # please propose a good design for this!
        # (instead of simple flag)
        self._pending_scaling = False

    @abstractmethod
    async def scale(self, event: ScalingEvent,
                    pending_requests: Iterable[SessionCreationRequest],
                    scaling_group: ScalingGroup):

        # pseudo-code
        if self._pending_scaling:
            if event.type == ScalingEventType.AGENT_JOINED:
                if event.agent_id in self._pending_created:
                    self._pending_scaling = False
            elif event.type == ScalingEventType.AGENT_LEFT:
                if event.agent_id in self._pending_terminated:
                    self._pending_scaling = False
            return

        available_shares = await scaling_group.current_available_shares()
        minimum_prepared_shares = await scaling_group.minimum_prepared_shares()
        remaining_shares = available_shares - minimum_prepared_shares

        if len(pending_requests) == 0:
            if any(lambda s: s < 0, remaining_shares):
                agent_type = self.choose_agent_type(remaining_shares)
                count = ...  # TODO: how many to spawn??
                self._pending_scaling = True
                await self.add_agents(agent_type, count)
            else:
                agent_ids = self.choose_agents_to_terminate(...)
                self._pending_scaling = True
                await self.remove_agents(agent_ids)
        else:
            # just an example.......
            requested_shares = sum(map(lambda rqst: rqst.required_resource,
                                       pending_requests))
            if requested_shares > available_shares + some_margin:
                agent_type = self.choose_agent_type(
                    available_shares - requested_shares)
                self._pending_scaling = True
                await self.add_agents(agent_type, count)

    # pseudo-code

    def choose_agent_type(self, remaining_shares):
        most_deficient_share = min(remaining_shares, key=lambda s: s.value)
        if most_deficient_share.type == 'cuda':
            return 'p2.xlarge'
        else:
            return 't2.2xlarge'

    async def add_agents(self, agent_type, count):
        launch_config = get_launch_template(agent_type)
        await self.libcloud.create_instance(launch_template, count)

    async def remove_agents(self, agent_ids):
        pass


class AbstractJobScheduler(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def schedule(self, pending_requests: Iterable[SessionCreationRequest],
                       scaling_group: ScalingGroup):
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    async def schedule(self, pending_requests, scaling_group):
        # pseudo-code
        schedulable_requests = []
        remaining_resource = scaling_group.current_available_shares()
        for rqst in pending_requests.order_by('created_at', desc=True):
            if rqst.can_run_with(remaining_resource):
                schedulable_requests.append(rqst)
                remaining_resource -= rqst.required_resource
        return schedulable_requests


scaling_groups = sa.Table(
    'scaling_groups', metadata,
    sa.Column('name', sa.String(length=64), unique=True, index=True),
    sa.Column('is_active', sa.Boolean, index=True,
              server_default=true()),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('driver', sa.String(length=64), nullable=False),
    sa.Column('job_scheduler', sa.String(length=64), nullable=False),
    sa.Column('params', sa.JSONB(), nullable=False),
)


def ensure_default():
    pass
