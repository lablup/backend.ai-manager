from abc import ABCMeta, abstractmethod
import asyncio
from decimal import Decimal
import enum

import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata
from .kernel import SessionCreationRequest
from .agent import agents, ResourceSlot


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


__all__ = ('ScalingEventType', 'ScalingGroup', 'AWSDefaultScalingDriver')


class ScalingEventType(enum.Enum):
    SESSION_CREATED = 1
    SESSION_TERMINATED = 2
    AGENT_JOINED = 3
    AGENT_LEFT = 4


@attr.s(auto_attribs=True, slots=True)
class ScalingEvent:
    type: ScalingEventType
    session_id: str = None
    agent_id: str = None


class ScalingGroup:

    def __init__(self, group_id, registry, scaling_driver=None, job_scheduler=None):
        self.group_id = group_id
        self.registry = registry
        if not scaling_driver:
            scaling_driver = AWSDefaultScalingDriver()
        self.scaling_driver = scaling_driver
        if not job_scheduler:
            job_scheduler = SimpleFIFOJobScheduler()
        self.job_scheduler = job_scheduler
        # TODO: set resource_margin
        # TODO: create instances fit to resource_margin

    async def get_pending_requests(self):
        # TODO: get pending kernels from DB
        return []

    async def register_request(self, request: SessionCreationRequest):
        # TODO: save pending kernel DB
        return {
            'id': '',
            'sess_id': request.sess_id,
            'agent': None,
            'agent_addr': None,
            'kernel_host': None,
            'lang': request.lang,
        }

    async def get_belonging_agents(self):
        # TODO: get agents from DB
        return []

    async def get_available_shares(self):
        async with self.registry.dbpool.acquire() as conn, conn.begin():
            cols = [agents.c.id, agents.c.mem_slots, agents.c.used_mem_slots,
                    agents.c.cpu_slots, agents.c.used_cpu_slots,
                    agents.c.gpu_slots, agents.c.used_gpu_slots]
            query = (sa.select(cols)
                       .select_from(agents)
                       .where(agents.c.scaling_group == self.group_id))

            available_shares = []

            async for row in conn.execute(query):
                available_shares.append(ResourceSlot(
                    id=row.id,
                    mem=row.mem_slots - row.used_mem_slots,
                    cpu=row.cpu_slots - row.used_cpu_slots,
                    gpu=row.gpu_slots - row.used_gpu_slots,
                ))
            return available_shares

    async def get_required_shares(self, lang):
        # TODO: return proper required shares depending on lang.
        return ResourceSlot()

    async def minimum_prepared_shares(self):
        return ResourceSlot(
            mem=Decimal(0),
            cpu=Decimal(0),
            gpu=Decimal(0),
        )

    async def schedule(self):
        scheduled_requests = await self.job_scheduler.schedule(self)
        await asyncio.gather(*[self._process_request(agent_id, request)
                               for agent_id, request in scheduled_requests])

    async def _process_request(self, agent_id, request):
        await self.registry.create_session(agent_id, request)


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
        self.config_server = config_server

        # prevent race-condition of asynchronous agent launches
        # and scale-up decisions *during* scaling

        # please propose a good design for this!
        # (instead of simple flag)
        self._pending_scaling = False

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

        available_shares = await scaling_group.get_available_shares()
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
            if requested_shares > remaining_shares:
                agent_type = self.choose_agent_type(
                    remaining_shares - requested_shares)
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
    async def schedule(self, scaling_group: ScalingGroup):
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    async def schedule(self, scaling_group):
        # pseudo-code
        pending_requests = await scaling_group.get_pending_requests()
        schedulable_requests = []
        available_shares = scaling_group.get_available_shares()
        for rqst in pending_requests.order_by('created_at', desc=True):
            if rqst.can_run_with(available_shares):
                schedulable_requests.append(rqst)
                available_shares -= rqst.required_resource
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
