from abc import ABCMeta, abstractmethod
import asyncio
from decimal import Decimal
import enum
import math
from typing import Iterable, List, Tuple
import uuid

import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata
from .kernel import kernels, KernelStatus, SessionCreationRequest
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


__all__ = ('ScalingEventType', 'ScalingEvent',
           'ScalingGroup', 'AWSDefaultScalingDriver')


class ScalingEventType(enum.Enum):
    SESSION_CREATED = 1
    SESSION_TERMINATED = 2
    AGENT_JOINED = 3
    AGENT_LEFT = 4


@attr.s(auto_attribs=True, slots=True)
class ScalingEvent:
    type: ScalingEventType
    sess_id: str = None
    agent_id: str = None


class ScalingGroup:

    def __init__(self, group_id, registry, scaling_driver=None, job_scheduler=None):
        self.group_id = group_id
        self.registry = registry
        if not scaling_driver:
            scaling_driver = AWSDefaultScalingDriver(registry.config_server)
        self.scaling_driver = scaling_driver
        if not job_scheduler:
            job_scheduler = SimpleFIFOJobScheduler()
        self.job_scheduler = job_scheduler

    async def get_pending_requests(self):
        async with self.registry.dbpool.acquire() as conn, conn.begin():
            query = (sa.select('*')
                     .select_from(kernels)
                     .where(kernels.c.status == KernelStatus.PENDING))

            requests = []
            async for row in await conn.execute(query):
                request = SessionCreationRequest.from_row(row)
                requests.append(request)

            return requests

    async def register_request(self, request: SessionCreationRequest):
        async with self.registry.dbpool.acquire() as conn, conn.begin():
            # Register kernel.
            kernel_id = uuid.uuid4()
            request.kernel_id = kernel_id

            kernel_info_base = {
                'kernel_id'
                'status': KernelStatus.PENDING,
                'role': 'master',
                'agent': None,
                'agent_addr': '',
                'cpu_set': [],
                'gpu_set': [],
                'kernel_host': None,
                'repl_in_port': 0,
                'repl_out_port': 0,
                'stdin_port': 0,
                'stdout_port': 0,
            }

            kernel_info = request.serialize()
            kernel_info.update(kernel_info_base)
            query = kernels.insert().values(kernel_info)
            result = await conn.execute(query)
            assert result.rowcount == 1

            return {
                'id': '',
                'sess_id': request.sess_id,
                'agent': None,
                'agent_addr': None,
                'kernel_host': None,
                'lang': kernel_info['lang'],
            }

    async def get_belonging_agents(self, idle=False):
        async with self.registry.dbpool.acquire() as conn, conn.begin():
            if idle:
                j = sa.join(agents, kernels,
                            agents.c.id == kernels.c.agent, isouter=True)
                query = (sa.select([agents.c.id])
                           .select_from(j)
                           .where((agents.c.scaling_group == self.group_id) &
                                  kernels.c.id.is_(None)))
            else :
                query = (sa.select([agents.c.id])
                           .select_from(agents)
                           .where(agents.c.scaling_group == self.group_id))

            agent_ids = []
            async for row in conn.execute(query):
                agent_ids.append(row.id)

            return agent_ids

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
        tokens = lang.split('/')
        name, tag = tokens[-1].split(':')
        return await self.registry.config_server.get_image_required_slots(name, tag)

    async def schedule(self):
        pending_requests = await self.get_pending_requests()
        schedule_info = await self.job_scheduler.schedule(self, pending_requests)
        scheduled_requests = map(lambda x: x[1], schedule_info)
        remaining_requests = set(pending_requests) - set(scheduled_requests)

        # Scaling
        #
        # It is possible that scaling driver re-schedule requests
        # by optimizing utilization of agents.
        schedule_info = await self.scaling_driver.scale(
            self, schedule_info, remaining_requests)

        # Scheduling
        if schedule_info:
            await asyncio.gather(*[self._process_request(agent_id, request)
                                   for agent_id, request in schedule_info],
                                 return_exceptions=True)

    async def _process_request(self, agent_id, request):
        return await self.registry.create_session(agent_id, request)


class AbstractScalingDriver(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationRequest]]
                    = None,
                    pending_requests: Iterable[SessionCreationRequest] = None):
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

    async def minimum_prepared_shares(self):
        shares_unit = ResourceSlot(
            cpu=Decimal(1),
            mem=Decimal(1.0),
            gpu=Decimal(0),
        )
        required_unit_count = 3
        return [(shares_unit, required_unit_count)]

    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationRequest]]
                    = None,
                    pending_requests: Iterable[SessionCreationRequest] = None):
        if schedule_info is None:
            schedule_info = []
        if pending_requests is None:
            pending_requests = []

        if not pending_requests:
            schedule_info = await self.scale_in(scaling_group,
                                                schedule_info)
        else:
            await self.scale_out(scaling_group, pending_requests)

        return schedule_info

    async def scale_out(self, scaling_group: ScalingGroup,
                        pending_requests: Iterable[SessionCreationRequest]):
        assert pending_requests is not None
        scale_out_info = await self.get_scale_out_info(scaling_group,
                                                       pending_requests)
        await self.add_agents(scale_out_info)

    async def scale_in(self, scaling_group: ScalingGroup,
                       schedule_info: Iterable[Tuple[str, SessionCreationRequest]]):
        assert schedule_info is not None
        agents_to_terminate = self.get_agents_to_remove(schedule_info,
                                                        scaling_group)
        await self.remove_agents(agents_to_terminate)
        return schedule_info

    async def get_scale_out_info(self, scaling_group, pending_requests):
        # TODO: enhance get_scale_out_info logic.
        # pseudo-code
        available_shares = await scaling_group.get_available_shares()
        minimum_prepared_shares = await scaling_group.minimum_prepared_shares()
        remaining_shares = available_shares - minimum_prepared_shares

        required_shares = await asyncio.gather(
            *[scaling_group.get_required_shares(rqst.lang)
              for rqst in pending_requests]
        )
        required_shares = sum(required_shares)
        required_shares -= remaining_shares

        if required_shares > SOME_THRESHOLD:
            instance_type = 'p2.xlarge'
            instance_shares = ResourceSlot()
        else:
            instance_type = 't2.2xlarge'
            instance_shares = ResourceSlot()
        count = math.ceil(required_shares / instance_shares)
        return [(instance_type, count)]

    async def get_agents_to_remove(self, schedule_info, scaling_group):
        # TODO: enhance get_agents_to_remove logic.
        #       scale_in() currently terminates only idle agents, but
        #       this logic can be enhanced in many ways.
        #       For instance, consider the case where each agent has
        #       only one kernel. It is obviously a waste of resources.
        #       We can "mark" agents as MARK_TERMINATED to prevent
        #       other kernels to be created in that agent and terminate
        #       that agent as soon as all kernels running in the agent is
        #       terminated.
        # pseudo-code
        idle_agents = await scaling_group.get_belonging_agents(idle=True)
        agents_to_remove = set(idle_agents) - set(map(lambda x: x[0], schedule_info))
        return agents_to_remove

    async def add_agents(self, scale_out_info: List[Tuple[str, int]]):
        # Just an example.
        ec2 = boto3.resource('ec2')
        for instance_type, count in scale_out_info:
            ami_id = self.get_ami_id(instance_type)
            ec2.create_instances(ImageId=ami_id, MinCount=1, MaxCount=1)

    async def remove_agents(self, agents_to_remove):
        # Just an example.
        ec2 = boto3.client('ec2')
        ec2.stop_instances(InstanceIds=[agents_to_remove], DryRun=True)

    async def get_ami_id(self, instance_type):
        # Just an example.
        ami_id_map = {
            'p2.xlarge': 'p2.xlarge-ami-id',
            't2.2xlarge': 't2.2xlarge-ami-id',
        }
        assert instance_type in ami_id_map, \
            f'Not supported instance type: {instance_type}'
        return ami_id_map[instance_type]


class AbstractJobScheduler(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def schedule(self, scaling_group: ScalingGroup,
                       pending_requests: Iterable[SessionCreationRequest]):
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    async def schedule(self, scaling_group, pending_requests):
        # pseudo-code
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
