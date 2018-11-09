from abc import ABCMeta, abstractmethod
import asyncio
from collections import defaultdict
from decimal import Decimal
import enum
from functools import reduce
import math
from typing import Iterable, List, Tuple
import os
import uuid

import aiobotocore
import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata
from .kernel import kernels, KernelStatus, SessionCreationRequest
from .agent import agents, ResourceSlot
from ...gateway.exceptions import InvalidAPIParameters


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


__all__ = ('ScalingEventType', 'ScalingEvent', 'SessionCreationJob',
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


@attr.s(auto_attribs=True, slots=True)
class SessionCreationJob:
    kernel_id: str
    resources: ResourceSlot  # TODO: change into a new data structure type.


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

    async def get_pending_jobs(self):
        async with self.registry.dbpool.acquire() as conn, conn.begin():
            query = (sa.select('*')
                     .select_from(kernels)
                     .where(kernels.c.status == KernelStatus.PENDING))

            jobs = []
            async for row in await conn.execute(query):
                job = SessionCreationJob(
                    kernel_id=row.id,
                    resources=ResourceSlot(
                        cpu=row.cpu_slot,
                        mem=row.mem_slot,
                        gpu=row.gpu_slot,
                    )
                )
                jobs.append(job)

            return jobs

    async def register_request(self, request: SessionCreationRequest):
        async with self.registry.dbpool.acquire() as conn, conn.begin():

            # Apply resource limits.
            name, tag = request.lang.split(':')
            max_allowed_slot = \
                await self.registry.config_server.get_image_required_slots(name, tag)
            creation_config = request.creation_config
            try:
                cpu_share = Decimal(0)
                if max_allowed_slot.cpu is not None:
                    cpu_share = min(
                        max_allowed_slot.cpu,
                        Decimal(creation_config.get('instanceCores') or Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceCores'] is not None
                    cpu_share = Decimal(creation_config['instanceCores'])

                mem_share = Decimal(0)
                if max_allowed_slot.mem is not None:
                    mem_share = min(
                        max_allowed_slot.mem,
                        Decimal(creation_config.get('instanceMemory') or Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceMemory'] is not None
                    mem_share = Decimal(creation_config['instanceMemory'])

                gpu_share = Decimal(0)
                if max_allowed_slot.gpu is not None:
                    gpu_share = min(
                        max_allowed_slot.gpu,
                        Decimal(creation_config.get('instanceGPUs') or Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceGPUs'] is not None
                    gpu_share = Decimal(creation_config['instanceGPUs'])
            except (AssertionError, KeyError):
                msg = ('You have missing resource limits that must be specified. '
                       'If the server does not have default resource configurations, '
                       'you must specify all resource limits by yourself.')
                raise InvalidAPIParameters(msg)

            # Register request.
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
            kernel_info.update({
                'cpu_slot': cpu_share,
                'mem_slot': mem_share,
                'gpu_slot': gpu_share,
            })
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
        # Planning Scheduling
        pending_jobs = await self.get_pending_jobs()
        schedule_info = await self.job_scheduler.schedule(self, pending_jobs)

        # Scaling
        #
        # It is possible that scaling driver re-schedule jobs
        # by optimizing utilization of agents.
        scheduled_jobs = map(lambda x: x[1], schedule_info)
        remaining_jobs = set(pending_jobs) - set(scheduled_jobs)
        schedule_info = await self.scaling_driver.scale(
            self, schedule_info, remaining_jobs)

        # Process scheduled jobs
        if schedule_info:
            await asyncio.gather(*[self._process_job(agent_id, job)
                                   for agent_id, job in schedule_info],
                                 return_exceptions=True)

    async def _process_job(self, agent_id, job):
        return await self.registry.create_session(agent_id, job.kernel_id)


class AbstractScalingDriver(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationJob]]
                    = None,
                    pending_jobs: Iterable[SessionCreationJob] = None):
        '''
        This callback method is invoked.
        '''
        raise NotImplementedError  # noqa


class BasicScalingDriver(AbstractScalingDriver):

    def __init__(self):
        # prevent race-condition of asynchronous agent launches
        # and scale-up decisions *during* scaling

        # please propose a good design for this!
        # (instead of simple flag)
        self._pending_scaling = False

        self.resource_precedence = ['gpu', 'cpu', 'mem']

        cpu_shares_unit = ResourceSlot(
            cpu=Decimal(1),
            mem=Decimal(1.0),
            gpu=Decimal(0),
        )
        cpu_required_unit_count = 3
        gpu_shares_unit = ResourceSlot(
            cpu=Decimal(1),
            mem=Decimal(1.0),
            gpu=Decimal(1),
        )
        gpu_required_unit_count = 1
        self.min_prepared_shares = [(cpu_shares_unit, cpu_required_unit_count),
                                    (gpu_shares_unit, gpu_required_unit_count)]

    @property
    def instance_info(self):
        instance_info = defaultdict(list)
        for instance in self.available_instances:
            if instance[1].gpu != 0:
                instance_info['gpu'].append(instance)
            else:
                instance_info['cpu'].append(instance)
                instance_info['mem'].append(instance)
        for resource_type in instance_info.keys():
            instance_info[resource_type].sort(
                key=lambda o: getattr(o[1], resource_type))
        return instance_info

    def get_required_buffer_jobs(self):
        buffer_jobs = []
        for share_unit, min_count in self.min_prepared_shares:
            for _ in range(min_count):
                job = SessionCreationJob(
                    kernel_id=uuid.uuid4().hex,
                    resources=ResourceSlot(
                        cpu=share_unit.cpu,
                        mem=share_unit.mem,
                        gpu=share_unit.gpu,
                    )
                )
                buffer_jobs.append(job)
        return buffer_jobs

    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationJob]] = None,
                    pending_jobs: Iterable[SessionCreationJob] = None):

        if schedule_info is None:
            schedule_info = []
        if pending_jobs is None:
            pending_jobs = []

        required_buffer_jobs = self.get_required_buffer_jobs()

        if pending_jobs:
            await self.scale_out(pending_jobs + required_buffer_jobs)
        else:
            # Assert that minimum prepared shares condition is satisfied.
            curr_buffer_jobs = await scaling_group.job_scheduler.schedule(
                scaling_group, required_buffer_jobs)
            curr_buffer_jobs = map(lambda x: x[1], curr_buffer_jobs)
            deficient_buffer_jobs = set(required_buffer_jobs) - set(curr_buffer_jobs)
            if deficient_buffer_jobs:
                await self.scale_out(deficient_buffer_jobs)
            else:
                schedule_info = await self.scale_in(scaling_group, schedule_info)

        return schedule_info

    async def scale_out(self, pending_jobs: Iterable[SessionCreationJob]):
        # 1. Assume that no # limit of instances,
        # only t2.2xlarge & p2.xlarge, and only cpu, mem, gpu.
        assert pending_jobs is not None

        scale_out_info = []
        for resource_type in self.resource_precedence:
            target_jobs = filter(lambda job: job.resources.gpu != 0, pending_jobs)
            instance_type, instance_spec = self.instance_info[resource_type][0]

            def _acc_resources(acc: ResourceSlot, job: SessionCreationJob):
                return ResourceSlot(
                    cpu=acc.cpu + job.resources.cpu,
                    mem=acc.mem + job.resources.mem,
                    gpu=acc.gpu + job.resources.gpu,
                )

            resource_sum = reduce(_acc_resources, target_jobs,
                                  initial=ResourceSlot())
            instance_num = max(math.ceil(resource_sum.cpu / instance_spec[1].cpu),
                               math.ceil(resource_sum.mem / instance_spec[1].mem),
                               math.ceil(resource_sum.gpu / instance_spec[1].gpu))

            scale_out_info.append((instance_type, instance_num))

        await self.add_agents(scale_out_info)
        # 2, Now consider multiple instances for each resource types.
        # 3. Now consider # limit of instances.
        # 4. Now consider other types of resources, e.g. tpu.
        # 5. Now consider "marked-to-be-terminated" agents if exists.

    async def scale_in(self, scaling_group: ScalingGroup,
                       schedule_info: Iterable[Tuple[str, SessionCreationJob]]):

        assert schedule_info is not None

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
        await self.remove_agents(agents_to_remove)

        return schedule_info


class AbstractVendorScalingDriverMixin:

    @abstractmethod
    async def add_agents(self, scale_out_info: List[Tuple[str, int]]):
        raise NotImplementedError

    @abstractmethod
    async def remove_agents(self, agents_to_remove: List[Tuple[str, int]]):
        raise NotImplementedError


aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'dummy-access-key')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'dummy-secret-key')
aws_region = os.environ.get('AWS_REGION', 'ap-northeast-1')
aws_launch_template_name_format = os.environ.get('AWS_LAUNCH_TEMPLATE_NAME_FORMAT',
                                                 'backend.ai/agent/{}/v1.4')


class AWSScalingDriverMixin(AbstractVendorScalingDriverMixin):

    available_instances: List[Tuple[str, ResourceSlot]] = [
        ('t2.2xlarge', ResourceSlot(
            cpu=Decimal(8),
            mem=Decimal(32.0),
            gpu=Decimal(0),
        )),
        ('p2.xlarge', ResourceSlot(
            cpu=Decimal(4),
            mem=Decimal(61.0),
            gpu=Decimal(1),
        ))
    ]

    async def add_agents(self, scale_out_info: List[Tuple[str, int]]):
        session = aiobotocore.get_session()
        async with session.create_client('ec2', region_name=aws_region,
                                         aws_secret_access_key=aws_secret_key,
                                         aws_access_key_id=aws_access_key) as client:
            tasks = []
            for instance_type, instance_num in scale_out_info:
                launch_template_name = self.get_launch_template_name(instance_type)
                launch_template = {
                    'LaunchTemplateName': launch_template_name,
                }
                coro = client.run_instances(LaunchTemplate=launch_template,
                                            MinCount=instance_num,
                                            MaxCount=instance_num)
                tasks.append(coro)
            # TODO: handle exceptions
            await asyncio.gather(*tasks, return_exceptions=True)

    async def remove_agents(self, agents_to_remove):
        session = aiobotocore.get_session()
        async with session.create_client('ec2', region_name=aws_region,
                                         aws_secret_access_key=aws_secret_key,
                                         aws_access_key_id=aws_access_key) as client:
            # TODO: handle exceptions
            await client.run_instances(InstanceIds=agents_to_remove)

    def get_launch_template_name(self, instance_type):
        if instance_type == 't2.2xlarge':
            resource_type = 'cpu'
        elif instance_type == 'p2.xlarge':
            resource_type = 'gpu'
        else:
            raise ValueError(f'Invalid instance type: {instance_type}')
        return aws_launch_template_name_format.format(resource_type)


class AWSDefaultScalingDriver(AWSScalingDriverMixin, BasicScalingDriver):
    pass


class AbstractJobScheduler(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def schedule(self, scaling_group: ScalingGroup,
                       pending_jobs: Iterable[SessionCreationJob]):
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    async def schedule(self, scaling_group, pending_jobs):
        # pseudo-code
        schedulable_jobs = []
        available_shares = scaling_group.get_available_shares()
        for rqst in pending_jobs.order_by('created_at', desc=True):
            if rqst.can_run_with(available_shares):
                schedulable_jobs.append(rqst)
                available_shares -= rqst.required_resource
        return schedulable_jobs


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
