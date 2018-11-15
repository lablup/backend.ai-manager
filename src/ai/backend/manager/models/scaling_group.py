from abc import ABCMeta, abstractmethod
import asyncio
from collections import defaultdict
from datetime import datetime
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
from ...gateway.utils import reenter_txn


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
           'ScalingGroup', 'AWSDefaultScalingDriver', 'scaling_groups')


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
    created_at: datetime


class ScalingGroup:

    def __init__(self, name, registry, scaling_driver=None, job_scheduler=None):
        self.name = name
        self.registry = registry
        if not scaling_driver:
            scaling_driver = AWSDefaultScalingDriver()
        self.scaling_driver = scaling_driver
        if not job_scheduler:
            job_scheduler = SimpleFIFOJobScheduler()
        self.job_scheduler = job_scheduler

    async def get_pending_jobs(self, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            query = (sa.select('*')
                       .select_from(kernels)
                       .where(kernels.c.status == KernelStatus.RESIZING))

            jobs = []
            async for row in await conn.execute(query):
                job = SessionCreationJob(
                    kernel_id=row.id,
                    resources=ResourceSlot(
                        cpu=Decimal(row.cpu_slot),
                        mem=Decimal(row.mem_slot),
                        gpu=Decimal(row.gpu_slot),
                    ),
                    created_at=row.created_at,
                )
                jobs.append(job)

            return jobs

    async def register_request(self, request: SessionCreationRequest, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
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
                'status': KernelStatus.RESIZING,
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
                'id': request.kernel_id,
                'sess_id': request.sess_id,
                'agent': None,
                'agent_addr': None,
                'kernel_host': None,
                'lang': kernel_info['lang'],
            }

    async def get_belonging_agents(self, idle=False, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            if idle:
                j = sa.join(agents, kernels,
                            agents.c.id == kernels.c.agent, isouter=True)
                query = (sa.select([agents.c.id])
                           .select_from(j)
                           .where((agents.c.scaling_group == self.name) &
                                  kernels.c.id.is_(None)))
            else :
                query = (sa.select([agents.c.id])
                           .select_from(agents)
                           .where(agents.c.scaling_group == self.name))

            agent_ids = []
            async for row in conn.execute(query):
                agent_ids.append(row.id)

            return agent_ids

    async def get_available_shares(self, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            cols = [agents.c.id, agents.c.mem_slots, agents.c.used_mem_slots,
                    agents.c.cpu_slots, agents.c.used_cpu_slots,
                    agents.c.gpu_slots, agents.c.used_gpu_slots]
            query = (sa.select(cols)
                       .select_from(agents)
                       .where(agents.c.scaling_group == self.name))

            available_shares = []
            async for row in conn.execute(query):
                available_shares.append(ResourceSlot(
                    id=row.id,
                    mem=Decimal(row.mem_slots - row.used_mem_slots),
                    cpu=Decimal(row.cpu_slots - row.used_cpu_slots),
                    gpu=Decimal(row.gpu_slots - row.used_gpu_slots),
                ))
            return available_shares

    async def get_required_shares(self, lang):
        tokens = lang.split('/')
        name, tag = tokens[-1].split(':')
        return await self.registry.config_server.get_image_required_slots(name, tag)

    async def schedule(self, conn=None):
        # Planning Scheduling
        pending_jobs = await self.get_pending_jobs(conn=conn)
        schedule_info = await self.job_scheduler.schedule(self, pending_jobs,
                                                          conn=conn)
        # Scaling
        #
        # It is possible that scaling driver re-schedule jobs
        # by optimizing utilization of agents.
        scheduled_job_ids = map(lambda x: x[1].kernel_id, schedule_info)
        remaining_jobs = [job for job in pending_jobs
                          if job.kernel_id not in scheduled_job_ids]
        schedule_info = await self.scaling_driver.scale(
            self, schedule_info, remaining_jobs, conn=conn)

        # Process scheduled jobs
        if schedule_info:
            await asyncio.gather(*[self._process_job(agent_id, job, conn=conn)
                                   for agent_id, job in schedule_info],
                                 return_exceptions=True)

    async def _process_job(self, agent_id, job, conn=None):
        return await self.registry.create_session(agent_id, job.kernel_id, conn=conn)


class AbstractScalingDriver(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationJob]]
                    = None,
                    pending_jobs: Iterable[SessionCreationJob] = None,
                    conn=None):
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
                    ),
                    created_at=datetime.now(),
                )
                buffer_jobs.append(job)
        return buffer_jobs

    async def scale(self, scaling_group: ScalingGroup,
                    schedule_info: Iterable[Tuple[str, SessionCreationJob]] = None,
                    pending_jobs: Iterable[SessionCreationJob] = None,
                    conn=None):

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
                scaling_group, required_buffer_jobs, conn=conn)
            curr_buffer_job_ids = set(map(lambda x: x[1].kernel_id,
                                          curr_buffer_jobs))
            deficient_buffer_jobs = [job for job in required_buffer_jobs
                                     if job.kernel_id not in curr_buffer_job_ids]

            if deficient_buffer_jobs:
                await self.scale_out(deficient_buffer_jobs)
            else:
                schedule_info = await self.scale_in(scaling_group, schedule_info,
                                                    conn=conn)

        return schedule_info

    async def scale_out(self, pending_jobs: Iterable[SessionCreationJob]):
        # 1. Assume that no # limit of instances,
        # only t2.2xlarge & p2.xlarge, and only cpu, mem, gpu.
        raise RuntimeError
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
                       schedule_info: Iterable[Tuple[str, SessionCreationJob]],
                       conn=None):

        return schedule_info
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
        idle_agents = await scaling_group.get_belonging_agents(idle=True, conn=conn)
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
                # TODO: set tags
                # refs: https://botocore.amazonaws.com/v1/documentation/api/latest
                #       /reference/services/ec2.html#EC2.Client.run_instances,
                #       'TagSpecifications' kwargs
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
                       pending_jobs: Iterable[SessionCreationJob],
                       conn=None):
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    async def schedule(self, scaling_group, pending_jobs, conn=None):
        available_agent_infos = await scaling_group.get_available_shares(conn=conn)
        if not available_agent_infos:
            return []
        list(pending_jobs).sort(key=lambda _job: _job.created_at, reverse=True)

        async with reenter_txn(scaling_group.registry.dbpool, conn) as conn:
            scheduled_jobs = []
            curr_agent_info = available_agent_infos.pop(0)
            for job in pending_jobs:
                while curr_agent_info.cpu < job.resources.cpu or \
                        curr_agent_info.mem < job.resources.mem or \
                        curr_agent_info.gpu or job.resources.gpu:
                    if not available_agent_infos:
                        break
                    curr_agent_info = available_agent_infos.pop(0)
                agent_id = curr_agent_info.id
                updates = {
                    'used_cpu_slots': agents.c.used_cpu_slots + job.resources.cpu,
                    'used_mem_slots': agents.c.used_mem_slots + job.resources.mem,
                    'used_gpu_slots': agents.c.used_gpu_slots + job.resources.gpu,
                }
                query = (sa.update(agents)
                           .values(updates)
                           .where(agents.c.id == agent_id))
                await conn.execute(query)

                query = (sa.select([agents.c.addr])
                           .select_from(agents)
                           .where(agents.c.id == agent_id))
                result = await conn.execute(query)
                row = await result.first()
                agent_addr = row.addr

                query = (kernels.update()
                                .values(agent_addr=agent_addr)
                                .where(kernels.c.id == job.kernel_id))
                await conn.execute(query)

                curr_agent_info = ResourceSlot(
                    id=curr_agent_info.id,
                    cpu=Decimal(curr_agent_info.cpu - job.resources.cpu),
                    mem=Decimal(curr_agent_info.mem - job.resources.mem),
                    gpu=Decimal(curr_agent_info.gpu - job.resources.gpu),
                )

                scheduled_jobs.append((agent_id, job))

            return scheduled_jobs


scaling_groups = sa.Table(
    'scaling_groups', metadata,
    sa.Column('name', sa.String(length=64), unique=True, index=True),
    sa.Column('is_active', sa.Boolean, index=True,
              server_default=true()),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('driver', sa.String(length=64), nullable=False),
    sa.Column('job_scheduler', sa.String(length=64), nullable=False),
    # sa.Column('params', sa.JSONB(), nullable=False),
)


def ensure_default():
    pass
