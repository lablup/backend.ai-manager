from abc import ABCMeta, abstractmethod
import asyncio
from collections import defaultdict
from datetime import datetime
from dateutil.tz import tzutc
from decimal import Decimal
import enum
from functools import reduce
import math
from typing import List, Tuple
import os
import uuid

import aiobotocore
import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import true

from .base import metadata
from .kernel import kernels, KernelStatus, SessionCreationRequest
from .agent import agents, ResourceSlot, AgentStatus
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
           'ScalingGroup', 'AWSDefaultScalingDriver', 'scaling_groups',
           'remove_scaling')


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


def _scalings_to_dict(scalings):
    if not scalings:
        return {}

    scalings_dict = {}
    for scaling in scalings:
        instance_type, instance_info = scaling.split(':')
        scalings_dict[instance_type] = instance_info
    return scalings_dict


def merge_scalings(s1, s2):
    s1 = _scalings_to_dict(s1)
    s2 = _scalings_to_dict(s2)
    s = {}
    for k, v in s1.items():
        s[k] = int(v)
    for k, v in s2.items():
        if k in s:
            s[k] += int(v)
        else:
            s[k] = int(v)
    return [f'{k}:{v}' for k, v in s.items()]


def remove_scaling(scalings, instance_type):
    changed = False
    if not scalings:
        return [], changed

    scalings_dict = _scalings_to_dict(scalings)
    if instance_type in scalings_dict:
        scalings_dict[instance_type] -= 1
        if not scalings_dict[instance_type]:
            scalings_dict.pop(instance_type)
        changed = True
    return [f'{k}:{v}' for k, v in scalings_dict.items()], changed


class ScalingGroup:

    def __init__(self, name, registry, scaling_driver=None, job_scheduler=None):
        self.name = name
        self.registry = registry
        if not scaling_driver:
            scaling_driver = AWSDefaultScalingDriver(self)
        self.scaling_driver = scaling_driver
        if not job_scheduler:
            job_scheduler = SimpleFIFOJobScheduler(self)
        self.job_scheduler = job_scheduler

    async def get_pending_jobs(self, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            query = (sa.select('*')
                       .select_from(kernels)
                       .where(kernels.c.status == KernelStatus.PENDING))

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
                        Decimal(creation_config.get('instanceCores') or
                                Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceCores'] is not None
                    cpu_share = Decimal(creation_config['instanceCores'])

                mem_share = Decimal(0)
                if max_allowed_slot.mem is not None:
                    mem_share = min(
                        max_allowed_slot.mem,
                        Decimal(creation_config.get('instanceMemory') or
                                Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceMemory'] is not None
                    mem_share = Decimal(creation_config['instanceMemory'])

                gpu_share = Decimal(0)
                if max_allowed_slot.gpu is not None:
                    gpu_share = min(
                        max_allowed_slot.gpu,
                        Decimal(creation_config.get('instanceGPUs') or
                                Decimal('inf')),
                    )
                else:
                    assert creation_config['instanceGPUs'] is not None
                    gpu_share = Decimal(creation_config['instanceGPUs'])
            except (AssertionError, KeyError):
                msg = (
                    'You have missing resource limits that must be specified. '
                    'If the server does not have default resource configurations, '
                    'you must specify all resource limits by yourself.'
                )
                raise InvalidAPIParameters(msg)

            # Register request.
            kernel_info_base = {
                'status': KernelStatus.PENDING,
                'role': 'master',
                'agent': None,
                'agent_addr': '',
                'scaling_group': self.name,
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
                                  (agents.c.status == AgentStatus.ALIVE) &
                                  kernels.c.id.is_(None)))
            else:
                query = (sa.select([agents.c.id])
                           .select_from(agents)
                           .where((agents.c.scaling_group == self.name) &
                                  (agents.c.status == AgentStatus.ALIVE)))

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
                       .where((agents.c.scaling_group == self.name) &
                              (agents.c.status == AgentStatus.ALIVE)))

            available_shares = []
            async for row in conn.execute(query):
                available_shares.append(ResourceSlot(
                    id=row.id,
                    cpu=Decimal(row.cpu_slots - row.used_cpu_slots),
                    mem=Decimal(row.mem_slots - row.used_mem_slots),
                    gpu=Decimal(row.gpu_slots - row.used_gpu_slots),
                ))
            return available_shares

    async def get_pending_scaling_shares(self, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            query = (sa.select([scaling_groups.c.pending_scalings])
                       .select_from(scaling_groups)
                       .where(scaling_groups.c.name == self.name))
            result = await conn.execute(query)
            pending_scalings = (await result.first()).pending_scalings
            if not pending_scalings:
                pending_scaling_shares = []
            else:
                pending_scalings = map(lambda x: x.split(':'), pending_scalings)

                def _calculate_shares(pending_scaling):
                    instance_type, instance_num = pending_scaling
                    instance_num = int(instance_num)
                    available_instances = self.scaling_driver.available_instances
                    _, instance_shares = next(x for x in available_instances
                                              if x[0] == instance_type)
                    shares = []
                    for _ in range(instance_num):
                        fake_agent_id = uuid.uuid4().hex
                        fake_instance_shares = ResourceSlot(
                            id=fake_agent_id,
                            cpu=instance_shares.cpu,
                            mem=instance_shares.mem,
                            gpu=instance_shares.gpu,
                        )
                        shares.append(fake_instance_shares)
                    return shares

                pending_scaling_shares = reduce(lambda acc, x: acc + x,
                                                map(_calculate_shares,
                                                    pending_scalings),
                                                [])
            return pending_scaling_shares

    async def get_required_shares(self, lang):
        tokens = lang.split('/')
        name, tag = tokens[-1].split(':')
        return await self.registry.config_server.get_image_required_slots(name, tag)

    async def schedule(self, conn=None):
        async with reenter_txn(self.registry.dbpool, conn) as conn:
            # Scheduling goes first, and then scaling goes next.
            # This is to prevent scaling down agents which are
            # necessary to schedule pending jobs.
            pending_jobs = await self.get_pending_jobs(conn=conn)
            available_agent_infos = await self.get_available_shares(conn=conn)
            scheduling_plan = await self.job_scheduler.schedule(
                available_agent_infos, pending_jobs, conn=conn)

            # It is possible that scaling driver re-schedule jobs
            # by optimizing utilization of agents.
            scheduled_job_ids = map(lambda x: x[1].kernel_id, scheduling_plan)
            remaining_jobs = [job for job in pending_jobs
                              if job.kernel_id not in scheduled_job_ids]
            scheduling_plan = await self.scaling_driver.scale(
                scheduling_plan, remaining_jobs, conn=conn)

            # Process scheduled jobs
            if scheduling_plan:
                await asyncio.gather(*[self._process_job(agent_id, job, conn=conn)
                                       for agent_id, job in scheduling_plan],
                                     return_exceptions=True)

    async def _process_job(self, agent_id: str, job: SessionCreationJob, conn=None):
        return await self.registry.create_session(agent_id, job.kernel_id,
                                                  conn=conn)


class AbstractScalingDriver(metaclass=ABCMeta):

    def init(self, config_server):
        pass

    @abstractmethod
    async def scale(self, scheduling_plan: List[Tuple[str, SessionCreationJob]],
                    pending_jobs: List[SessionCreationJob], conn=None):
        '''
        This callback method is invoked.
        '''
        raise NotImplementedError  # noqa


class BasicScalingDriver(AbstractScalingDriver):

    def __init__(self, scaling_group):
        self.scaling_group = scaling_group
        self.dbpool = scaling_group.registry.dbpool

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

    def get_minimum_prepared_jobs(self):
        min_prepared_jobs = []
        for share_unit, min_count in self.min_prepared_shares:
            for _ in range(min_count):
                job = SessionCreationJob(
                    kernel_id=uuid.uuid4().hex,
                    resources=ResourceSlot(
                        cpu=share_unit.cpu,
                        mem=share_unit.mem,
                        gpu=share_unit.gpu,
                    ),
                    created_at=datetime.now(tzutc()),
                )
                min_prepared_jobs.append(job)
        return min_prepared_jobs

    async def scale(self, scheduling_plan: List[Tuple[str, SessionCreationJob]],
                    pending_jobs: List[SessionCreationJob], conn=None):

        min_prepared_jobs = self.get_minimum_prepared_jobs()
        available_agent_infos = await self.scaling_group.get_available_shares()
        pending_scaling_infos = \
            await self.scaling_group.get_pending_scaling_shares()

        # Although pending_jobs is not empty, it is possible that
        # there already exists pending scaling requests and
        # all pending_jobs can be scheduled in those to-be-ready agents.
        # So we need to check for pending scaling requests.
        # Consider minimum prepared shares.
        scheduler: AbstractJobScheduler = self.scaling_group.job_scheduler
        if pending_jobs:
            virtual_scheduling_plan = await scheduler.schedule(
                pending_scaling_infos, pending_jobs + min_prepared_jobs,
                dry_run=True)
        else:
            # TODO: Give precedence to ALIVE agents compared to
            # pending scalings when assigning min_prepared_jobs.
            virtual_scheduling_plan = await scheduler.schedule(
                available_agent_infos + pending_scaling_infos,
                min_prepared_jobs, dry_run=True)

        scheduled_job_ids = set(map(lambda x: x[1].kernel_id,
                                    virtual_scheduling_plan))
        unschedulable_jobs = [job for job in min_prepared_jobs
                              if job.kernel_id not in scheduled_job_ids]

        if unschedulable_jobs:
            scaling_out_plan = await scheduler.get_minimum_required_instances(
                self.instance_info, unschedulable_jobs)
            await self.scale_out(scaling_out_plan, conn=conn)
        else:
            belonging_agents = await self.scaling_group.get_belonging_agents()
            scheduled_agent_ids = \
                set(map(lambda x: x[0],
                        scheduling_plan + virtual_scheduling_plan))
            # TODO: Consider removable pending scalings
            agent_ids_to_remove = [agent_id for agent_id in belonging_agents
                                   if agent_id not in scheduled_agent_ids]
            await self.scale_in(agent_ids_to_remove, conn=conn)

        # Currently, scale() does not change scheduling_plan
        # for additional optimization of resource usage of agents.
        return scheduling_plan

    async def scale_out(self, scaling_out_plan, conn=None):
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (sa.select([scaling_groups.c.pending_scalings], for_update=True)
                       .select_from(scaling_groups)
                       .where(scaling_groups.c.name == self.scaling_group.name))
            result = await conn.execute(query)
            pending_scalings = (await result.first()).pending_scalings
            new_scalings = list(map(lambda x: f'{x[0]}:{x[1]}', scaling_out_plan))
            pending_scalings = merge_scalings(pending_scalings, new_scalings)

            query = (sa.update(scaling_groups)
                       .values(pending_scalings=pending_scalings)
                       .where(scaling_groups.c.name == self.scaling_group.name))
            await conn.execute(query)

            await self.add_agents(scaling_out_plan)

    async def scale_in(self, agent_ids_to_remove, conn=None):
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

        # TODO: Always maintain certain number of agents.
        # This is for preventing too many scaling tasks occurs.
        async with reenter_txn(self.dbpool, conn) as conn:
            query = (agents.update()
                           .values(status=AgentStatus.MARK_TERMINATED)
                           .where(agents.c.id in agent_ids_to_remove))
            await conn.execute(query)

            await self.remove_agents(list(agent_ids_to_remove))


class AbstractVendorScalingDriverMixin:

    @abstractmethod
    async def add_agents(self, scaling_out_plan: List[Tuple[str, int]]):
        raise NotImplementedError

    @abstractmethod
    async def remove_agents(self, agent_ids: List[Tuple[str]]):
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

    async def add_agents(self, scaling_out_plan: List[Tuple[str, int]]):
        session = aiobotocore.get_session()
        async with session.create_client('ec2', region_name=aws_region,
                                         aws_secret_access_key=aws_secret_key,
                                         aws_access_key_id=aws_access_key) as client:
            tasks = []
            for instance_type, instance_num in scaling_out_plan:
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

    async def remove_agents(self, agent_ids):
        if not agent_ids:
            return
        session = aiobotocore.get_session()
        async with session.create_client('ec2', region_name=aws_region,
                                         aws_secret_access_key=aws_secret_key,
                                         aws_access_key_id=aws_access_key) as client:
            # TODO: handle exceptions
            await client.terminate_instances(InstanceIds=agent_ids)

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
    async def schedule(self, available_agent_infos: List[ResourceSlot],
                       pending_jobs: List[SessionCreationJob],
                       conn=None,
                       dry_run=False) -> List[Tuple[str, SessionCreationJob]]:
        '''
        This callback method is invoked when there are new session creation requests,
        terminated sessions, and increases of the scaling group resources.

        Its job is to determine which pending session creation requests can be
        scheduled on the given scaling group now.
        '''
        raise NotImplementedError

    @abstractmethod
    async def get_minimum_required_instances(self, instance_infos, pending_jobs):
        raise NotImplementedError


class SimpleFIFOJobScheduler(AbstractJobScheduler):

    def __init__(self, scaling_group):
        self.scaling_group = scaling_group
        self.dbpool = scaling_group.registry.dbpool
        self.resource_precedence = ['gpu', 'cpu', 'mem']

    async def schedule(self, available_agent_infos, pending_jobs,
                       conn=None, dry_run=False):
        if not available_agent_infos:
            return []
        list(pending_jobs).sort(key=lambda _job: _job.created_at)
        # TODO: We should consider agents' images.

        idx = 0
        async with reenter_txn(self.dbpool, conn) as conn:
            scheduling_plan = []

            curr_agent_info = available_agent_infos[idx]
            idx += 1
            for job in pending_jobs:
                while curr_agent_info.cpu < job.resources.cpu or \
                        curr_agent_info.mem < job.resources.mem or \
                        curr_agent_info.gpu < job.resources.gpu:
                    if idx >= len(available_agent_infos):
                        curr_agent_info = None
                        break
                    curr_agent_info = available_agent_infos[idx]
                    idx += 1

                if curr_agent_info is None:
                    break
                agent_id = curr_agent_info.id

                if not dry_run:
                    # Prevent assigning kernels to pending scalings. (agents)
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
                             .values({
                                 'agent_addr': agent_addr,
                                 'scaling_group': self.scaling_group.name,
                             })
                             .where(kernels.c.id == job.kernel_id))
                    await conn.execute(query)

                curr_agent_info = ResourceSlot(
                    id=curr_agent_info.id,
                    cpu=Decimal(curr_agent_info.cpu - job.resources.cpu),
                    mem=Decimal(curr_agent_info.mem - job.resources.mem),
                    gpu=Decimal(curr_agent_info.gpu - job.resources.gpu),
                )

                scheduling_plan.append((agent_id, job))

            return scheduling_plan

    async def get_minimum_required_instances(self, instance_infos, pending_jobs):
        # 1. Assume that no # limit of instances,
        # only t2.2xlarge & p2.xlarge, and only cpu, mem, gpu.
        assert pending_jobs is not None

        # Sort jobs by resource precedence.
        pending_jobs_dict = defaultdict(list)
        for job in pending_jobs:
            for resource_type in self.resource_precedence:
                if getattr(job.resources, resource_type) > Decimal(0):
                    pending_jobs_dict[resource_type].append(job)
                    break

        min_required_instances = []
        for resource_type in self.resource_precedence:
            target_jobs = pending_jobs_dict[resource_type]
            if not target_jobs:
                continue

            instance_type, instance_spec = instance_infos[resource_type][0]

            def _acc_resources(acc: ResourceSlot, job: SessionCreationJob):
                return ResourceSlot(
                    cpu=acc.cpu + job.resources.cpu,
                    mem=acc.mem + job.resources.mem,
                    gpu=acc.gpu + job.resources.gpu,
                )

            required_resource_sum = reduce(_acc_resources, target_jobs,
                                           ResourceSlot())

            def _required_instance_num(resource_type):
                required_resource = getattr(required_resource_sum, resource_type)
                instance_resource = getattr(instance_spec, resource_type)

                if required_resource == Decimal(0):
                    return Decimal(0)
                if instance_resource == Decimal(0):
                    return Decimal('Infinity')
                return math.ceil(required_resource / instance_resource)

            required_instance_num = max(
                _required_instance_num('cpu'),
                _required_instance_num('mem'),
                _required_instance_num('gpu'),
            )

            min_required_instances.append((instance_type, required_instance_num))

        return min_required_instances
        # 2, Now consider multiple instances for each resource types.
        # 3. Now consider # limit of instances.
        # 4. Now consider other types of resources, e.g. tpu.
        # 5. Now consider "marked-to-be-terminated" agents if exists.


scaling_groups = sa.Table(
    'scaling_groups', metadata,
    sa.Column('name', sa.String(length=64), unique=True, index=True),
    sa.Column('is_active', sa.Boolean, index=True,
              server_default=true()),
    sa.Column('created_at', sa.DateTime(timezone=True),
              server_default=sa.func.now()),
    sa.Column('driver', sa.String(length=64), nullable=False),
    sa.Column('job_scheduler', sa.String(length=64), nullable=False),
    sa.Column('pending_scalings', sa.ARRAY(sa.String(length=64)))
    # sa.Column('params', sa.JSONB(), nullable=False),
)


def ensure_default():
    pass
