from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from typing import (
    Any, List, Optional,
    Protocol,
    Mapping,
    Sequence, MutableSequence,
)
import uuid

from aiopg.sa.connection import SAConnection
import attr

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import (
    ImageRef,
)
from ai.backend.common.types import (
    AgentId, KernelId, AccessKey, SessionTypes,
    ResourceSlot,
)
from ..registry import AgentRegistry

log = BraceStyleAdapter(logging.getLogger('ai.backend.manager.scheduler'))


@attr.s(auto_attribs=True, slots=True)
class AgentAllocationContext:
    agent_id: AgentId
    agent_addr: str
    scaling_group: str


@attr.s(auto_attribs=True, slots=True)
class AgentContext:
    agent_id: AgentId
    agent_addr: str
    scaling_group: str
    available_slots: ResourceSlot
    occupied_slots: ResourceSlot


@attr.s(auto_attribs=True, slots=True)
class ScheduleDecision:
    agent_id: AgentId
    kernel_id: KernelId


@attr.s(auto_attribs=True, slots=True)
class SchedulingContext:
    '''
    Context for each scheduling decision.
    '''
    registry: AgentRegistry
    known_slot_types: Mapping[str, str]


@attr.s(auto_attribs=True, slots=True)
class ExistingSession:
    kernels: List[KernelInfo]
    access_key: AccessKey
    session_name: str
    session_uuid: uuid.UUID
    session_type: SessionTypes
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    occupying_slots: ResourceSlot


@attr.s(auto_attribs=True, slots=True)
class PendingSession:
    '''
    Context for individual session-related information used during scheduling.
    Resource parameters defined here should contain total amount of resources
    for all kernels in one session.
    '''
    kernels: List[KernelInfo]
    access_key: AccessKey
    session_name: str
    session_uuid: uuid.UUID
    session_type: SessionTypes
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    resource_policy: str
    resource_opts: Mapping[str, Any]
    requested_slots: ResourceSlot
    target_sgroup_names: MutableSequence[str]
    environ: Mapping[str, str]
    mounts: Sequence[str]
    mount_map: Mapping[str, str]
    bootstrap_script: Optional[str]
    startup_command: Optional[str]
    internal_data: Optional[Mapping[str, Any]]
    preopen_ports: List[int]

    @property
    def master_kernel_id(self) -> KernelId:
        for k in self.kernels:
            if k.role == 'master':
                return k.kernel_id
        raise RuntimeError('Unable to get master kernel ID')

@attr.s(auto_attribs=True, slots=True)
class KernelInfo:
    '''
    Representing invididual kernel info.
    Resource parameters defined here should contain single value of resource
    for each kernel.
    '''
    kernel_id: KernelId
    role: str
    idx: int
    image_ref: ImageRef
    resource_opts: Mapping[str, Any]
    requested_slots: ResourceSlot
    bootstrap_script: Optional[str]
    startup_command: Optional[str]

    def __str__(self):
        return f'{self.kernel_id}#{self.role}'


@attr.s(auto_attribs=True, slots=True)
class KernelAgentBinding:
    kernel: KernelInfo
    agent_alloc_ctx: AgentAllocationContext


class PredicateCallback(Protocol):
    async def __call__(
        self,
        db_conn: SAConnection,
        sched_ctx: SchedulingContext,
        sess_ctx: PendingSession,
    ) -> None:
        ...


@attr.s(auto_attribs=True, slots=True)
class PredicateResult:
    passed: bool
    message: Optional[str] = None
    success_cb: Optional[PredicateCallback] = None
    failure_cb: Optional[PredicateCallback] = None


class SchedulingPredicate(Protocol):
    async def __call__(
        self,
        db_conn: SAConnection,
        sched_ctx: SchedulingContext,
        sess_ctx: PendingSession,
    ) -> PredicateResult:
        ...


class AbstractScheduler(metaclass=ABCMeta):

    '''
    Interface for scheduling algorithms where the
    ``schedule()`` method is a pure function.
    '''

    config: Mapping[str, Any]

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def pick_session(
        self,
        total_capacity: ResourceSlot,
        pending_sessions: Sequence[PendingSession],
        existing_sessions: Sequence[ExistingSession],
    ) -> Optional[KernelId]:
        return None

    @abstractmethod
    def assign_agent(
        self,
        possible_agents: Sequence[AgentContext],
        pending_session: PendingSession,
    ) -> Optional[AgentId]:
        return None
