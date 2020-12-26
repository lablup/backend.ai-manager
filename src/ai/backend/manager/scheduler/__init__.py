from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Protocol,
    Sequence,
)
import uuid

from aiopg.sa.connection import SAConnection
import attr

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.docker import (
    ImageRef,
)
from ai.backend.common.types import (
    AccessKey,
    AgentId,
    ClusterMode,
    KernelId,
    SessionId,
    SessionTypes,
    ResourceSlot,
    SlotName,
    SlotTypes,
)

from ..defs import DEFAULT_ROLE
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
    """
    Context for each scheduling decision.
    """
    registry: AgentRegistry
    known_slot_types: Mapping[SlotName, SlotTypes]


@attr.s(auto_attribs=True, slots=True)
class ExistingSession:
    kernels: List[KernelInfo]
    access_key: AccessKey
    session_id: uuid.UUID
    session_type: SessionTypes
    session_name: str
    cluster_mode: ClusterMode
    cluster_size: int
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    occupying_slots: ResourceSlot


@attr.s(auto_attribs=True, slots=True)
class PendingSession:
    """
    Context for individual session-related information used during scheduling.
    Resource parameters defined here should contain total amount of resources
    for all kernels in one session.
    """
    kernels: List[KernelInfo]
    access_key: AccessKey
    session_id: SessionId
    session_creation_id: str
    session_type: SessionTypes
    session_name: str
    cluster_mode: ClusterMode
    cluster_size: int
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    resource_policy: str
    resource_opts: Mapping[str, Any]
    requested_slots: ResourceSlot
    target_sgroup_names: MutableSequence[str]
    environ: MutableMapping[str, str]
    mounts: Sequence[str]
    mount_map: Mapping[str, str]
    bootstrap_script: Optional[str]
    startup_command: Optional[str]
    internal_data: Optional[MutableMapping[str, Any]]
    preopen_ports: List[int]

    @property
    def main_kernel_id(self) -> KernelId:
        for k in self.kernels:
            if k.cluster_role == DEFAULT_ROLE:
                return k.kernel_id
        raise RuntimeError('Unable to get the main kernel ID')


@attr.s(auto_attribs=True, slots=True)
class KernelInfo:
    """
    Representing invididual kernel info.
    Resource parameters defined here should contain single value of resource
    for each kernel.
    """
    kernel_id: KernelId
    access_key: AccessKey
    session_id: uuid.UUID
    cluster_role: str
    cluster_idx: int
    cluster_hostname: str
    image_ref: ImageRef
    resource_opts: Mapping[str, Any]
    requested_slots: ResourceSlot
    bootstrap_script: Optional[str]
    startup_command: Optional[str]

    def __str__(self):
        return f'{self.kernel_id}#{self.cluster_role}{self.cluster_idx}'


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
    permanent: bool = False


class SchedulingPredicate(Protocol):
    async def __call__(
        self,
        db_conn: SAConnection,
        sched_ctx: SchedulingContext,
        sess_ctx: PendingSession,
    ) -> PredicateResult:
        ...


class AbstractScheduler(metaclass=ABCMeta):

    """
    Interface for scheduling algorithms where the
    ``schedule()`` method is a pure function.
    """

    config: Mapping[str, Any]

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def pick_session(
        self,
        total_capacity: ResourceSlot,
        pending_sessions: Sequence[PendingSession],
        existing_sessions: Sequence[ExistingSession],
    ) -> Optional[SessionId]:
        """
        Pick a session to try schedule.
        This is where the queueing semantics is implemented such as prioritization.
        """
        return None

    @abstractmethod
    def assign_agent_for_session(
        self,
        possible_agents: Sequence[AgentContext],
        pending_session: PendingSession,
    ) -> Optional[AgentId]:
        """
        Assign an agent for the entire session, only considering the total requested
        slots of the session.  This is used for both single-container sessions and
        single-node multi-container sessions.

        In single-node multi-container sessions, all sub-containers are spawned by
        slicing the assigned agent's resource.
        """
        return None

    @abstractmethod
    def assign_agent_for_kernel(
        self,
        possible_agents: Sequence[AgentContext],
        pending_kernel: KernelInfo,
    ) -> Optional[AgentId]:
        """
        Assign an agent for a kernel of the session.
        This may be called multiple times for multi-node multi-container sessions.
        """
        return None
