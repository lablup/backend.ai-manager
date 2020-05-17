from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from typing import (
    Any, Optional,
    Protocol,
    Mapping,
    Sequence, MutableSequence, List,
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
    kernel_id: KernelId
    access_key: AccessKey
    session_name: str
    session_type: SessionTypes
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    image_ref: ImageRef
    occupying_slots: ResourceSlot


@attr.s(auto_attribs=True, slots=True)
class PendingSession:
    '''
    Context for individual session-related information used during scheduling.
    '''
    kernel_id: KernelId
    access_key: AccessKey
    session_name: str
    session_type: SessionTypes
    domain_name: str
    group_id: uuid.UUID
    scaling_group: str
    image_ref: ImageRef
    resource_policy: dict
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
