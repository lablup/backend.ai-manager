from __future__ import annotations

from decimal import Decimal
from typing import (
    Any, Optional,
    Sequence,
    Mapping,
    Tuple,
)

from ai.backend.common.types import (
    AgentId, KernelId,
    ResourceSlot,
)
from . import (
    AbstractScheduler,
    AgentContext,
    PendingSession,
    ExistingSession,
)


def key_by_requested_slots(
    agent: AgentContext,
    requested_slots: ResourceSlot,
) -> Tuple[int, ResourceSlot]:
    unused_slot_keys = set()
    for k, v in requested_slots.items():
        if v == Decimal(0):
            unused_slot_keys.add(k)
    num_extras = 0
    for k, v in agent.available_slots.items():
        if k in unused_slot_keys and v > Decimal(0):
            num_extras += 1
    # Put back agents with more extra slot types
    # (e.g., accelerators)
    # Also put front agents with exactly required slot types
    return (-num_extras, agent.available_slots)


class FIFOSlotScheduler(AbstractScheduler):

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__(config)

    def pick_session(self,
                     total_capacity: ResourceSlot,
                     pending_sessions: Sequence[PendingSession],
                     existing_sessions: Sequence[ExistingSession],
                     ) -> Optional[KernelId]:
        # Just pick the first pending session.
        return pending_sessions[0].kernel_id

    def assign_agent(self,
                     agents: Sequence[AgentContext],
                     pending_session: PendingSession,
                     ) -> Optional[AgentId]:
        possible_agents = []
        for agent in agents:
            remaining_slots = agent.available_slots - agent.occupied_slots
            if remaining_slots >= pending_session.requested_slots:
                possible_agents.append(agent)
        if possible_agents:
            chosen_agent = \
                max(possible_agents,
                    key=lambda a: key_by_requested_slots(a,
                                                         pending_session.requested_slots))
            return chosen_agent.agent_id
        return None


class LIFOSlotScheduler(AbstractScheduler):

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__(config)

    def pick_session(self,
                     total_capacity: ResourceSlot,
                     pending_sessions: Sequence[PendingSession],
                     existing_sessions: Sequence[ExistingSession],
                     ) -> Optional[KernelId]:
        # Just pick the last pending session.
        return pending_sessions[-1].kernel_id

    def assign_agent(self,
                     agents: Sequence[AgentContext],
                     pending_session: PendingSession,
                     ) -> Optional[AgentId]:
        possible_agents = []
        for agent in agents:
            remaining_slots = agent.available_slots - agent.occupied_slots
            if remaining_slots >= pending_session.requested_slots:
                possible_agents.append(agent)
        if possible_agents:
            chosen_agent = \
                max(possible_agents,
                    key=lambda a: key_by_requested_slots(a,
                                                         pending_session.requested_slots))
            return chosen_agent.agent_id
        return None
