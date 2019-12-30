from __future__ import annotations

from typing import (
    Any, Optional,
    Sequence,
    Mapping,
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
            if agent.available_slots >= pending_session.requested_slots:
                possible_agents.append(agent)
        if possible_agents:
            chosen_agent = \
                max(possible_agents, key=lambda a: a.available_slots)
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
            if agent.available_slots >= pending_session.requested_slots:
                possible_agents.append(agent)
        if possible_agents:
            chosen_agent = \
                max(possible_agents, key=lambda a: a.available_slots)
            return chosen_agent.agent_id
        return None
