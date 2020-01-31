from __future__ import annotations


from typing import (
    Any, Optional,
    Sequence,
    Mapping,
)

from ai.backend.common.types import (
    AgentId, KernelId,
    ResourceSlot
)

from . import (
    AbstractScheduler,
    PendingSession,
    ExistingSession,
    AgentContext
)


class MOFScheduler(AbstractScheduler):
    "Minimum Occupied slot First Scheduler"

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
        # return min occupied slot agent or None
        if assigned_agent := next(iter(sorted(
            (agent for agent in agents
             if agent.available_slots >= pending_session.requested_slots),
            key=lambda a: a.occupied_slots
        )), None):
            return assigned_agent.agent_id
        return assigned_agent
