from __future__ import annotations

from contexvars import ContextVar
from decimal import Decimal
from typing import (
    Any, Optional,
    Dict,
    Sequence,
    Mapping,
)

from ai.backend.common.types import (
    AccessKey, AgentId, KernelId,
    ResourceSlot,
)
from . import (
    AbstractScheduler,
    AgentContext,
    PendingSession,
    ExistingSession,
)


class DRFScheduler(AbstractScheduler):

    per_user_dominant_share: Dict[AccessKey, Decimal]
    total_capacity: ContextVar[ResourceSlot]

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__(config)
        self.per_user_dominant_share = {}

    def pick_session(self,
                     total_capacity: ResourceSlot,
                     pending_sessions: Sequence[PendingSession],
                     existing_sessions: Sequence[ExistingSession],
                     ) -> Optional[KernelId]:

        least_dominant_share_user: Optional[AccessKey] = None
        self.total_capacity.set(total_capacity)

        # Update the dominant shares of all users.
        for sess in existing_sessions:
            dominant_share = Decimal(0)
            for slot, value in sess.occupying_slots.items():
                slot_share = value / total_capacity[slot]
                if dominant_share < slot_share:
                    dominant_share = slot_share
            if self.per_user_dominant_share[sess.access_key] < dominant_share:
                self.per_user_dominant_share[sess.access_key] = dominant_share

        least_dominant_share = Decimal(0)
        for akey, dshare in self.per_user_dominant_share.items():
            if least_dominant_share < dshare:
                least_dominant_share = dshare
                least_dominant_share_user = akey

        # Pick the first pending session of the user
        # who has the lowest dominant share.
        for sess in pending_sessions:
            if sess.access_key == least_dominant_share_user:
                return sess.kernel_id

        return None

    def assign_agent(self,
                     agents: Sequence[AgentContext],
                     pending_session: PendingSession,
                     ) -> Optional[AgentId]:
        possible_agents = []
        for agent in agents:
            if agent.available_slots >= pending_session.requested_slots:
                possible_agents.append(agent)

        # Update the dominant share
        # This is required to use to the latest dominant share information
        # when iterating over multiple pending sessions in a single scaling group.
        dominant_share_from_request = Decimal(0)
        total_capacity = self.total_capacity.get()
        for slot, value in pending_session.requested_slots.items():
            slot_share = value / total_capacity[slot]
            if dominant_share_from_request < slot_share:
                dominant_share_from_request = slot_share
        if self.per_user_dominant_share[pending_session.access_key] < dominant_share_from_request:
            self.per_user_dominant_share[pending_session.access_key] = dominant_share_from_request

        if possible_agents:
            chosen_agent = \
                max(possible_agents, key=lambda a: a.available_slots)
            return chosen_agent.agent_id
        return None
