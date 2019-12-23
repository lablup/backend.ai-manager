from __future__ import annotations

from decimal import Decimal
from typing import (
    Any, Optional,
    Dict,
    List, Sequence,
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
    total_capacity: ResourceSlot

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__(config)
        self.per_user_dominant_share = {}

    def pick_session(self,
                     total_capacity: ResourceSlot,
                     pending_sessions: Sequence[PendingSession],
                     existing_sessions: Sequence[ExistingSession],
                     ) -> Optional[KernelId]:

        least_dominant_share_user: Optional[AccessKey] = None
        self.total_capacity = total_capacity

        # Calculate the initial dominant shares of all users.
        for existing_sess in existing_sessions:
            dominant_share = Decimal(0)
            for slot, value in existing_sess.occupying_slots.items():
                slot_share = value / self.total_capacity[slot]
                if dominant_share < slot_share:
                    dominant_share = slot_share
            if self.per_user_dominant_share[existing_sess.access_key] < dominant_share:
                self.per_user_dominant_share[existing_sess.access_key] = dominant_share

        # Find who has the least dominant share among the pending session.
        users_with_pending_session: List[AccessKey] = []
        for pending_sess in pending_sessions:
            users_with_pending_session.append(pending_sess.access_key)

        least_dominant_share = Decimal(0)
        for akey in users_with_pending_session:
            dshare = self.per_user_dominant_share[akey]
            if least_dominant_share < dshare:
                least_dominant_share = dshare
                least_dominant_share_user = akey

        # Pick the first pending session of the user
        # who has the lowest dominant share.
        for pending_sess in pending_sessions:
            if pending_sess.access_key == least_dominant_share_user:
                return pending_sess.kernel_id

        return None

    def assign_agent(self,
                     agents: Sequence[AgentContext],
                     pending_session: PendingSession,
                     ) -> Optional[AgentId]:

        # If some predicate checks for a picked session fail,
        # this method is NOT called at all for the picked session.
        # In such case, we just skip updating self.per_user_dominant_share state
        # and the scheduler dispatcher continues to pick another session within the same scaling group.

        possible_agents = []
        for agent in agents:
            if agent.available_slots >= pending_session.requested_slots:
                possible_agents.append(agent)

        if possible_agents:
            # We have one or more agents that can host the picked session.

            # Update the dominant share.
            # This is required to use to the latest dominant share information
            # when iterating over multiple pending sessions in a single scaling group.
            dominant_share_from_request = Decimal(0)
            for slot, value in pending_session.requested_slots.items():
                slot_share = value / self.total_capacity[slot]
                if dominant_share_from_request < slot_share:
                    dominant_share_from_request = slot_share
            if self.per_user_dominant_share[pending_session.access_key] < dominant_share_from_request:
                self.per_user_dominant_share[pending_session.access_key] = dominant_share_from_request

            # Choose the agent.
            chosen_agent = \
                max(possible_agents, key=lambda a: a.available_slots)
            return chosen_agent.agent_id

        return None
