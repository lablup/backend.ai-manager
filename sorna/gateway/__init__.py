from enum import IntEnum


class GatewayStatus(IntEnum):
    STARTING = 1
    RUNNING = 2
    SYNCING = 3


class AgentStatus(IntEnum):
    PENDING = 1
    RUNNING = 2
    RESETTINU = 3
