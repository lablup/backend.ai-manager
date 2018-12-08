import enum


class ManagerStatus(str, enum.Enum):
    TERMINATED = 'terminated'
    PREPARING = 'preparing'
    RUNNING = 'running'
    FROZEN = 'frozen'
