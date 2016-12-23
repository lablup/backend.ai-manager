from dateutil.parser import parse as dtparse
from namedlist import namedlist


Instance = namedlist('Instance', [
    ('status', None),
    ('id', None),
    ('ip', None),
    ('addr', None),      # tcp://{ip}:{port}
    ('type', None),
    ('used_cpu', None),
    ('num_kernels', 0),
    ('max_kernels', 1),
    ('tag', None),
])
Instance.__doc__ = '''\
A compound data structure to represent instance information.
Note that this object does not represent the database records (e.g., those in
Redis), but just a metadata container passed around the sorna system.
'''
# VM instances should run a docker daemon using "-H tcp://0.0.0.0:2375" in DOCKER_OPTS.

class Kernel(namedlist('_Kernel', [
    ('id', None),
    ('instance', None),
    ('access_key', None),
    ('lang', None),
    ('addr', None),        # tcp://{ip}:{port}
    ('stdin_port', None),
    ('stdout_port', None),
    ('created_at', None),
    ('tag', None),
    # resource limit info (updated by agent heartbeats)
    ('exec_timeout', 0.0),  # sec
    ('idle_timeout', 0.0),  # sec
    ('mem_limit', 0),       # kbytes
    # stats (updated by agent heartbeats)
    ('num_queries', 0),
    ('idle', 0.0),         # sec
    ('cpu_used', 0.0),     # msec
    ('mem_max_bytes', 0),  # bytes
    ('net_rx_bytes', 0),
    ('net_tx_bytes', 0),
    ('io_read_bytes', 0),
    ('io_write_bytes', 0),
])):
    '''
    A compound data structure to represent kernel information.
    The purpose of this object is same to :class:`Instance <sorna.structs.Instance>`.
    '''

    def apply_type(self):
        self._update({
            'created_at': dtparse(self.created_at) if self.created_at is not None else None,
            'num_queries': int(self.num_queries),
            'cpu_used': float(self.cpu_used),
            'exec_timeout': float(self.exec_timeout),
            'idle_timeout': float(self.idle_timeout),
            'mem_limit': int(self.mem_limit),
            'mem_max_bytes': int(self.mem_max_bytes),
            'io_read_bytes': int(self.io_read_bytes),
            'io_write_bytes': int(self.io_write_bytes),
            'net_rx_bytes': int(self.net_rx_bytes),
            'net_tx_bytes': int(self.net_tx_bytes),
            'idle': float(self.idle),
            'stdin_port': int(self.stdin_port),
            'stdout_port': int(self.stdout_port),
        })

