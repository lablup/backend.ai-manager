#! /usr/bin/env python3

from namedlist import namedlist


Instance = namedlist('Instance', [
    ('id', None),
    ('ip', None),
    ('spec', None),
    ('docker_port', 2375), # standard docker daemon port
    ('max_kernels', 1),
    ('num_kernels', 0),
    ('tag', ''),
])
Instance.__doc__ = '''\
A compound data structure to represent instance information.
Note that this object does not represent the database records (e.g., those in
Redis), but just a metadata container passed around the sorna system.
'''
# VM instances should run a docker daemon using "-H tcp://0.0.0.0:2375" in DOCKER_OPTS.

Kernel = namedlist('Kernel', [
    ('id', None),
    ('instance', None),
    ('agent_sock', None),
    ('stdin_sock', None),
    ('stdout_sock', None),
    ('stderr_sock', None),
    ('created_at', None),
    ('tag', None),
])
Kernel.__doc__ = '''\
A compound data structure to represent kernel information.
The purpose of this object is same to :class:`Instance <sorna.structs.Instance>`.
'''

