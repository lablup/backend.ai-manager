#! /usr/bin/env python3

from namedlist import namedlist


Instance = namedlist('Instance', [
    ('status', None),
    ('id', None),
    ('ip', None),
    ('addr', None),
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

Kernel = namedlist('Kernel', [
    ('id', None),
    ('instance', None),
    ('addr', None),
    ('stdin_port', None),
    ('stdout_port', None),
    ('created_at', None),
    ('tag', None),
])
Kernel.__doc__ = '''\
A compound data structure to represent kernel information.
The purpose of this object is same to :class:`Instance <sorna.structs.Instance>`.
'''

