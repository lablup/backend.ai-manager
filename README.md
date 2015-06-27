Lablup Sorna
============

The back-end calculation kernel server.

The name "Sorna" came from the name of an island in the movie Jurassic Park where InGen's dinosaurs research facility is located.
It means that this project is not visible to the users but plays a key role in our services.

Components
----------

### Sorna Manager

It routes the requests from front-end web servers to the kernel instances.
It also checks the health of kernels using heartbeats and manages resource allocation of the VM instances (EC2) and container slots on them.

 * Python package name: `sorna.manager`

### Sorna Agent

It resides in the container where the kernel process runs.

 * https://github.com/lablup/sorna-agent
 * Python package name: `sorna.agent`

### Sorna Protocols

It defines the protocols between front-end servers, manager server(s), and kernel instances running agents.

 * https://github.com/lablup/sorna-protocols
 * Python package name: `sorna.proto`
