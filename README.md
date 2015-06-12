Lablup Sorna
============

The back-end calculation kernel server.

The name "Sorna" came from the name of an island in the movie Jurassic Park where InGen's dinosaurs research facility is located.
It means that this project is not visible to the users but plays a key role in our services.

Components
----------

### API Server

It routes the requests from front-end web servers to the kernel instances.
It also checks the health of kernels using heartbeats and manages resource allocation of the VM instances (EC2) and container slots on them.

(TODO: For the performance, streaming connections between kernel instances and front-end servers may bypass the API server in future implementation.)

### Kernel Agent

It resides in the container where the kernel process runs.
