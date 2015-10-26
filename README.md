Lablup Sorna
============

The back-end calculation kernel server.

The name "Sorna" came from the name of an island in the movie Jurassic Park where InGen's dinosaurs research facility is located.
It means that this project is not visible to the users but plays a key role in our services.

Components
----------

### Sorna Manager

It routes initial kernel creation requests from front-end services to agent instances with available capacity.
It also monitors the overall resource capacity in the cluster.

 * Python package name: `sorna.manager`

#### Required Docker Links

 * redis
   - `REDIS_PORT_6379_TCP_ADDR` (default: 127.0.0.1)
   - `REDIS_PORT_6379_TCP_PORT` (default: 6379)

### Sorna Agent

It manages individual EC2 instances and launches/destroyes Docker containers where REPL daemons (kernels) run.
Each agent on a new EC2 instance self-registers itself to the manager via heartbeats.
Once a kernel is set up, front-end services interact with agents directly.

 * https://github.com/lablup/sorna-agent
 * Python package name: `sorna.agent`

### Sorna REPL

It is a set of small ZMQ-based REPL daemons in various programming languages.

 * https://github.com/lablup/sorna-repl
 * Each daemon is a separate program, usually named "run.{lang-specific-extension}".

### Sorna Common

It is a collection of utility modules used throughout Sorna services, such as logging and messaging protocols.

 * https://github.com/lablup/sorna-common
 * Python package name: `sorna.proto`, `sorna.logging` (maybe added more)

Development
-----------

### git flow

The sorna repositories use [git flow](http://danielkummer.github.io/git-flow-cheatsheet/index.html) to streamline branching during development and deployment.
We use the default configuration (master -> preparation for release, develop -> main development, feature/ -> features, etc.) as-is.
