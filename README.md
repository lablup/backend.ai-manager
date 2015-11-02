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

Deployment
----------

In the simplest setting, you may run sorna-manager and sorna-agnet in a screen or tmux session.
For more "production"-like setup, we recommend to use supervisord.

Example `/etc/supervisor/conf.d/apps.conf`:
```
[program:sorna-manager]
user = ubuntu
stopsignal = TERM
stopasgroup = true
command = /home/sorna/run-manager.sh

[program:sorna-agent]
user = ubuntu
stopsignal = TERM
stopasgroup = true
command = /home/sorna/run-agent.sh
```

Note that the user must have the same UID that the dockerizes sorna-repl daemons have: 1000.
`stopasgroup` must be set true for proper termination.

Example `run-manager.sh`:
```
#! /bin/bash
export HOME=/home/sorna
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv shell 3.5.0
python3 -m sorna.manager.server
```

The agents may run on the same instance where the manager runs, or multiple EC2 instances in an auto-scaling group.

Example `run-agent.sh`:
```
#! /bin/bash
export HOME=/home/sorna
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv shell 3.5.0
export AWS_ACCESS_KEY_ID="<your-access-key-for-s3>"
export AWS_SECRET_ACCESS_KEY="<your-secret-key-for-s3>"
python3 -m sorna.agent.server --manager-addr tcp://sorna-manager.lablup:5001 --max-kernels 15
```

