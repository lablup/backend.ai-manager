Backend.AI Manager with API Gateway
===================================

Package Structure
-----------------

* `ai.backend`
  - `manager`: Abstraction of agents and computation kernels
  - `gateway`: User and Admin API (REST/GraphQL) gateway based on aiohttp

Installation
------------

Please visit [the installation guides](https://github.com/lablup/backend.ai/wiki).

### For development

#### Prerequisites

* `libnsappy-dev` or `snappy-devel` system package depending on your distro
* Python 3.6 or higher with [pyenv](https://github.com/pyenv/pyenv)
and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) (optional but recommneded)
* Docker 18.03 or later with docker-compose (18.09 or later is recommended)

#### Common steps

Clone [the meta repository](https://github.com/lablup/backend.ai) and install a "halfstack"
configuration.  The halfstack configuration installs and runs several dependency daemons such as etcd in
the background.

```console
$ git clone https://github.com/lablup/backend.ai halfstack
$ cd halfstack
$ docker-compose -f docker-compose.halfstack.yml up -d
```

Then prepare the source clone of the agent as follows.
First install the current working copy.

```console
$ git clone https://github.com/lablup/backend.ai-manager manager
$ cd manager
$ pyenv virtualenv venv-manager
$ pyenv local venv-manager
$ pip install -U pip setuptools
$ pip install -U -r requirements-dev.txt
```

From now on, let's assume all shell commands are executed inside the virtualenv.

### Halfstack (single-node development & testing)

#### Recommended directory structure

* `backend.ai-dev`
  - `manager` (git clone from this repo)
  - `agent` (git clone from [the agent repo](https://github.com/lablup/backend.ai-agent))
  - `common` (git clone from [the common repo](https://github.com/lablup/backend.ai-common))

Install `backend.ai-common` as an editable package in the manager (and the agent) virtualenvs
to keep the codebase up-to-date.

```console
$ cd manager
$ pip install -U -e ../common
```

#### Steps

Copy (or symlink) the halfstack configs:
```console
$ cp config/halfstack.toml ./manager.toml
$ cp config/halfstack.alembic.ini ./alembic.ini
```

Set up Redis:
```console
$ python -m ai.backend.manager.cli etcd put config/redis/addr 127.0.0.1:8110
```

Set up the public Docker registry:
```console
$ python -m ai.backend.manager.cli etcd put config/docker/registry/index.docker.io "https://registry-1.docker.io"
$ python -m ai.backend.manager.cli etcd put config/docker/registry/index.docker.io/username "lablup"
$ python -m ai.backend.manager.cli etcd rescan-images index.docker.io
```

Set up the vfolder paths:
```console
$ mkdir -p "$HOME/vfroot/local"
$ python -m ai.backend.manager.cli etcd put volumes/_mount "$HOME/vfroot"
$ python -m ai.backend.manager.cli etcd put volumes/_default_host local
```

Set up the database:
```console
$ python -m ai.backend.manager.cli schema oneshot
$ python -m ai.backend.manager.cli fixture populate sample-configs/example-keypairs.json
$ python -m ai.backend.manager.cli fixture populate sample-configs/example-resource-presets.json
```

Then, run it (for debugging, append a `--debug` flag):

```console
$ python -m ai.backend.gateway.server
```

To run tests:

```console
$ python -m flake8 src tests
$ python -m pytest -m 'not integration' tests
```

Now you are ready to install the agent.
Head to [the README of Backend.AI Agent](https://github.com/lablup/backend.ai-agent/blob/master/README.md).

NOTE: To run tests including integration tests, you first need to install and run the agent on the same host.

## Deployment

### Configuration

Put a TOML-formatted manager configuration (see the sample in `config/sample.toml`)
in one of the following locations:

 * `manager.toml` (current working directory)
 * `~/.config/backend.ai/manager.toml` (user-config directory)
 * `/etc/backend.ai/manager.toml` (system-config directory)

Only the first found one is used by the daemon.

Also many configurations shared by both manager and agent are stored in etcd.
As you might have noticed above, the manager provides a CLI interface to access and manipulate the etcd
data.  Check out the help page of our etcd command set:

```console
$ python -m ai.backend.manager.cli etcd --help
```

If you run etcd as a Docker container (e.g., via halfstack), you may use the native client as well.
In this case, PLEASE BE WARNED that you must prefix the keys with "/sorna/{namespace}" manaully:

```console
$ docker exec -it ${ETCD_CONTAINER_ID} /bin/ash -c 'ETCDCTL_API=3 etcdctl ...'
```

### Running from a command line

The minimal command to execute:

```sh
python -m ai.backend.gateway.server
```

For more arguments and options, run the command with `--help` option.

### Networking

The manager and agent should run in the same local network or different
networks reachable via VPNs, whereas the manager's API service must be exposed to
the public network or another private network that users have access to.

The manager requires access to the etcd, the PostgreSQL database, and the Redis server.

| User-to-Manager TCP Ports | Usage |
|:-------------------------:|-------|
| manager:{80,443}          | Backend.AI API access |

| Manager-to-X TCP Ports | Usage |
|:----------------------:|-------|
| etcd:2379              | etcd API access |
| postgres:5432          | Database access |
| redis:6379             | Redis API access |

The manager must also be able to access TCP ports 6001, 6009, and 30000 to 31000 of the agents in default
configurations.  You can of course change those port numbers and ranges in the configuration.

| Manager-to-Agent TCP Ports | Usage |
|:--------------------------:|-------|
| 6001                       | ZeroMQ-based RPC calls from managers to agents |
| 6009                       | HTTP watcher API |
| 30000-31000                | Port pool for in-container services |
