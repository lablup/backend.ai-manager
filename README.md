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
* Docker 17.03 or later with docker-compose (18.03 or later is recommended)

Clone [the meta repository](https://github.com/lablup/backend.ai) and install a "halfstack" configuration.
The halfstack configuration installs and runs dependency daemons such as etcd in the background.

```console
~$ git clone https://github.com/lablup/backend.ai halfstack
~$ cd halfstack
~/halfstack$ docker-compose -f docker-compose.halfstack.yml up -d
```

Then prepare the source clone of the agent as follows.
First install the current working copy.

```console
~$ git clone https://github.com/lablup/backend.ai-manager manager
~$ cd manager
~/manager$ pyenv virtualenv venv-manager
~/manager$ pyenv local venv-manager
~/manager (venv-manager) $ pip install -U pip setuptools   # ensure latest versions
~/manager (venv-manager) $ pip install -U -r requirements-dev.txt
```

With the halfstack, you can run the agent simply like
(note that you need a working manager running with the halfstack already):

```console
~/manager (venv-manager) $ scripts/run-with-halfstack.sh python -m ai.backend.gateway.server --debug
```

To develop and debug, configure the manager as follows:

```console
~/manager (venv-manager) $ scripts/run-with-halfstack.sh backend.ai-manager etcd put vfolder/_mount /mnt
~/manager (venv-manager) $ scripts/run-with-halfstack.sh backend.ai-manager etcd update-images -f sample-configs/image-metadata.yml
~/manager (venv-manager) $ scripts/run-with-halfstack.sh backend.ai-manager etcd update-aliases -f sample-configs/image-aliases.yml
~/manager (venv-manager) $ cp alembic.ini.sample alembic.ini
~/manager (venv-manager) $ edit alembic.ini  # match the config with the halfstack
~/manager (venv-manager) $ scripts/run-with-halfstack.sh backend.ai-manager schema oneshot head
~/manager (venv-manager) $ scripts/run-with-halfstack.sh backend.ai-manager fixture populate example_keypair
```

To run tests:

```console
~/manager (venv-manager) $ scripts/run-with-halfstack.sh python -m pytest -m 'not integration'
```

To run tests including integration tests, you first need to install the agent in the same virtualenv.

```console
~$ git clone https://github.com/lablup/backend.ai-agent agent
~$ cd manager
~/manager (venv-manager) $ pip install -e ../agent
~/manager (venv-manager) $ scripts/run-with-halfstack.sh python -m pytest
```
