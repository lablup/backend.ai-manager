Backend.AI-Manager and API Gateway
=============================

Package Structure
-----------------

 * Backend.AI
   * manager: Abstraction of agents and computation kernels
   * gateway: RESTful API gateway based on aiohttp

Installation
------------

Backend.AI Manager requires Python 3.6 or higher.  We highly recommend to use
[pyenv](https://github.com/yyuu/pyenv) for an isolated setup of custom Python
versions that might be different from default installations managed by your OS
or Linux distros.

```console
$ pip install backend.ai-manager
```

### For development:

We recommend to use virtual environments in Python.
You may share a virtual environment with other Backend.AI projects.

```console
$ git clone https://github.com/lablup/backend.ai-manager.git
$ python -m venv venv-backend.ai
$ source venv-backend.ai/bin/activate
$ pip install -U pip setuptools  # ensure latest versions!
$ pip install -r requirements-dev.txt
```

Running and Deployment
----------------------

### Prepare databases.

 * An RDBMS (PostgreSQL)
 * A Redis server
   - Backend.AI Manager uses the following [database IDs](http://redis.io/commands/SELECT)
     - 1: to track status and availability of kernel sessions
     - 2: to track status and availability of instances (agents)
     - 3: to track session IDs
     - These IDs are defined in [backend.ai-common](https://github.com/lablup/backend.ai-common/blob/master/backend.ai/defs.py)

### Configuration

You need to specify configuration parameters using either CLI arguments or environment
variables.  The default values are for development settings so you should set most of them
explicitly in production.
For details about arguments and their equivalent environment variable names,
run the server module with `--help`.

### Running the API gateway server from a command line:

```console
$ python -m backend.ai.gateway.server
```

### Example supervisord config:

```dosini
[program:backend.ai-manager]
stopsignal = TERM
stopasgroup = true
command = /home/backend.ai/run-manager.sh
```

### TCP Port numbers to open

 * 5001 (for legacy ZeroMQ-based interface)
 * 8080 / 8443 (for local development)
 * 80 / 443 (for HTTP/HTTPS API requests in production)

