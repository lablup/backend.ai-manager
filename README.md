Sorna Manager
=============

Package Structure
-----------------

 * sorna
   * manager: instance registry manager and an old ZMQ-based interface (to be deprecated)
   * gateway: RESTful API gateway based on aiohttp

Installation
------------

Sorna Manager requires Python 3.5 or higher.  We highly recommend to use
[pyenv](https://github.com/yyuu/pyenv) for an isolated setup of custom Python
versions that might be different from default installations managed by your OS
or Linux distros.

```sh
pip install sorna-manager
```

### For development:

We recommend to use virtual environments in Python.
You may share a virtual environment with other Sorna projects.

```sh
git clone https://github.com/lablup/sorna-manager.git
python -m venv venv-sorna
source venv-sorna/bin/activate
python setup.py develop
```

Running and Deployment
----------------------

### Prepare databases.

 * An RDBMS (PostgreSQL)
 * A Redis server
   - Sorna Manager uses the following [database IDs](http://redis.io/commands/SELECT)
     - 1: to track status and availability of kernel sessions
     - 2: to track status and availability of instances (agents)
     - 3: to track session IDs
     - These IDs are defined in [sorna-common](https://github.com/lablup/sorna-common/blob/master/sorna/defs.py)

### Configuration

You need to specify configuration parameters using either CLI arguments or environment
variables.  The default values are for development settings so you should set most of them
explicitly in production.
For details about arguments and their equivalent environment variable names,
run the server module with `--help`.

### Running the API gateway server from a command line:

```sh
python -m sorna.gateway.server
```

### Running the legacy server from a command line:

```sh
python -m sorna.manager.server
```

### Example supervisord config:

```dosini
[program:sorna-manager]
stopsignal = TERM
stopasgroup = true
command = /home/sorna/run-manager.sh
```

### TCP Port numbers to open

 * 5001 (for legacy ZeroMQ-based interface)
 * 8080 / 8443 (for local development)
 * 80 / 443 (for HTTP/HTTPS API requests in production)

