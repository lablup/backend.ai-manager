Sorna Manager
=============

Package Structure
-----------------

 * sorna
   * manager: instance registry manager and an old ZMQ-based interface (to be deprecated)
   * gateway: RESTful API gateway based on aiohttp

Installation
------------

Sorna Manager requires Python 3.5 or higher.
We highly recommend to use [pyenv](https://github.com/yyuu/pyenv) to use an
isolated setup with custom Python versions that might not be supported by your
OS or Linux distributions by default.

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
python setup.py develop --user
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

### Running from a command line:

```sh
python -m sorna.manager.server
```

**NOTE:** If you are using Docker for Mac, you need an extra argument like:

```sh
python -m sorna.manager.server --kernel-ip-override=192.168.65.1
```

so that host-side Python processes can connect to the kernel containers.

For details about arguments, run the server with `--help`.


### Example supervisord config:

```dosini
[program:sorna-manager]
stopsignal = TERM
stopasgroup = true
command = /home/sorna/run-manager.sh
```

### TCP Port numbers to open

 * 6001 (for legacy ZeroMQ-based interface)
 * 443 (for HTTPS API requests)
 * 80 (optional for HTTP API requests)

