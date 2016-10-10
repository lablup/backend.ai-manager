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

All configurations are done using environment variables.

 * `SORNA_SERVICE_IP`: The IP where the API gateway server listens on (default: 0.0.0.0).
 * `SORNA_SERVICE_PORT`: The TCP port number where the API gateway server listens on (default: 8080, 8443 when SSL is enabled).
   To run in production, you need the root privilege to use the standard 80/443 ports.
 * `SORNA_AGENT_PORT`: The TCP port number where the agent instances are listening on (default: 6001).
 * `SORNA_SSL_CERT`: The path to an SSL certificate file.  It may contain inter/root CA certificates as well (default: None).
 * `SORNA_SSL_KEY`: The path to the private key used to make requests for the SSL certificate (default: None).
 * `SORNA_DB_HOST`: The hostname of a database server (default: localhost).
 * `SORNA_DB_NAME`: The database name (default: sorna).
 * `SORNA_DB_USER`: The username to authenticate to the database server (default: postgres).
 * `SORNA_DB_PASSWORD`: The password to authenticate to the database server (default: develove).

### Running the API gateway server from a command line:

```sh
SORNA_SERVICE_PORT=
```


### Running the legacy server from a command line:

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

