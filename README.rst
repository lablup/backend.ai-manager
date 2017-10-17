Backend.AI Manager with API Gateway
===================================

Package Structure
-----------------

* ai.backend

  * manager: Abstraction of agents and computation kernels

  * gateway: RESTful API gateway based on aiohttp

Installation
------------

Backend.AI Agent requires Python 3.6 or higher.  We highly recommend to use
`pyenv <https://github.com/yyuu/pyenv>`_ for an isolated setup of custom Python
versions that might be different from default installations managed by your OS
or Linux distros.

.. code-block:: sh

   pip install backend.ai-manager

To use optional monitoring service (Datadog and Sentry) supports, add ``monitor``
extras tag to the pip command:

.. code-block:: sh

   pip install 'backend.ai-manager[monitor]'


For development
~~~~~~~~~~~~~~~

We recommend to use virtual environments in Python.
You may share a virtual environment with other Backend.AI projects.

.. code-block:: sh

   git clone https://github.com/lablup/backend.ai-manager.git
   python -m venv /home/user/venv
   source /home/user/venv/bin/activate
   pip install -U pip setuptools   # ensure latest versions
   pip install -U -r requirements-dev.txt

The above example shows a standalone installation process for the manager, but
normally you would want to install all other depedencies like agents and
databases for integration tests.

Running and Deployment
----------------------

Prepare databases
~~~~~~~~~~~~~~~~~

* An RDBMS (PostgreSQL)

* An etcd (v3) server

* A Redis server

  - The manager uses the following `database IDs <http://redis.io/commands/SELECT>`_

    - 0 (default): to track realtime performance metrics and statistics of computing sessions

    - 1: to track realtime request rate-limits of each API access key

Check out `README on the meta-repo <https://github.com/lablup/backend.ai>`_ for the
docker-compose example to run above databases with a single command.

Configuration
~~~~~~~~~~~~~

You need to specify configuration parameters using either CLI arguments or environment
variables.  The default values are for development settings so you should set most of them
explicitly in production.
For details about arguments and their equivalent environment variable names,
run the server module with ``--help``.

.. code-block:: console

   $ cp alembic.ini.sample alembic.ini
   $ edit alembic.ini
   $ python -m ai.backend.manager.cli schema oneshot head
   Creating tables...
   Stamping alembic version to ...

Optionally you can populate pre-defined fixtures.
You may add your own ones in fixtures directory for deployment.
``example_keypair`` fixture is required to run the test suite.

.. code-block:: console

   $ python -m ai.backend.manager.cli fixture populate example_keypair
   populating fixture 'example_keypair'

Running the API gateway server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

   $ python -m ai.backend.gateway.server \
            --etcd-addr localhost:2379 \
            --namespace my-cluster \
            --redis-addr localhost:6379 \
            --db-addr localhost:5432 \
            --db-name my-cluster \
            --db-user dbuser \
            --db-password dbpass \
            --docker-registry docker.example.com:5000 \
            --service-ip 127.0.0.1 \
            --service-port 8080 \
            --events-port 5002


The gateway server can directly serve the public traffic, either via plain HTTP
or HTTPS (with ``--ssl-cert`` and ``--ssl-key`` options), but we recommend to
use a dedicated reverse-proxy such as `nginx <https://nginx.org/en/>`_ for
advanced HTTPS handling (e.g., SNI).
Note that the gateway itself can fully utilize all the CPU cores in the system
without limits from GIL (global interpreter lock).

Please check out ``--help`` to see more options and their defaults.

Example configs
~~~~~~~~~~~~~~~

``/etc/supervisor/conf.d/manager.conf``:

.. code-block:: dosini

   [program:backend.ai-manager]
   user = user
   stopsignal = TERM
   stopasgroup = true
   command = /home/user/run-manager.sh

``/home/user/run-manager.sh``:

.. code-block:: sh

   #!/bin/sh
   source /home/user/venv/bin/activate
   # AWS API keypair for S3 file uploads (optional)
   export AWS_ACCESS_KEY_ID="..."
   export AWS_SECRET_ACCESS_KEY="..."
   # Datadog monitoring (optional)
   export DATADOG_API_KEY="..."
   export DATADOG_APP_KEY="..."
   # Sentry monitoring (optional)
   export RAVEN_URI="..."
   # the main command
   exec python -m ai.backend.gateway.server \
        --etcd-addr localhost:2379 \
        --namespace my-cluster \
        --redis-addr localhost:6379 \
        # ... other options ...
        --service-ip 127.0.0.1 \
        --service-port 8080

``/etc/nginx/sites-enabled/gateway``:

.. code-block:: text

   ssl_session_cache shared:SSL:10m;
   ssl_session_timeout 10m;
   ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
   ssl_prefer_server_ciphers on;
   ssl_ciphers EECDH+CHACHA20:EECDH+AES128:RSA+AES128:EECDH+AES256:RSA+AES256:EECDH+3DES:RSA+3DES:!MD5;

   map $http_connection $connection_upgrade {
       default upgrade;
       ''      close;
   }

   server {
       listen 443 ssl;
       server_name my-cluster.example.com;
       charset utf-8;
       client_max_body_size 32M;

       ssl_certificate /path/to/ssl.crt
       ssl_certificate_key /path/to/ssl.key
       add_header Strict-Transport-Security "max-age=31536000; includeSubdomains";

       location / {
           proxy_pass http://127.0.0.1:8080;
           proxy_pass_request_headers on;
           proxy_set_header Host "my-cluster.example.com";
           proxy_redirect off;
           proxy_buffering off;
           proxy_read_timeout 600s;
       }

       location ~ ^/v\d+/stream/ {
           proxy_pass http://127.0.0.1:8080;
           proxy_pass_request_headers on;
           proxy_set_header Host "my-cluster.example.com";
           proxy_redirect off;
           proxy_buffering off;
           proxy_read_timeout 60s;

           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection $connection_upgrade;
       }
   }


Networking
~~~~~~~~~~

The manager and agent should run in the same local network or different
networks reachable via VPNs.

You need to check the firewall settings to allow the following access patterns
(all ports are TCP):

* The manager's service port: open to the reverse-proxy or the public Internet
* The manager's events port: open to the agents
* The etcd's service port: open to the manager and agents
* The redis' service port: open to the manager and agents
* The (optional) private docker registry's service port: open to the manager and agents
* The database's service port: open to the manager
* The agents' ALL ports: open to the manager

Note that etcd/redis server may run on different physical servers or cloud
instances as long as the manager and agents can access them.
The PostgreSQL database is only accessed by the manager.
