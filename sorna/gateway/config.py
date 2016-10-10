import os
from types import SimpleNamespace


def load_config():

    c = SimpleNamespace()

    c.service_ip = os.environ.get('SORNA_SERVICE_IP', '0.0.0.0')
    c.service_port = int(os.environ.get('SORNA_SERVICE_PORT', 0))
    c.agent_port = int(os.environ.get('SORNA_AGENT_PORT', 6001))

    c.ssl_cert = os.environ.get('SORNA_SSL_CERT', None)
    c.ssl_key = os.environ.get('SORNA_SSL_KEY', None)

    c.database = {
        'engine': 'pgsql',  # currently fixed
        'host': os.environ.get('SORNA_DB_HOST', 'localhost'),
        'port': int(os.environ.get('SORNA_DB_PORT', 5432)),
        'dbname': os.environ.get('SORNA_DB_NAME', 'sorna'),
        'user': os.environ.get('SORNA_DB_USER', 'postgres'),
        'password': os.environ.get('SORNA_DB_PASSWORD', 'develove'),
    }

    return c
