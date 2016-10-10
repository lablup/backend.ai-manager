import argparse
from namedlist import namedlist
from ipaddress import ip_address
import pathlib
import os
import sys
from types import SimpleNamespace

from sorna.argparse import host_port_pair, ipaddr, path, port_no
import configargparse


def load_config(argv=None, legacy=False):
    parser = configargparse.ArgumentParser()
    if not legacy:
        parser.add('--service-ip', env_var='SORNA_SERVICE_IP', type=ipaddr, default=ip_address('0.0.0.0'),
                   help='The IP where the API gateway server listens on. (default: 0.0.0.0)')
        parser.add('--service-port', env_var='SORNA_SERVICE_PORT', type=port_no, default=0,
                   help='The TCP port number where the API gateway server listens on. '
                        '(default: 8080, 8443 when SSL is enabled) '
                    'To run in production, you need the root privilege to use the standard 80/443 ports.')
    parser.add('--manager-port', env_var='SORNA_MANAGER_PORT', type=port_no, default=5001,  # for legacy
               help='The TCP port number where the legacy manager listens on. (default: 5001)')
    parser.add('--agent-port',   env_var='SORNA_AGENT_PORT', type=port_no, default=6001,
               help='The TCP port number where the agent instances are listening on. (default: 6001)')
    if not legacy:
        parser.add('--ssl-cert', env_var='SORNA_SSL_CERT', type=path, default=None,
                   help='The path to an SSL certificate file. '
                        'It may contain inter/root CA certificates as well. '
                        '(default: None)')
        parser.add('--ssl-key', env_var='SORNA_SSL_KEY', type=path, default=None,
                   help='The path to the private key used to make requests for the SSL certificate. '
                        '(default: None)')
        parser.add('--db-addr', env_var='SORNA_DB_ADDR', type=host_port_pair, default=('localhost', 5432),
                   help='The hostname-port pair of a database server. (default: localhost:5432)')
        parser.add('--db-name', env_var='SORNA_DB_NAME', type=str, default='sorna',
                   help='The database name. (default: sorna)')
        parser.add('--db-user', env_var='SORNA_DB_USER', type=str, default='postgres',
                   help='The username to authenticate to the database server. (default: postgres)')
        parser.add('--db-password', env_var='SORNA_DB_PASSWORD', type=str, default='develove',
                   help='The password to authenticate to the database server. (default: develove)')
    parser.add('--redis-addr', env_var='SORNA_REDIS_ADDR', type=host_port_pair, default=('localhost', 6379),
               help='The hostname-port pair of a redis server. (default: localhost:6379)')
    parser.add('--kernel-ip-override', env_var='SORNA_KERNEL_IP_OVERRIDE', type=ip_address, default=None,
               help='The IP address that overrides the actual IP address of kernel containers '
                    'when responding to our clients. '
                    'This option is used only in local development setups where both '
                    'all Sorna components and a dockerized front-end service '
                    'that uses the legacy ZMQ interface run on the same host. '
                    '"192.168.65.1" is a special IP that Docker containers '
                    '(e.g., a front-end container) use to access the host-side '
                    'services or ports opened by anonymous kernel containers.')
    args = parser.parse_args(args=argv)
    return args
