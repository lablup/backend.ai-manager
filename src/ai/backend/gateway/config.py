import argparse
from ipaddress import ip_address

from ai.backend.common.argparse import HostPortPair, host_port_pair, port_no
import configargparse


def load_config(argv=None, extra_args_funcs=()):
    parser = configargparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add('--namespace', env_var='BACKEND_NAMESPACE',
               type=str, default='local',
               help='The namespace of this Backend.AI cluster.')
    parser.add('--etcd-addr', env_var='BACKEND_ETCD_ADDR',
               type=host_port_pair, metavar='HOST:PORT',
               default=HostPortPair(ip_address('127.0.0.1'), 2379),
               help='The host:port pair of the etcd cluster or its proxy.')
    parser.add('--etcd-user', env_var='BACKEND_ETCD_USER',
               type=str, default=None,
               help='The username for the etcd cluster.')
    parser.add('--etcd-password', env_var='BACKEND_ETCD_PASSWORD',
               type=str, default=None,
               help='The password of the user for the etcd cluster.')
    parser.add('--agent-port', env_var='BACKEND_AGENT_PORT',
               type=port_no, default=6001,
               help='The TCP port number where agent instances are listening on.')
    parser.add('--redis-addr', env_var='BACKEND_REDIS_ADDR', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 6379),
               help='The hostname-port pair of a redis server.')
    parser.add('--redis-password', env_var='BACKEND_REDIS_PASSWORD',
               type=str, default=None,
               help='The authentication password for the Redis server.')
    parser.add('--db-addr', env_var='BACKEND_DB_ADDR', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 5432),
               help='The hostname-port pair of a database server.')
    parser.add('--db-name', env_var='BACKEND_DB_NAME',
               type=str, default='sorna',
               help='The database name.')
    parser.add('--db-user', env_var='BACKEND_DB_USER',
               type=str, default='postgres',
               help='The username to authenticate to the database server.')
    parser.add('--db-password', env_var='BACKEND_DB_PASSWORD',
               type=str, default='develove',
               help='The password to authenticate to the database server.')
    parser.add('--disable-plugins', env_var='BACKEND_DISABLE_PLUGINS',
               type=str, default='',
               help='A comma-separated blacklist of app plugins not to use.')
    parser.add('--skip-sslcert-validation',
               env_var='BACKEND_SKIP_SSLCERT_VALIDATION',
               action='store_true', default=False,
               help='Let the underlying HTTP library to skip SSL certificate '
                    'validation (e.g., for accessing private Docker registries).  '
                    'Only enable this for setups using privately signed '
                    'certificates.')
    for func in extra_args_funcs:
        func(parser)
    args = parser.parse_args(args=argv)
    return args
