from ipaddress import ip_address
import logging, logging.config

from ai.backend.common.argparse import HostPortPair, host_port_pair, ipaddr, port_no
import configargparse


def load_config(argv=None, extra_args_func=None):
    parser = configargparse.ArgumentParser()
    parser.add('--agent-port', env_var='BACKEND_AGENT_PORT',
               type=port_no, default=6001,
               help='The TCP port number where the agent instances are listening on.'
                    ' (default: 6001)')
    parser.add('--redis-addr', env_var='BACKEND_REDIS_ADDR', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 6379),
               help='The hostname-port pair of a redis server. '
                    '(default: localhost:6379)')
    parser.add('--db-addr', env_var='BACKEND_DB_ADDR', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 5432),
               help='The hostname-port pair of a database server. '
                    '(default: localhost:5432)')
    parser.add('--db-name', env_var='BACKEND_DB_NAME',
               type=str, default='sorna',
               help='The database name. (default: sorna)')
    parser.add('--db-user', env_var='BACKEND_DB_USER',
               type=str, default='postgres',
               help='The username to authenticate to the database server. '
                    '(default: postgres)')
    parser.add('--db-password', env_var='BACKEND_DB_PASSWORD',
               type=str, default='develove',
               help='The password to authenticate to the database server. '
                    '(default: develove)')
    parser.add('--debug', env_var='BACKEND_DEBUG',
               action='store_true', default=False,
               help='Set the debug mode and verbose logging. (default: false)')
    parser.add('-v', '--verbose', env_var='BACKEND_VERBOSE',
               action='store_true', default=False,
               help='Set even more verbose logging which includes all SQL '
                    'statements issued. (default: false)')
    parser.add('--kernel-ip-override', env_var='BACKEND_KERNEL_IP_OVERRIDE',
               type=ipaddr, default=None,
               help='The IP address that overrides the actual IP address of kernel '
                    'containers when responding to our clients. '
                    'This option is used only in local development setups where all '
                    'the Backend.AI deamons and a dockerized front-end service '
                    'that uses the legacy ZMQ interface run on the same host. '
                    '"192.168.65.1" is a special IP that Docker containers '
                    '(e.g., a front-end container) use to access the host-side '
                    'services or ports opened by anonymous kernel containers.')
    if extra_args_func:
        extra_args_func(parser)
    args = parser.parse_args(args=argv)
    return args


def init_logger(config):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
                'field_styles': {'levelname': {'color': 'black', 'bold': True},
                                 'name': {'color': 'black', 'bold': True},
                                 'asctime': {'color': 'black'}},
                'level_styles': {'info': {'color': 'cyan'},
                                 'debug': {'color': 'green'},
                                 'warning': {'color': 'yellow'},
                                 'error': {'color': 'red'},
                                 'critical': {'color': 'red', 'bold': True}},
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'colored',
                'stream': 'ext://sys.stderr',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'INFO',
            },
            'aiopg': {
                'handlers': ['console'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
            'ai.backend': {
                'handlers': ['console'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
        },
    })
