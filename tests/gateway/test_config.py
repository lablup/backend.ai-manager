from ipaddress import ip_address

from ai.backend.common.argparse import host_port_pair, port_no
from ai.backend.gateway.config import load_config
from ai.backend.gateway.server import gw_args


def test_args_parse_by_load_config():
    # basic args
    agent_port = 6003
    redis_addr = '127.0.0.1:6381'
    db_addr = '127.0.0.1:5434'
    db_name = 'backendai-test'
    db_user = 'postgres-test'
    db_password = 'develove-test'

    # extra args
    namespace = 'local-test'
    etcd_addr = '127.0.0.1:2381'
    events_port = 5002

    argv = [
        '--agent-port', str(agent_port),
        '--redis-addr', redis_addr,
        '--db-addr', db_addr,
        '--db-name', db_name,
        '--db-user', db_user,
        '--db-password', db_password,
        '--namespace', namespace,
        '--etcd-addr', etcd_addr,
        '--events-port', str(events_port),
    ]

    args = load_config(argv, extra_args_funcs=(gw_args,))

    assert args.agent_port == agent_port
    assert args.redis_addr == host_port_pair(redis_addr)
    assert args.db_addr == host_port_pair(db_addr)
    assert args.db_name == db_name
    assert args.db_user == db_user
    assert args.db_password == db_password

    assert args.namespace == namespace
    assert args.etcd_addr == host_port_pair(etcd_addr)
    assert args.events_port == port_no(events_port)
    assert args.docker_registry == 'lablup'
    assert args.heartbeat_timeout == 5.0
    assert args.service_ip == ip_address('0.0.0.0')
    assert args.service_port == 0
