import os
import sys
from pathlib import Path
from pprint import pformat, pprint

import click
import trafaret as t
from ai.backend.common import config, validators as tx

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
}).allow_extra('*')


def load(config_path: Path, debug: bool = False):
    max_cpu_count = os.cpu_count()

    manager_config_iv = t.Dict({
        t.Key('db'): t.Dict({
            t.Key('type', default='postgresql'): t.Enum('postgresql'),
            t.Key('addr'): tx.HostPortPair,
            t.Key('name'): tx.Slug[2:64],
            t.Key('user'): t.String,
            t.Key('password'): t.String,
        }),
        t.Key('manager'): t.Dict({
            t.Key('num-proc', default=max_cpu_count): t.Int[1:max_cpu_count],
            t.Key('api-listen-addr', default=('0.0.0.0', 8080)): tx.HostPortPair,
            t.Key('event-listen-addr', default=('127.0.0.1', 5002)): tx.HostPortPair,
            t.Key('heartbeat-timeout', default=5.0): t.Float[1.0:],
            t.Key('ssl-enabled', default=False): t.Bool,
            t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
            t.Key('ssl-privkey', default=None): t.Null | tx.Path(type='file'),
            t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                           allow_nonexisting=True,
                                                           allow_devnull=True),
        }).allow_extra('*'),
        t.Key('docker-registry'): t.Dict({
            t.Key('ssl-verify', default=True): t.Bool,
        }).allow_extra('*'),
        t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
        t.Key('debug'): t.Dict({
            t.Key('enabled', default=False): t.Bool,
        }).allow_extra('*'),
    }).allow_extra('*').merge(config.etcd_config_iv)

    # Determine where to read configuration.
    raw_cfg, cfg_src_path = config.read_from_file(config_path, 'manager')

    # Override the read config with environment variables (for legacy).
    config.override_with_env(raw_cfg, ('etcd', 'namespace'), 'BACKEND_NAMESPACE')
    config.override_with_env(raw_cfg, ('etcd', 'addr'), 'BACKEND_ETCD_ADDR')
    config.override_with_env(raw_cfg, ('etcd', 'user'), 'BACKEND_ETCD_USER')
    config.override_with_env(raw_cfg, ('etcd', 'password'), 'BACKEND_ETCD_PASSWORD')
    config.override_with_env(raw_cfg, ('db', 'addr'), 'BACKEND_DB_ADDR')
    config.override_with_env(raw_cfg, ('db', 'name'), 'BACKEND_DB_NAME')
    config.override_with_env(raw_cfg, ('db', 'user'), 'BACKEND_DB_USER')
    config.override_with_env(raw_cfg, ('db', 'password'), 'BACKEND_DB_PASSWORD')
    config.override_with_env(raw_cfg, ('manager', 'num-proc'), 'BACKEND_GATEWAY_NPROC')
    config.override_with_env(raw_cfg, ('manager', 'ssl-cert'), 'BACKEND_SSL_CERT')
    config.override_with_env(raw_cfg, ('manager', 'ssl-privkey'), 'BACKEND_SSL_KEY')
    config.override_with_env(raw_cfg, ('manager', 'pid-file'), 'BACKEND_PID_FILE')
    config.override_with_env(raw_cfg, ('manager', 'api-listen-addr', 'host'),
                             'BACKEND_SERVICE_IP')
    config.override_with_env(raw_cfg, ('manager', 'api-listen-addr', 'port'),
                             'BACKEND_SERVICE_PORT')
    config.override_with_env(raw_cfg, ('manager', 'event-listen-addr', 'host'),
                             'BACKEND_ADVERTISED_MANAGER_HOST')
    config.override_with_env(raw_cfg, ('manager', 'event-listen-addr', 'port'),
                             'BACKEND_EVENTS_PORT')
    config.override_with_env(raw_cfg, ('docker-registry', 'ssl-verify'),
                             'BACKEND_SKIP_SSLCERT_VALIDATION')
    if debug:
        config.override_key(raw_cfg, ('debug', 'enabled'), True)
        config.override_key(raw_cfg, ('logging', 'level'), 'DEBUG')
        config.override_key(raw_cfg, ('logging', 'pkg-ns', 'ai.backend'), 'DEBUG')
        config.override_key(raw_cfg, ('logging', 'pkg-ns', 'aiohttp'), 'DEBUG')

    # Validate and fill configurations
    # (allow_extra will make configs to be forward-copmatible)
    try:
        cfg = config.check(raw_cfg, manager_config_iv)
        if 'debug'in cfg and cfg['debug']['enabled']:
            print('== Agent configuration ==')
            pprint(cfg)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('Validation of agent configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()
    else:
        return cfg
