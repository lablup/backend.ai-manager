import os
import re
import secrets
import sys
from pathlib import Path
from pprint import pformat
from typing import (
    Any,
    Mapping,
)

import click
import trafaret as t
from ai.backend.common import config, validators as tx
from ai.backend.common.etcd import AsyncEtcd

_max_cpu_count = os.cpu_count()
_file_perm = (Path(__file__).parent / 'server.py').stat()

DEFAULT_CHUNK_SIZE = 256 * 1024  # 256 KiB
DEFAULT_INFLIGHT_CHUNKS = 8
_RESERVED_VFOLDER_PATTERNS = [r'^\.[a-z0-9]+rc$', r'^\.[a-z0-9]+_profile$']
RESERVED_DOTFILES = ['.terminfo', '.jupyter', '.ssh', '.ssh/authorized_keys', '.local', '.config']
RESERVED_VFOLDERS = ['.terminfo', '.jupyter', '.tmux.conf', '.ssh']
RESERVED_VFOLDER_PATTERNS = [re.compile(x) for x in _RESERVED_VFOLDER_PATTERNS]

manager_config_iv = t.Dict({
    t.Key('db'): t.Dict({
        t.Key('type', default='postgresql'): t.Enum('postgresql'),
        t.Key('addr'): tx.HostPortPair,
        t.Key('name'): tx.Slug[2:64],
        t.Key('user'): t.String,
        t.Key('password'): t.String,
    }),
    t.Key('manager'): t.Dict({
        t.Key('num-proc', default=_max_cpu_count): t.Int[1:_max_cpu_count],
        t.Key('user', default=None): tx.UserID(default_uid=_file_perm.st_uid),
        t.Key('group', default=None): tx.GroupID(default_gid=_file_perm.st_gid),
        t.Key('service-addr', default=('0.0.0.0', 8080)): tx.HostPortPair,
        t.Key('heartbeat-timeout', default=5.0): t.Float[1.0:],  # type: ignore
        t.Key('secret', default=None): t.Null | t.String,
        t.Key('ssl-enabled', default=False): t.Bool | t.StrBool,
        t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
        t.Key('ssl-privkey', default=None): t.Null | tx.Path(type='file'),
        t.Key('event-loop', default='asyncio'): t.Enum('asyncio', 'uvloop'),
        t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                       allow_nonexisting=True,
                                                       allow_devnull=True),
        t.Key('hide-agents', default=False): t.Bool,
        t.Key('importer-image', default='lablup/importer:manylinux2010'): t.String,
    }).allow_extra('*'),
    t.Key('docker-registry'): t.Dict({
        t.Key('ssl-verify', default=True): t.Bool | t.StrBool,
    }).allow_extra('*'),
    t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
    t.Key('debug'): t.Dict({
        t.Key('enabled', default=False): t.Bool | t.StrBool,
        t.Key('log-events', default=False): t.Bool | t.StrBool,
        t.Key('log-scheduler-ticks', default=False): t.Bool | t.StrBool,
    }).allow_extra('*'),
}).merge(config.etcd_config_iv).allow_extra('*')

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
}).allow_extra('*')

_shdefs: Mapping[str, Any] = {
    'system': {
        'timezone': 'UTC',
    },
    'api': {
        'allow-origins': '*',
    },
    'redis': {
        'addr': '127.0.0.1:6379',
        'password': None,
    },
    'network': {
        'subnet': {
            'agent': '0.0.0.0/0',
            'container': '0.0.0.0/0',
        },
    },
    'watcher': {
        'token': None,
    }
}

shared_config_iv = t.Dict({
    t.Key('system', default=_shdefs['system']): t.Dict({
        t.Key('timezone', default=_shdefs['system']['timezone']): tx.TimeZone,
    }).allow_extra('*'),
    t.Key('api', default=_shdefs['api']): t.Dict({
        t.Key('allow-origins', default=_shdefs['api']['allow-origins']): t.String,
    }).allow_extra('*'),
    t.Key('redis', default=_shdefs['redis']): t.Dict({
        t.Key('addr', default=_shdefs['redis']['addr']): tx.HostPortPair,
        t.Key('password', default=_shdefs['redis']['password']): t.Null | t.String,
    }).allow_extra('*'),
    t.Key('plugins', default={}): t.Mapping(t.String, t.Mapping(t.String, t.Any)),
    t.Key('network', default=_shdefs['network']): t.Dict({
        t.Key('subnet', default=_shdefs['network']['subnet']): t.Dict({
            t.Key('agent', default=_shdefs['network']['subnet']['agent']): tx.IPNetwork,
            t.Key('container', default=_shdefs['network']['subnet']['container']): tx.IPNetwork,
        }).allow_extra('*'),
    }).allow_extra('*'),
    t.Key('watcher', default=_shdefs['watcher']): t.Dict({
        t.Key('token', default=_shdefs['watcher']['token']): t.Null | t.String,
    }).allow_extra('*'),
}).allow_extra('*')


def load(config_path: Path = None, debug: bool = False):

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
        if 'debug' in cfg and cfg['debug']['enabled']:
            print('== Manager configuration ==', file=sys.stderr)
            print(pformat(cfg), file=sys.stderr)
        cfg['_src'] = cfg_src_path
        if cfg['manager']['secret'] is None:
            cfg['manager']['secret'] = secrets.token_urlsafe(16)
    except config.ConfigurationError as e:
        print('Validation of manager configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()
    else:
        return cfg


async def load_shared(etcd: AsyncEtcd):
    raw_cfg = await etcd.get_prefix('config')
    try:
        cfg = shared_config_iv.check(raw_cfg)
    except config.ConfigurationError as e:
        print('Validation of shared etcd configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()
    else:
        return cfg
