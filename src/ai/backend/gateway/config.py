from __future__ import annotations

"""
Configuration Schema on etcd
----------------------------

The etcd (v3) itself is a flat key-value storage, but we use its prefix-based filtering
by using a directory-like configuration structure.
At the root, it contains "/sorna/{namespace}" as the common prefix.

In most cases, a single global configurations are sufficient, but cluster administrators
may want to apply different settings (e.g., resource slot types, vGPU sizes, etc.)
to different scaling groups or even each node.

To support such requirements, we add another level of prefix named "configuration scope".
There are three types of configuration scopes:

 * Global
 * Scaling group
 * Node

When reading configurations, the underlying `ai.backend.common.etcd.AsyncEtcd` class
returns a `collections.ChainMap` instance that merges three configuration scopes
in the order of node, scaling group, and global, so that node-level configs override
scaling-group configs, and scaling-group configs override global configs if they exist.

Note that the global scope prefix may be an empty string; this allows use of legacy
etcd databases without explicit migration.  When the global scope prefix is an empty string,
it does not make a new depth in the directory structure, so "{namespace}/config/x" (not
"{namespace}//config/x"!) is recognized as the global config.

Notes on Docker registry configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A registry name contains the host, port (only for non-standards), and the path.
So, they must be URL-quoted (including slashes) to avoid parsing
errors due to intermediate slashes and colons.
Alias keys are also URL-quoted in the same way.

{namespace}
 + ''  # ConfigScoeps.GLOBAL
   + config
     + system
       - timezone: "UTC"   # pytz-compatible timezone names (e.g., "Asia/Seoul")
     + api
       - allow-origins: "*"
       + resources
         - group_resource_visibility: "true"  # return group resource status in check-presets
                                              # (default: false)
     + docker
       + image
         - auto_pull: "digest" (default) | "tag" | "none"
       + registry
         + "index.docker.io": "https://registry-1.docker.io"
           - username: "lablup"
         + {registry-name}: {registry-URL}  # {registry-name} is url-quoted
           - username: {username}
           - password: {password}
           - type: "docker" | "harbor"
           - project: "project1-name,porject2-name,..."  # harbor only
           - ssl-verify: "yes" | "no"
         ...
     + redis
       - addr: "{redis-host}:{redis-port}"
       - password: {password}
     + idle
       - enabled: "timeout,utilization"      # comma-separated list of checker names
       - app-streaming-packet-timeout: "5m"  # in seconds; idleness of app-streaming TCP connections
         # NOTE: idle checkers get activated AFTER the app-streaming packet timeout has passed.
       - checkers
         + "timeout"
           - treshold: "10m"
         + "utilization"
           + resource-thresholds
             + "cpu"
               - average: 30   # in percent
               - window: "1m"  # average for duration
             + "cuda.mem"
               - average: 15   # in percent
               - window: "1m"  # average for duration
               # NOTE: To use "cuda.mem" criteria, user programs must use
               #       an incremental allocation strategy for CUDA memory.
           - initial-grace_period: "30s"
     + resource_slots
       - {"cuda.device"}: {"count"}
       - {"cuda.mem"}: {"bytes"}
       - {"cuda.smp"}: {"count"}
       ...
     + plugins
       + accelerator
         + "cuda"
           - allocation_mode: "discrete"
           ...
       + scheduler
         + "fifo"
         + "lifo"
         + "drf"
         ...
     + network
       + subnet
         - agent: "0.0.0.0/0"
         - container: "0.0.0.0/0"
     + watcher
       - token: {some-secret}
   + volumes
     # pre-20.09
     - _mount: {path-to-mount-root-for-vfolder-partitions}
     - _default_host: {default-vfolder-partition-name}
     - _fsprefix: {path-prefix-inside-host-mounts}
     # 20.09 and later
     - default_host: "{default-proxy}:{default-volume}"
     + proxies:   # each proxy may provide multiple volumes
       + "local"  # proxy name
         - client_api: "localhost:6021"
         - manager_api: "localhost:6022"
         - secret: "xxxxxx..."       # for manager API
         - ssl_verify: true | false  # for manager API
       + "mynas1"
         - client_api: "proxy1.example.com:6021"
         - manager_api: "proxy1.example.com:6022"
         - secret: "xxxxxx..."       # for manager API
         - ssl_verify: true | false  # for manager API
       ...
   + images
     + _aliases
       - {alias}: "{registry}/{image}:{tag}"   # {alias} is url-quoted
       ...
     + {registry}   # url-quoted
       + {image}    # url-quoted
         + {tag}: {digest-of-config-layer}
           - size_bytes: {image-size-in-bytes}
           - accelerators: "{accel-name-1},{accel-name-2},..."
           + labels
             - {key}: {value}
             ...
           + resource
             + cpu
               - min
               - max   # may not be defined
             + mem
               - min
               - max   # may not be defined
             + {"cuda.smp"}
               - min
               - max   # treated as 0 if not defined
             + {"cuda.mem"}
               - min
               - max   # treated as 0 if not defined
             ...
         ...
       ...
     ...
   ...
 + nodes
   + manager
     - {instance-id}: "up"
     ...
   + redis: {"tcp://redis:6379"}
     - password: {redis-auth-password}
   + agents
     + {instance-id}: {"starting","running"}  # ConfigScopes.NODE
       - ip: {"127.0.0.1"}
       - watcher_port: {"6009"}
     ...
 + sgroup
   + {name}  # ConfigScopes.SGROUP
     - swarm-manager/token
     - swarm-manager/host
     - swarm-worker/token
     - iprange          # to choose ethernet iface when creating containers
     - resource_policy  # the name of scaling-group resource-policy in database
     + nodes
       - {instance-id}: 1  # just a membership set
"""

from abc import abstractmethod
import asyncio
from collections import UserDict, defaultdict
from contextvars import ContextVar
from decimal import Decimal
import logging
import os
from pathlib import Path
from pprint import pformat
import secrets
import sys
from typing import (
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    Final,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

import aiotools
import click
import trafaret as t
import yaml
import yarl

from ai.backend.common import config, validators as tx
from ai.backend.common.docker import (
    ImageRef, get_known_registries,
)
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.identity import get_instance_id
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    BinarySize, ResourceSlot,
    SlotName, SlotTypes,
    HostPortPair,
    current_resource_slots,
)
from ai.backend.common.exception import UnknownImageReference
from ai.backend.common.etcd import (
    quote as etcd_quote,
    unquote as etcd_unquote,
    ConfigScopes,
)

from .exceptions import ServerMisconfiguredError
from .manager import ManagerStatus
if TYPE_CHECKING:
    from ..manager.background import ProgressReporter
from ..manager.container_registry import get_container_registry
from ..manager.defs import INTRINSIC_SLOTS

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.config'))

_max_cpu_count = os.cpu_count()
_file_perm = (Path(__file__).parent / 'server.py').stat()

DEFAULT_CHUNK_SIZE: Final = 256 * 1024  # 256 KiB
DEFAULT_INFLIGHT_CHUNKS: Final = 8

shared_config_defaults = {
    'volumes/_mount': '/mnt',
    'volumes/_default_host': 'local',
    'volumes/_fsprefix': '/',
    'config/api/allow-origins': '*',
    'config/docker/image/auto_pull': 'digest',
}

current_vfolder_types: ContextVar[List[str]] = ContextVar('current_vfolder_types')

manager_local_config_iv = t.Dict({
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
        t.Key('ssl-enabled', default=False): t.ToBool,
        t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
        t.Key('ssl-privkey', default=None): t.Null | tx.Path(type='file'),
        t.Key('event-loop', default='asyncio'): t.Enum('asyncio', 'uvloop'),
        t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                       allow_nonexisting=True,
                                                       allow_devnull=True),
        t.Key('hide-agents', default=False): t.Bool,
        t.Key('importer-image', default='lablup/importer:manylinux2010'): t.String,
        t.Key('max-wsmsg-size', default=16 * (2**20)): t.ToInt,  # default: 16 MiB
    }).allow_extra('*'),
    t.Key('docker-registry'): t.Dict({  # deprecated in v20.09
        t.Key('ssl-verify', default=True): t.ToBool,
    }).allow_extra('*'),
    t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
    t.Key('debug'): t.Dict({
        t.Key('enabled', default=False): t.ToBool,
        t.Key('log-events', default=False): t.ToBool,
        t.Key('log-scheduler-ticks', default=False): t.ToBool,
    }).allow_extra('*'),
}).merge(config.etcd_config_iv).allow_extra('*')

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
    'plugins': {
        'accelerator': {},
        'scheduler': {},
    },
    'watcher': {
        'token': None,
    }
}

container_registry_iv = t.Dict({
    t.Key(''): tx.URL,
    t.Key('type', default="docker"): t.String,
    t.Key('username', default=None): t.Null | t.String,
    t.Key('password', default=None): t.Null | t.String,
    t.Key('project', default=None): t.Null | tx.StringList | t.List(t.String),
    t.Key('ssl-verify', default=True): t.ToBool,
}).allow_extra('*')

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
    t.Key('docker'): t.Dict({
        t.Key('registry'): t.Mapping(t.String, container_registry_iv),
    }).allow_extra('*'),
    t.Key('plugins', default=_shdefs['plugins']): t.Dict({
        t.Key('accelerator', default=_shdefs['plugins']['accelerator']):
            t.Mapping(t.String, t.Mapping(t.String, t.Any)),
        t.Key('scheduler', default=_shdefs['plugins']['scheduler']):
            t.Mapping(t.String, t.Mapping(t.String, t.Any)),
    }).allow_extra('*'),
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

volume_config_iv = t.Dict({
    t.Key('default_host'): t.String,
    t.Key('proxies'): t.Mapping(
        tx.Slug,
        t.Dict({
            t.Key('client_api'): t.String,
            t.Key('manager_api'): t.String,
            t.Key('secret'): t.String,
            t.Key('ssl_verify'): t.ToBool,
        }),
    ),
}).allow_extra('*')


ConfigWatchCallback = Callable[[Sequence[str]], Awaitable[None]]


class AbstractConfig(UserDict):

    _watch_callbacks: List[ConfigWatchCallback]

    def __init__(self, initial_data: Mapping[str, Any] = None) -> None:
        super().__init__(initial_data)
        self._watch_callbacks = []

    @abstractmethod
    async def reload(self) -> None:
        pass

    def add_watch_callback(self, cb: ConfigWatchCallback) -> None:
        self._watch_callbacks.append(cb)

    async def dispatch_watch_callbacks(self, updated_keys: Sequence[str]) -> None:
        for cb in self._watch_callbacks:
            await cb(updated_keys)


class LocalConfig(AbstractConfig):

    async def reload(self) -> None:
        raise NotImplementedError


def load(config_path: Path = None, debug: bool = False) -> LocalConfig:

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
        cfg = config.check(raw_cfg, manager_local_config_iv)
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
        return LocalConfig(cfg)


class SharedConfig(AbstractConfig):

    def __init__(self, app_ctx: Mapping[str, Any],
                 etcd_addr: HostPortPair,
                 etcd_user: Optional[str],
                 etcd_password: Optional[str],
                 namespace: str) -> None:
        # WARNING: importing etcd3/grpc must be done after forks.
        super().__init__()
        self.context = app_ctx
        credentials = None
        if etcd_user:
            credentials = {
                'user': etcd_user,
                'password': etcd_password,
            }
        scope_prefix_map = {
            ConfigScopes.GLOBAL: '',
            # TODO: provide a way to specify other scope prefixes
        }
        self.etcd = AsyncEtcd(etcd_addr, namespace, scope_prefix_map, credentials=credentials)

    async def close(self) -> None:
        await self.etcd.close()

    async def reload(self) -> None:
        raw_cfg = await self.etcd.get_prefix('config')
        try:
            cfg = shared_config_iv.check(raw_cfg)
        except config.ConfigurationError as e:
            print('Validation of shared etcd configuration has failed:', file=sys.stderr)
            print(pformat(e.invalid_data), file=sys.stderr)
            raise click.Abort()
        else:
            self.data = cfg

    def __hash__(self) -> int:
        # When used as a key in dicts, we don't care our contents.
        # Just treat it lke an opaque object.
        return hash(id(self))

    async def get_raw(self, key: str, allow_null: bool = True) -> Optional[str]:
        value = await self.etcd.get(key)
        if value is None:
            value = shared_config_defaults.get(key, None)
        if not allow_null and value is None:
            raise ServerMisconfiguredError(
                'A required etcd config is missing.', key)
        return value

    async def register_myself(self) -> None:
        instance_id = await get_instance_id()
        manager_info = {
            f'nodes/manager/{instance_id}': 'up',
        }
        await self.etcd.put_dict(manager_info)

    async def deregister_myself(self) -> None:
        instance_id = await get_instance_id()
        await self.etcd.delete_prefix(f'nodes/manager/{instance_id}')

    async def update_aliases_from_file(self, file: Path) -> None:
        log.info('Updating image aliases from "{0}"', file)
        try:
            data = yaml.load(open(file, 'r', encoding='utf-8'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for item in data['aliases']:
            alias = item[0]
            target = item[1]
            await self.etcd.put(f'images/_aliases/{etcd_quote(alias)}', target)
            print(f'{alias} -> {target}')
        log.info('Done.')

    async def _scan_reverse_aliases(self) -> Mapping[str, List[str]]:
        aliases = await self.etcd.get_prefix('images/_aliases')
        result: DefaultDict[str, List[str]] = defaultdict(list)
        for key, value in aliases.items():
            result[value].append(etcd_unquote(key))
        return dict(result)

    async def _parse_image(self, image_ref, item, reverse_aliases):
        installed = (
            await self.context['redis_image'].scard(image_ref.canonical)
        ) > 0
        installed_agents = await self.context['redis_image'].smembers(image_ref.canonical)

        res_limits = []
        for slot_key, slot_range in item['resource'].items():
            min_value = slot_range.get('min')
            if min_value is None:
                min_value = Decimal(0)
            max_value = slot_range.get('max')
            if max_value is None:
                max_value = Decimal('Infinity')
            res_limits.append({
                'key': slot_key,
                'min': min_value,
                'max': max_value,
            })

        accels = item.get('accelerators')
        if accels is None:
            accels = []
        else:
            accels = accels.split(',')

        return {
            'name': image_ref.name,
            'humanized_name': image_ref.name,  # TODO: implement
            'tag': image_ref.tag,
            'registry': image_ref.registry,
            'digest': item[''],
            'labels': item.get('labels', {}),
            'aliases': reverse_aliases.get(image_ref.canonical, []),
            'size_bytes': item.get('size_bytes', 0),
            'resource_limits': res_limits,
            'supported_accelerators': accels,
            'installed': installed,
            'installed_agents': installed_agents,
        }

    async def _check_image(self, reference: str) -> ImageRef:
        known_registries = await get_known_registries(self.etcd)
        ref = ImageRef(reference, known_registries)
        digest = await self.etcd.get(ref.tag_path)
        if digest is None:
            raise UnknownImageReference(reference)
        return ref

    async def inspect_image(self, reference: Union[str, ImageRef]) -> Mapping[str, Any]:
        if isinstance(reference, str):
            ref = await ImageRef.resolve_alias(reference, self.etcd)
        else:
            ref = reference
        reverse_aliases = await self._scan_reverse_aliases()
        image_info = await self.etcd.get_prefix(ref.tag_path)
        if not image_info:
            raise UnknownImageReference(reference)
        return await self._parse_image(ref, image_info, reverse_aliases)

    async def forget_image(self, reference: Union[str, ImageRef]) -> None:
        if isinstance(reference, str):
            ref = await ImageRef.resolve_alias(reference, self.etcd)
        else:
            ref = reference
        await self.etcd.delete_prefix(ref.tag_path)

    async def list_images(self) -> Sequence[Mapping[str, Any]]:
        known_registries = await get_known_registries(self.etcd)
        reverse_aliases = await self._scan_reverse_aliases()
        data = await self.etcd.get_prefix('images')
        coros = []
        for registry, images in data.items():
            if registry == '_aliases':
                continue
            for image, tags in images.items():
                if image == '':
                    continue
                if tags == '1':
                    continue
                for tag, image_info in tags.items():
                    if tag == '':
                        continue
                    raw_ref = f'{etcd_unquote(registry)}/{etcd_unquote(image)}:{tag}'
                    ref = ImageRef(raw_ref, known_registries)
                    coros.append(self._parse_image(ref, image_info, reverse_aliases))
        result = await asyncio.gather(*coros)
        return result

    async def set_image_resource_limit(self, reference: str, slot_type: str,
                                       value_range: Tuple[Optional[Decimal], Optional[Decimal]]):
        ref = await self._check_image(reference)
        if value_range[0] is not None:
            await self.etcd.put(f'{ref.tag_path}/resource/{slot_type}/min', str(value_range[0]))
        if value_range[1] is not None:
            await self.etcd.put(f'{ref.tag_path}/resource/{slot_type}/max', str(value_range[1]))

    async def rescan_images(
        self,
        registry: str = None,
        *,
        reporter: ProgressReporter = None,
    ) -> None:
        registry_config_iv = t.Mapping(t.String, container_registry_iv)
        latest_registry_config = registry_config_iv.check(
            await self.etcd.get_prefix('config/docker/registry')
        )
        self['docker']['registry'] = latest_registry_config
        # TODO: delete images from registries removed from the previous config?
        if registry is None:
            # scan all configured registries
            registries = self['docker']['registry']
        else:
            try:
                registries = {registry: self['docker']['registry'][registry]}
            except KeyError:
                raise RuntimeError("It is an unknown registry.", registry)
        async with aiotools.TaskGroup() as tg:
            for registry_name, registry_info in registries.items():
                log.info('Scanning kernel images from the registry "{0}"', registry_name)
                scanner_cls = get_container_registry(registry_info)
                scanner = scanner_cls(self.etcd, registry_name, registry_info)
                tg.create_task(scanner.rescan_single_registry(reporter))
        # TODO: delete images removed from registry?

    async def alias(self, alias: str, target: str) -> None:
        await self.etcd.put(f'images/_aliases/{etcd_quote(alias)}', target)

    async def dealias(self, alias: str) -> None:
        await self.etcd.delete(f'images/_aliases/{etcd_quote(alias)}')

    async def update_resource_slots(
        self,
        slot_key_and_units: Mapping[SlotName, SlotTypes],
    ) -> None:
        updates = {}
        known_slots = await self.get_resource_slots()
        for k, v in slot_key_and_units.items():
            if k not in known_slots or v != known_slots[k]:
                updates[f'config/resource_slots/{k}'] = v.value
        if updates:
            await self.etcd.put_dict(updates)

    async def update_manager_status(self, status) -> None:
        await self.etcd.put('manager/status', status.value)
        self.get_manager_status.cache_clear()

    @aiotools.lru_cache(maxsize=1, expire_after=2.0)
    async def _get_resource_slots(self):
        raw_data = await self.etcd.get_prefix_dict('config/resource_slots')
        return {
            SlotName(k): SlotTypes(v) for k, v in raw_data.items()
        }

    async def get_resource_slots(self) -> Mapping[SlotName, SlotTypes]:
        """
        Returns the system-wide known resource slots and their units.
        """
        try:
            ret = current_resource_slots.get()
        except LookupError:
            configured_slots = await self._get_resource_slots()
            ret = {**INTRINSIC_SLOTS, **configured_slots}
            current_resource_slots.set(ret)
        return ret

    @aiotools.lru_cache(maxsize=1, expire_after=2.0)
    async def _get_vfolder_types(self):
        return await self.etcd.get_prefix('volumes/_types')

    async def get_vfolder_types(self) -> Sequence[str]:
        """
        Returns the vfolder types currently set. One of "user" and/or "group".
        If none is specified, "user" type is implicitly assumed.
        """
        try:
            ret = current_vfolder_types.get()
        except LookupError:
            vf_types = await self._get_vfolder_types()
            if not vf_types:
                vf_types = {'user': ''}
            ret = list(vf_types.keys())
            current_vfolder_types.set(ret)
        return ret

    @aiotools.lru_cache(maxsize=1, expire_after=5.0)
    async def get_manager_nodes_info(self):
        return await self.etcd.get_prefix_dict('nodes/manager')

    @aiotools.lru_cache(maxsize=1, expire_after=2.0)
    async def get_manager_status(self):
        status = await self.etcd.get('manager/status')
        if status is None:
            return None
        return ManagerStatus(status)

    async def watch_manager_status(self):
        async with aiotools.aclosing(self.etcd.watch('manager/status')) as agen:
            async for ev in agen:
                yield ev

    # TODO: refactor using contextvars in Python 3.7 so that the result is cached
    #       in a per-request basis.
    @aiotools.lru_cache(maxsize=1, expire_after=2.0)
    async def get_allowed_origins(self):
        return await self.get('config/api/allow-origins')

    # TODO: refactor using contextvars in Python 3.7 so that the result is cached
    #       in a per-request basis.
    @aiotools.lru_cache(expire_after=60.0)
    async def get_image_slot_ranges(self, image_ref: ImageRef):
        """
        Returns the minimum and maximum ResourceSlot values.
        All slot values are converted and normalized to Decimal.
        """
        data = await self.etcd.get_prefix_dict(image_ref.tag_path)
        slot_units = await self.get_resource_slots()
        min_slot = ResourceSlot()
        max_slot = ResourceSlot()

        for slot_key, slot_range in data['resource'].items():
            slot_unit = slot_units.get(slot_key)
            if slot_unit is None:
                # ignore unknown slots
                continue
            min_value = slot_range.get('min')
            if min_value is None:
                min_value = Decimal(0)
            max_value = slot_range.get('max')
            if max_value is None:
                max_value = Decimal('Infinity')
            if slot_unit == 'bytes':
                if not isinstance(min_value, Decimal):
                    min_value = BinarySize.from_str(min_value)
                if not isinstance(max_value, Decimal):
                    max_value = BinarySize.from_str(max_value)
            else:
                if not isinstance(min_value, Decimal):
                    min_value = Decimal(min_value)
                if not isinstance(max_value, Decimal):
                    max_value = Decimal(max_value)
            min_slot[slot_key] = min_value
            max_slot[slot_key] = max_value

        # fill missing
        for slot_key in slot_units.keys():
            if slot_key not in min_slot:
                min_slot[slot_key] = Decimal(0)
            if slot_key not in max_slot:
                max_slot[slot_key] = Decimal('Infinity')

        return min_slot, max_slot

    def get_redis_url(self, db: int = 0) -> yarl.URL:
        """
        Returns a complete URL composed from the given Redis config.
        """
        url = (yarl.URL('redis://host')
               .with_host(str(self.data['redis']['addr'][0]))
               .with_port(self.data['redis']['addr'][1])
               .with_password(self.data['redis']['password'])
               / str(db))
        return url
