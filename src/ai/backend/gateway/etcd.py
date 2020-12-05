from __future__ import annotations

'''
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
           - project: "project-name"  # harbor only
         ...
     + redis
       - addr: "{redis-host}:{redis-port}"
       - password: {password}
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
     - _mount: {path-to-mount-root-for-vfolder-partitions}
     - _default_host: {default-vfolder-partition-name}
     - _fsprefix: {path-prefix-inside-host-mounts}
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
'''

import asyncio
from contextvars import ContextVar
from collections import defaultdict
from decimal import Decimal
import logging
from pathlib import Path
from typing import (
    Any, Optional, Union,
    AsyncGenerator,
    Iterable,
    Mapping, DefaultDict,
    Sequence, List, Tuple,
)

from aiohttp import web
import aiohttp_cors
from aiojobs.aiohttp import atomic
import aiotools
import trafaret as t
import yaml

from ai.backend.common.identity import get_instance_id
from ai.backend.common.docker import (
    ImageRef, get_known_registries,
)
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
from .auth import superadmin_required
from .config import container_registry_iv
from ..manager.background import ProgressReporter
from ..manager.container_registry import get_container_registry
from ..manager.defs import INTRINSIC_SLOTS
from .exceptions import InvalidAPIParameters, ServerMisconfiguredError
from .manager import ManagerStatus
from .types import CORSOptions, WebMiddleware
from .utils import check_api_params

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.etcd'))

config_defaults = {
    'volumes/_mount': '/mnt',
    'volumes/_default_host': 'local',
    'volumes/_fsprefix': '/',
    'config/api/allow-origins': '*',
    'config/docker/image/auto_pull': 'digest',
}

current_vfolder_types: ContextVar[List[str]] = ContextVar('current_vfolder_types')


class ConfigServer:

    def __init__(self, app_ctx: Mapping[str, Any],
                 etcd_addr: HostPortPair,
                 etcd_user: Optional[str],
                 etcd_password: Optional[str],
                 namespace: str) -> None:
        # WARNING: importing etcd3/grpc must be done after forks.
        from ai.backend.common.etcd import AsyncEtcd
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

    async def get(self, key: str, allow_null: bool = True) -> Optional[str]:
        value = await self.etcd.get(key)
        if value is None:
            value = config_defaults.get(key, None)
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
        registry_config = await self.etcd.get_prefix('config/docker/registry')
        if registry is None:
            # scan all configured registries
            registries = {k: container_registry_iv.check(v) for k, v in registry_config.items()}
        else:
            try:
                registries = {registry: container_registry_iv.check(registry_config[registry])}
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

    async def update_volumes_from_file(self, file: Path) -> None:
        log.info('Updating network volumes from "{0}"', file)
        try:
            data = yaml.load(open(file, 'r', encoding='utf-8'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for item in data['volumes']:
            name = item['name']
            updates = {
                f'volumes/{name}/mount/{k}': v
                for k, v in item['mount'].items()
            }
            await self.etcd.put_dict(updates)
        log.info('done')

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
        '''
        Returns the system-wide known resource slots and their units.
        '''
        try:
            ret = current_resource_slots.get()
        except LookupError:
            configured_slots = await self._get_resource_slots()
            ret = {**INTRINSIC_SLOTS, **configured_slots}
            current_resource_slots.set(ret)
        return ret

    @aiotools.lru_cache(maxsize=1, expire_after=2.0)
    async def _get_vfolder_types(self):
        return await self.etcd.get_prefix_dict('volumes/_types')

    async def get_vfolder_types(self) -> Sequence[str]:
        '''
        Returns the vfolder types currently set. One of "user" and/or "group".
        If none is specified, "user" type is implicitly assumed.
        '''
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
        '''
        Returns the minimum and maximum ResourceSlot values.
        All slot values are converted and normalized to Decimal.
        '''
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


@atomic
async def get_resource_slots(request) -> web.Response:
    log.info('ETCD.GET_RESOURCE_SLOTS ()')
    known_slots = await request.app['config_server'].get_resource_slots()
    return web.json_response(known_slots, status=200)


@atomic
async def get_vfolder_types(request) -> web.Response:
    log.info('ETCD.GET_VFOLDER_TYPES ()')
    vfolder_types = await request.app['config_server'].get_vfolder_types()
    return web.json_response(vfolder_types, status=200)


@atomic
@superadmin_required
async def get_docker_registries(request) -> web.Response:
    '''
    Returns the list of all registered docker registries.
    '''
    log.info('ETCD.GET_DOCKER_REGISTRIES ()')
    etcd = request.app['registry'].config_server.etcd
    _registries = await get_known_registries(etcd)
    # ``yarl.URL`` is not JSON-serializable, so we need to represent it as string.
    known_registries: Mapping[str, str] = {k: v.human_repr() for k, v in _registries.items()}
    return web.json_response(known_registries, status=200)


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('prefix', default=False): t.Bool,
    }))
async def get_config(request: web.Request, params: Any) -> web.Response:
    etcd = request.app['config_server'].etcd
    log.info('ETCD.GET_CONFIG (ak:{}, key:{}, prefix:{})',
             request['keypair']['access_key'], params['key'], params['prefix'])
    if params['prefix']:
        # Flatten the returned ChainMap object for JSON serialization
        value = dict(await etcd.get_prefix_dict(params['key']))
    else:
        value = await etcd.get(params['key'])
    return web.json_response({'result': value})


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('value'): (t.String(allow_blank=True) |
                         t.Mapping(t.String(allow_blank=True), t.Any)),
    }))
async def set_config(request: web.Request, params: Any) -> web.Response:
    etcd = request.app['config_server'].etcd
    log.info('ETCD.SET_CONFIG (ak:{}, key:{}, val:{})',
             request['keypair']['access_key'], params['key'], params['value'])
    if isinstance(params['value'], Mapping):
        updates = {}

        def flatten(prefix, o):
            for k, v in o.items():
                inner_prefix = prefix if k == '' else f'{prefix}/{k}'
                if isinstance(v, Mapping):
                    flatten(inner_prefix, v)
                else:
                    updates[inner_prefix] = v

        flatten(params['key'], params['value'])
        # TODO: chunk support if there are too many keys
        if len(updates) > 16:
            raise InvalidAPIParameters(
                'Too large update! Split into smaller key-value pair sets.')
        await etcd.put_dict(updates)
    else:
        await etcd.put(params['key'], params['value'])
    return web.json_response({'result': 'ok'})


@atomic
@superadmin_required
@check_api_params(
    t.Dict({
        t.Key('key'): t.String,
        t.Key('prefix', default=False): t.Bool,
    }))
async def delete_config(request: web.Request, params: Any) -> web.Response:
    etcd = request.app['config_server'].etcd
    log.info('ETCD.DELETE_CONFIG (ak:{}, key:{}, prefix:{})',
             request['keypair']['access_key'], params['key'], params['prefix'])
    if params['prefix']:
        await etcd.delete_prefix(params['key'])
    else:
        await etcd.delete(params['key'])
    return web.json_response({'result': 'ok'})


async def app_ctx(app: web.Application) -> AsyncGenerator[None, None]:
    if app['pidx'] == 0:
        await app['config_server'].register_myself()
    yield
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()


def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app.cleanup_ctx.append(app_ctx)
    app['prefix'] = 'config'
    app['api_versions'] = (3, 4)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('GET',  r'/resource-slots', get_resource_slots))
    cors.add(app.router.add_route('GET',  r'/vfolder-types', get_vfolder_types))
    cors.add(app.router.add_route('GET',  r'/docker-registries', get_docker_registries))
    cors.add(app.router.add_route('POST', r'/get', get_config))
    cors.add(app.router.add_route('POST', r'/set', set_config))
    cors.add(app.router.add_route('POST', r'/delete', delete_config))
    return app, []
