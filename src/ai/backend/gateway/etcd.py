'''
Configuration Schema on etcd:

Note that {image} does not contain the common "<registry>/kernel-" prefix
while the real images on the registry have that.

A registry name contains the host, port (only for non-standards), and the path.
So, they must be URL-quoted (including slashes) to avoid parsing
errors due to intermediate slashes and colons.
Alias keys are also URL-quoted in the same way.

{namespace}
 + config
   + api
     - allow-origins: "*"
   + docker
     + registry
       - lablup: https://registry-1.docker.io
         - username: "lablup"
       + {registry-name}: {registry-URL}  # {registry-name} is url-quoted
         - username: {username}
         - password: {password}
         - auth: {auth-json-cached-from-config.json}
       ...
   + resource_slots
     + {"cuda.device"}: {"count"}
     + {"cuda.mem"}: {"bytes"}
     + {"cuda.smp"}: {"count"}
     ...
 + nodes
   + manager: {instance-id}
     - event_addr: {"tcp://manager:5001"}
     - status: {one-of-ManagerStatus-value}
   - redis: {"tcp://redis:6379"}
   + agents
     - {instance-id}: {"starting","running"}
 + volumes
   - _mount: {path-to-mount-root-for-vfolder-partitions}
   - _default_host: {default-vfolder-partition-name}
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
'''

import asyncio
from collections import defaultdict
from decimal import Decimal
import logging
import json
from pathlib import Path
import re
from typing import Union

import aiohttp
from aiohttp import web
import aiojobs
import aiotools
import yaml
import yarl

from ai.backend.common.identity import get_instance_id, get_instance_ip
from ai.backend.common.docker import get_known_registries
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ImageRef, BinarySize, ResourceSlot
from ai.backend.common.exception import UnknownImageReference
from ai.backend.common.etcd import (
    make_dict_from_pairs,
    quote as etcd_quote,
    unquote as etcd_unquote,
)
from ai.backend.common.docker import (
    login as registry_login,
    get_registry_info
)
from .exceptions import ServerMisconfiguredError
from .manager import ManagerStatus
from .utils import chunked

log = BraceStyleAdapter(logging.getLogger('ai.backend.gateway.etcd'))

_default_cpu_max = 1
_default_mem_max = '1g'


class ConfigServer:

    def __init__(self, etcd_addr, namespace):
        # WARNING: importing etcd3/grpc must be done after forks.
        from ai.backend.common.etcd import AsyncEtcd
        self.etcd = AsyncEtcd(etcd_addr, namespace)

    async def register_myself(self, app_config):
        instance_id = await get_instance_id()
        if app_config.advertised_manager_host:
            instance_ip = app_config.advertised_manager_host
            log.info('manually set advertised manager host: {0}', instance_ip)
        else:
            # fall back 1: read private IP from cloud instance metadata
            # fall back 2: read hostname and resolve it
            # fall back 3: "127.0.0.1"
            instance_ip = await get_instance_ip()
        event_addr = f'{instance_ip}:{app_config.events_port}'
        await self.etcd.put_multi(
            ['nodes/manager', 'nodes/redis', 'nodes/manager/event_addr'],
            [instance_id, app_config.redis_addr, event_addr])

    async def deregister_myself(self):
        await self.etcd.delete_prefix('nodes/manager')

    async def update_aliases_from_file(self, file: Path):
        log.info('Updating image aliases from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for item in data['aliases']:
            alias = item[0]
            target = item[1]
            await self.etcd.put(f'images/_aliases/{etcd_quote(alias)}', target)
            print(f'{alias} -> {target}')
        log.info('Done.')

    async def _scan_reverse_aliases(self):
        kvpairs = dict(await self.etcd.get_prefix('images/_aliases'))
        result = defaultdict(list)
        for key, value in kvpairs.items():
            kpath = key.split('/')
            result[value].append(etcd_unquote(kpath[2]))
        return result

    def _parse_image(self, image_ref, kvpairs, reverse_aliases):
        tag_path = image_ref.tag_path
        item = make_dict_from_pairs(tag_path, kvpairs)

        res_limits = []
        for slot_key, slot_range in item['resource'].items():
            min_value = slot_range.get('min')
            if min_value is None:
                raise ServerMisconfiguredError(
                    f'{image_ref} is not configured to use "{slot_key}" resource.')
            max_value = slot_range.get('max')
            if max_value is None:
                if slot_key == 'cpu':
                    max_value = max(Decimal(min_value),
                                    Decimal(_default_cpu_max))
                elif slot_key == 'mem':
                    max_value = '{:g}'.format(
                        max(BinarySize.from_str(min_value),
                            BinarySize.from_str(_default_mem_max)))
                else:
                    # disallowed!
                    max_value = '0'
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
            'aliases': reverse_aliases.get(
                f'{image_ref.name}:{image_ref.tag}', []),
            'size_bytes': item.get('size_bytes', 0),
            'resource_limits': res_limits,
            'supported_accelerators': accels,
        }

    async def _check_image(self, reference: str) -> ImageRef:
        known_registries = await get_known_registries(self.etcd)
        ref = ImageRef(reference, known_registries)
        digest = await self.etcd.get(ref.tag_path)
        if digest is None:
            raise UnknownImageReference(reference)
        return ref

    async def inspect_image(self, reference: Union[str, ImageRef]):
        if isinstance(reference, str):
            known_registries = await get_known_registries(self.etcd)
            ref = ImageRef(reference, known_registries)
        else:
            ref = reference
        reverse_aliases = await self._scan_reverse_aliases()
        kvpairs = dict(await self.etcd.get_prefix(ref.tag_path))
        if not kvpairs or not kvpairs.get(ref.tag_path):
            raise UnknownImageReference(reference)
        return self._parse_image(ref, kvpairs, reverse_aliases)

    async def list_images(self):
        items = []
        known_registries = await get_known_registries(self.etcd)
        reverse_aliases = await self._scan_reverse_aliases()
        kvpairs = dict(await self.etcd.get_prefix('images'))
        rx_tag_digest_key = re.compile(
            r'^images/(?P<registry>(?!_aliases)[^/]+)/'
            r'(?P<image>[^/]+)/(?P<tag>[^/]+)$')
        image_set = {}
        for key, value in kvpairs.items():
            match = rx_tag_digest_key.search(key)
            if match is None:
                continue
            image_tuple = (
                etcd_unquote(match.group('registry')),
                etcd_unquote(match.group('image')),
                match.group('tag'),
            )
            image_set[image_tuple] = key

        for (registry, image, tag), tag_path in image_set.items():
            ref = ImageRef(f'{registry}/{image}:{tag}', known_registries)
            items.append(self._parse_image(ref, kvpairs, reverse_aliases))
        return items

    async def set_image_resource_limit(self, reference: str,
                                       slot_type: str,
                                       max_value: str):
        # TODO: add some validation
        ref = await self._check_image(reference)
        await self.etcd.put(f'{ref.tag_path}/resource/{slot_type}/max',
                            max_value)

    async def _rescan_images(self, registry_name: str,
                             registry_url: yarl.URL,
                             credentials: dict):
        all_updates = {}
        base_hdrs = {
            'Accept': 'application/vnd.docker.distribution.manifest.v2+json',
        }

        async def _scan_image(sess, image):
            rqst_args = await registry_login(
                sess, registry_url,
                credentials, f'repository:{image}:pull')
            tags = []
            rqst_args['headers'].update(**base_hdrs)
            async with sess.get(registry_url / f'v2/{image}/tags/list',
                                **rqst_args) as resp:
                data = json.loads(await resp.read())
                if 'tags' in data:
                    # sometimes there are dangling image names in the hub.
                    tags.extend(data['tags'])
            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_tag(sess, rqst_args, image, tag))
                    for tag in tags])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

        async def _scan_tag(sess, rqst_args, image, tag):
            config_digest = None
            labels = {}
            async with sess.get(registry_url / f'v2/{image}/manifests/{tag}',
                                **rqst_args) as resp:
                resp.raise_for_status()
                data = await resp.json()
                config_digest = data['config']['digest']
                size_bytes = (sum(layer['size'] for layer in data['layers']) +
                              data['config']['size'])
            async with sess.get(registry_url / f'v2/{image}/blobs/{config_digest}',
                                **rqst_args) as resp:
                # content-type may not be json...
                resp.raise_for_status()
                data = json.loads(await resp.read())
                raw_labels = data['container_config']['Labels']
                if raw_labels:
                    labels.update(raw_labels)

            if not labels.get('ai.backend.kernelspec'):
                # Skip non-Backend.AI kernel images
                return

            log.info('Updating metadata for {0}:{1}', image, tag)
            updates = {}
            updates[f'images/{etcd_quote(registry_name)}/'
                    f'{etcd_quote(image)}'] = '1'
            tag_prefix = f'images/{etcd_quote(registry_name)}/' \
                         f'{etcd_quote(image)}/{tag}'
            updates[tag_prefix] = config_digest
            updates[f'{tag_prefix}/size_bytes'] = size_bytes
            for k, v in labels.items():
                updates[f'{tag_prefix}/labels/{k}'] = v

            accels = labels.get('ai.backend.accelerators')
            if accels:
                updates[f'{tag_prefix}/accels'] = accels

            res_prefix = 'ai.backend.resource.min.'
            for k, v in filter(lambda pair: pair[0].startswith(res_prefix),
                               labels.items()):
                res_key = k[len(res_prefix):]
                updates[f'{tag_prefix}/resource/{res_key}/min'] = v
            all_updates.update(updates)

        async with aiohttp.ClientSession() as sess:
            images = []
            if registry_url.host.endswith('.docker.io'):
                # We need some special treatment for the Docker Hub.
                params = {'page_size': '100'}
                username = await self.etcd.get(
                    f'config/docker/registry/{etcd_quote(registry_name)}/username')
                hub_url = yarl.URL('https://hub.docker.com')
                async with sess.get(hub_url / f'v2/repositories/{username}/',
                                    params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        images.extend(f"{username}/{item['name']}"
                                      for item in data['results']
                                      # a little optimization to ignore legacies
                                      if not item['name'].startswith('kernel-'))
                    else:
                        log.error('Failed to fetch repository list from {0} '
                                  '(status={1})',
                                  hub_url, resp.status)
            else:
                # In other cases, try the catalog search.
                rqst_args = await registry_login(
                    sess, registry_url,
                    credentials, 'registry:catalog:*')
                async with sess.get(registry_url / 'v2/_catalog',
                                    **rqst_args) as resp:
                    if resp.status == 200:
                        data = json.loads(await resp.read())
                        images.extend(data['repositories'])
                    else:
                        log.warning('Docker registry {0} does not allow/support '
                                    'catalog search. (status={1})',
                                    registry_url, resp.status)

            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_image(sess, image)) for image in images])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

        if not all_updates:
            log.info('No images found in registry {0}', registry_url)
            return
        for kvlist in chunked(sorted(all_updates.items()), 16):
            await self.etcd.put_dict(dict(kvlist))

    async def rescan_images(self, registry: str = None):
        if registry is None:
            registries = []
            pairs = await self.etcd.get_prefix('config/docker/registry')
            for key, val in pairs:
                match = re.search(r'^config/docker/registry/([^/]+)$', key)
                if match is not None:
                    registries.append(etcd_unquote(match.group(1)))
        else:
            registries = [registry]
        coros = []
        for registry in registries:
            log.info('Scanning kernel images from the registry "{0}"', registry)
            try:
                registry_url, creds = await get_registry_info(self.etcd, registry)
            except ValueError:
                log.error('Unknown registry: "{0}"', registry)
                continue
            coros.append(self._rescan_images(registry, registry_url, creds))
        await asyncio.gather(*coros)

    async def alias(self, alias: str, target: str):
        await self.etcd.put(f'images/_aliases/{etcd_quote(alias)}', target)

    async def dealias(self, alias: str):
        await self.etcd.delete(f'images/_aliases/{etcd_quote(alias)}')

    async def update_volumes_from_file(self, file: Path):
        log.info('Updating network volumes from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
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

    async def update_resource_slots(self, slot_key_and_units):
        updates = {}
        for k, v in slot_key_and_units.items():
            if k in ('cpu', 'mem'):
                continue
            # currently we support only two units
            # (where count may be fractional)
            assert v in ('bytes', 'count')
            updates[f'config/resource_slots/{k}'] = v
        await self.etcd.put_dict(updates)

    async def update_manager_status(self, status):
        await self.etcd.put('manager/status', status.value)

    @aiotools.lru_cache(maxsize=1)
    async def get_resource_slots(self):
        '''
        Returns the system-wide known resource slots and their units.
        '''
        intrinsic_slots = {'cpu': 'count', 'mem': 'bytes'}
        configured_slots = await self.etcd.get_prefix_dict('config/resource_slots')
        return {**intrinsic_slots, **configured_slots}

    @aiotools.lru_cache(maxsize=1)
    async def get_manager_status(self):
        status = await self.etcd.get('manager/status')
        return ManagerStatus(status)

    async def watch_manager_status(self):
        async for ev in self.etcd.watch('manager/status'):
            yield ev

    @aiotools.lru_cache(maxsize=1, expire_after=60.0)
    async def get_allowed_origins(self):
        origins = await self.etcd.get('config/api/allow-origins')
        if origins is None:
            origins = '*'
        return origins

    @aiotools.lru_cache(expire_after=60.0)
    async def get_image_slot_ranges(self, image_ref: ImageRef):
        '''
        Returns the minimum and maximum ResourceSlot values.
        All slot values are converted and normalized to Decimal.
        '''
        data = await self.etcd.get_prefix_dict(image_ref.tag_path)
        slot_units = await self.get_resource_slots()
        min_slot = ResourceSlot(numeric=True)
        max_slot = ResourceSlot(numeric=True)

        for slot_key, slot_range in data['resource'].items():
            slot_unit = slot_units.get(slot_key)
            if slot_unit is None:
                raise RuntimeError('The requested image requires resource slots '
                                   'that are not known to the manager.')
            min_value = slot_range['min']
            max_value = slot_range.get('max')
            if max_value is None:
                if slot_key == 'cpu':
                    max_value = max(Decimal(min_value),
                                    Decimal(_default_cpu_max))
                elif slot_key == 'mem':
                    max_value = '{:g}'.format(
                        max(BinarySize.from_str(min_value),
                            BinarySize.from_str(_default_mem_max)))
                else:
                    # disallowed!
                    max_value = '0'
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

        return min_slot, max_slot


async def init(app):
    if app['pidx'] == 0:
        await app['config_server'].register_myself(app['config'])


async def shutdown(app):
    if app['pidx'] == 0:
        await app['config_server'].deregister_myself()


def create_app(default_cors_options):
    app = web.Application()
    app['api_versions'] = (3, 4)
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    return app, []
