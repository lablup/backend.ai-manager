'''
Configuration Schema on etcd:

Note that {image} does not contain the common "<registry>/kernel-" prefix
while the real images on the registry have that.

{namespace}
 + config
   + docker
     + registry
       - lablup: https://registry-1.docker.io
       + {registry-name}: {registry-URL}
         - user: {username}
         - password: {password}
         - auth: {auth-json-cached-from-config.json}
       ...
 + ndoes
   + manager: {instance-id}
     - event_addr: {tcp://manager:5001}
   - redis: {tcp://redis:6379}
   + agents
     + {instance-id}
       - status: {one-of-ManagerStatus-value}
 + volumes
   - _mount: {path-to-mount-root-for-vfolder-partitions}
   - _default_host: {default-vfolder-partition-name}
 + images
   + _aliases
     - {alias}: {image:tag}
     - {alias}: {image:tag}
     - {alias}: {image:tag}
     ...
   + {image}
     + tags
       + {tag}: {digest-for-config-layer}
         - size_bytes: {image-size-in-bytes}
         - registry: {registry-name}
         - accelerators: "{accel-name-1},{accel-name-2},..."
         + labels
           - {key}: {value}
           ...
         + resource
           + cpu
             - min
             - max
           + mem
             - min
             - max
           + {cuda.smp}
             - min
             - max
           + {cuda.mem}
             - min
             - max
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
from typing import Tuple

import aiohttp
from aiohttp import web
import aiojobs
import aiotools
import yaml
import yarl

from ai.backend.common.identity import get_instance_id, get_instance_ip
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import ImageRef, BinarySize

from ..manager.models.agent import ResourceSlot
from .manager import ManagerStatus

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

    async def update_kernel_images_from_file(self, file: Path):
        log.info('Loading kernel image data from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for image in data['images']:
            name = image['name']
            print(f"Updating {name}")

            inserted_aliases = []
            for tag, hash in image['tags']:
                assert hash
                if hash.startswith(':'):  # tag-level alias
                    inserted_aliases.append(
                        (f'images/_aliases/{name}:{tag}', f'{name}:{hash[1:]}')
                    )
            if inserted_aliases:
                await self.etcd.put_multi(*zip(*inserted_aliases))

            cpu_share = image['slots']['cpu']
            cpu_share = 'null' if cpu_share is None else f'{cpu_share:.2f}'
            mem_share = image['slots']['mem']
            mem_share = 'null' if mem_share is None else f'{mem_share:.2f}'
            gpu_share = image['slots'].get('gpu', None)
            gpu_share = 'null' if gpu_share is None else f'{gpu_share:.2f}'
            tpu_share = image['slots'].get('tpu', None)
            tpu_share = 'null' if tpu_share is None else f'{tpu_share:.2f}'
            await self.etcd.put_multi(
                [f'images/{name}',
                 f'images/{name}/cpu',
                 f'images/{name}/mem',
                 f'images/{name}/gpu',
                 f'images/{name}/tpu'],
                ['1', cpu_share, mem_share, gpu_share, tpu_share])

            inserted_tags = [(f'images/{name}/tags/{tag}', hash)
                             for tag, hash in image['tags']]
            await self.etcd.put_multi(*zip(*inserted_tags))
        log.info('Done.')

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
            await self.etcd.put(f'images/_aliases/{alias}', target)
            print(f'{alias} -> {target}')
        log.info('Done.')

    # @aiotools.lru_cache(maxsize=8)
    async def get_docker_registry(self, registry: str) -> Tuple[yarl.URL, dict]:
        registry_addr = await self.etcd.get(f'config/docker/registry/{registry}')
        if registry_addr is None:
            raise RuntimeError(f'Unknown registry: {registry}')
        auth = await self.etcd.get(f'config/docker/registry/{registry}/auth')
        auth = json.loads(auth) if auth is not None else {}
        return yarl.URL(registry_addr), auth

    async def list_images(self):
        items = []
        images = []
        reverse_aliases = defaultdict(list)
        kvdict = dict(await self.etcd.get_prefix('images'))
        for key, value in kvdict.items():
            kpath = key.split('/')
            if len(kpath) == 3 and kpath[1] == '_aliases':
                reverse_aliases[value].append(kpath[2])
                continue
            if len(kpath) == 2 and value == '1':
                images.append(kpath[1])
        for image in images:
            tag_paths = filter(lambda k: k.startswith(f'images/{image}/tags/'),
                               kvdict.keys())
            tag_path_set = set()
            for tag_path in tag_paths:
                tag_path = '/'.join(tag_path.split('/', maxsplit=4)[:4])
                tag_path_set.add(tag_path)

            for tag_path in tag_path_set:
                tag = tag_path.rsplit('/', maxsplit=1)[-1]
                hash_ = kvdict.get(tag_path, '')

                res_paths = filter(lambda k: k.startswith(f'{tag_path}/resource/'),
                                   kvdict.keys())
                res_types = []
                res_values = {}
                for res_path in res_paths:
                    res_type, limit_type = res_path.rsplit('/', maxsplit=2)[-2:]
                    value = kvdict[res_path]
                    res_types.append(res_type)
                    res_values[(res_type, limit_type)] = value

                res_limits = []
                for res_type in res_types:
                    min_value = res_values[(res_type, 'min')]
                    max_value = res_values.get((res_type, 'max'), None)
                    if max_value is None:
                        if res_type == 'cpu':
                            max_value = max(Decimal(min_value),
                                            Decimal(_default_cpu_max))
                        elif res_type == 'mem':
                            max_value = '{:g}'.format(
                                max(BinarySize.from_str(min_value),
                                    BinarySize.from_str(_default_mem_max)))
                        else:
                            # disallowed!
                            max_value = '0'

                    res_limits.append({
                        'key': res_type,
                        'min': min_value,
                        'max': max_value,
                    })

                accels = kvdict.get(f'{tag_path}/accelerators')
                if accels is None:
                    accels = []
                else:
                    accels = accels.split(',')

                labels = {}
                label_paths = filter(lambda k: k.startswith(f'{tag_path}/labels/'),
                                     kvdict.keys())
                for label_path in label_paths:
                    label_key = label_path.rsplit('/', maxsplit=1)[-1]
                    value = kvdict[label_path]
                    labels[label_key] = value

                item = {
                    'name': image,
                    'humanized_name': image,  # TODO: implement
                    'tag': tag,
                    'hash': hash_,
                    'labels': labels,
                    'aliases': reverse_aliases.get(f'{image}:{tag}', []),
                    'size_bytes': kvdict.get(f'{tag}/size_bytes', 0),
                    'resource_limits': res_limits,
                    'supported_accelerators': accels,
                }
                items.append(item)
        return items

    async def _rescan_images(self, registry_url: yarl.URL, auth: dict):
        base_hdrs = {
            'Accept': 'application/vnd.docker.distribution.manifest.v2+json',
        }

        async def _scan_image(sess, image):
            hdrs = {**base_hdrs}
            if registry_url.host == 'registry-1.docker.io':
                params = {
                    'scope': f'repository:{image}:pull',
                    'service': 'registry.docker.io',
                }
                async with sess.get('https://auth.docker.io/token',
                                    params=params) as resp:
                    data = await resp.json()
                    hdrs['Authorization'] = f"Bearer {data['token']}"
            else:
                if 'auth' in auth:
                    hdrs['Authorization'] = f"Bearer {auth['auth']}"
            tags = []
            async with sess.get(registry_url / f'v2/{image}/tags/list',
                                headers=hdrs) as resp:
                data = await resp.json()
                if 'tags' in data:
                    # sometimes there are dangling image names in the hub.
                    tags.extend(data['tags'])
            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_tag(sess, hdrs, image, tag))
                    for tag in tags])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

        async def _scan_tag(sess, hdrs, image, tag):
            config_digest = None
            labels = {}
            async with sess.get(registry_url / f'v2/{image}/manifests/{tag}',
                                headers=hdrs) as resp:
                data = await resp.json()
                config_digest = data['config']['digest']
                size_bytes = (sum(layer['size'] for layer in data['layers']) +
                              data['config']['size'])
            async with sess.get(registry_url / f'v2/{image}/blobs/{config_digest}',
                                headers=hdrs) as resp:
                # content-type may not be json...
                data = json.loads(await resp.read())
                raw_labels = data['container_config']['Labels']
                if raw_labels:
                    labels.update(raw_labels)

            if not labels.get('ai.backend.kernelspec'):
                # Skip non-Backend.AI kernel images
                return

            updates = {}
            img_ref = ImageRef(image + ':' + tag)
            updates[f'images/{img_ref.name}'] = '1'
            tag_prefix = f'images/{img_ref.name}/tags/{img_ref.tag}'
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

            ks = []
            vs = []
            for k, v in updates.items():
                ks.append(k)
                vs.append(v)
            await self.etcd.put_multi(ks, vs)

        async with aiohttp.ClientSession() as sess:
            # image = 'nvidia/digits'
            # tag = '18.12-tensorflow'
            images = []
            if registry_url.host == 'registry-1.docker.io':
                # We need some special treatment for the Docker Hub.
                params = {
                    'page_size': '100',
                }
                async with sess.get('https://hub.docker.com/v2/repositories/lablup/',
                                    params=params) as resp:
                    data = await resp.json()
                    images.extend(f"lablup/{item['name']}"
                                  for item in data['results'])
            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_image(sess, image)) for image in images])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

    async def rescan_images(self, registry: str = None):
        if registry is None:
            registries = []
            pairs = await self.etcd.get_prefix('config/docker/registry')
            for key, val in pairs:
                match = re.search(r'^config/docker/registry/([^/]+)$', key)
                if match is not None:
                    registries.append(match.group(1))
        else:
            registries = [registry]
        coros = []
        for registry in registries:
            log.info('Scanning kernel images from the registry "{0}"', registry)
            registry_url, auth = await self.get_docker_registry(registry)
            coros.append(self._rescan_images(registry_url, auth))
        await asyncio.gather(*coros)

    async def alias(self, alias: str, target: str):
        image, tag = target.rsplit(':', maxsplit=1)
        ref = await self.etcd.get(f'images/{image}/tags/{tag}')
        if ref is None:
            raise ValueError('target must be a valid iamge.')
        await self.etcd.put(f'images/_aliases/{alias}', target)

    async def dealias(self, alias: str):
        await self.etcd.delete(f'images/_aliases/{alias}')

    async def update_volumes_from_file(self, file: Path):
        log.info('Updating network volumes from "{0}"', file)
        try:
            data = yaml.load(open(file, 'rb'))
        except IOError:
            log.error('Cannot open "{0}".', file)
            return
        for item in data['volumes']:
            name = item['name']
            ks = []
            vs = []
            for k, v in item['mount'].items():
                ks.append(f'volumes/{name}/mount/{k}')
                vs.append(v)
            await self.etcd.put_multi(ks, vs)
        log.info('done')

    async def manager_status_update(self):
        async for ev in self.etcd.watch('manager/status'):
            yield ev

    async def update_manager_status(self, status):
        await self.etcd.put('manager/status', status.value)

    @aiotools.lru_cache(maxsize=1)
    async def get_manager_status(self):
        status = await self.etcd.get('manager/status')
        return ManagerStatus(status)

    @aiotools.lru_cache(maxsize=1, expire_after=60.0)
    async def get_allowed_origins(self):
        origins = await self.etcd.get('config/api/allow-origins')
        if origins is None:
            origins = '*'
        return origins

    @aiotools.lru_cache(expire_after=60.0)
    async def get_image_required_slots(self, image_ref: ImageRef):
        installed = await self.etcd.get(f'images/{image_ref.name}')
        if installed is None:
            raise RuntimeError('Image metadata is not available!')
        tag_path = f'images/{image_ref.name}/tags/{image_ref.tag}'
        cpu = await self.etcd.get(f'{tag_path}/resource/cpu/max')
        if cpu is None:
            cpu_min = Decimal(await self.etcd.get(f'{tag_path}/resource/cpu/min'))
            cpu = max(Decimal(cpu_min), Decimal(_default_cpu_max))
        else:
            cpu = Decimal(cpu)
        mem = await self.etcd.get(f'{tag_path}/resource/mem/max')
        if mem is None:
            mem_min = Decimal(await self.etcd.get(f'{tag_path}/resource/mem/min'))
            mem = Decimal(max(BinarySize.from_str(mem_min),
                              BinarySize.from_str(_default_mem_max)))
        else:
            mem = Decimal(mem)
        accel_slots = []
        accels = await self.etcd.get(f'{tag_path}/accelerators')
        if accels is None:
            accels = []
        for accel in accels:
            # TODO: store resource slot metadata in agent heartbeats
            accel_info = AcceleratorInfo.from_key(accel)
            for res_slot in accel_info.resource_slots():
                r_max = await self.etcd.get(f'{tag_path}/resource/{res_slot}/max')
                if r_max is None:
                    r_value = Decimal(0)
                else:
                    if res_slot.style == 'memory':
                        r_value = Decimal(BinarySize.from_str(r_max))
                    else:
                        r_value = Decimal(r_max)
                accel_slots[res_slot.key] = r_value
        return ResourceSlot(mem=mem, cpu=cpu, accel_slots=accel_slots)


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
