from __future__ import annotations

import asyncio
from contextvars import ContextVar
import logging
import json
from typing import Any, AsyncIterator, Dict, Mapping, Optional, TYPE_CHECKING, cast

import aiohttp
import aiotools
import yarl

from abc import ABCMeta, abstractmethod

from ai.backend.common.docker import (
    login as registry_login,
    MIN_KERNELSPEC, MAX_KERNELSPEC,
)
from ai.backend.common.etcd import (
    AsyncEtcd,
    quote as etcd_quote,
)
from ai.backend.common.logging import BraceStyleAdapter
if TYPE_CHECKING:
    from ..background import ProgressReporter
from ..api.utils import chunked

log = BraceStyleAdapter(logging.getLogger(__name__))


class BaseContainerRegistry(metaclass=ABCMeta):

    etcd: AsyncEtcd
    registry_name: str
    registry_info: Mapping[str, Any]
    registry_url: yarl.URL
    max_concurrency_per_registry: int
    base_hdrs: Dict[str, str]
    credentials: Dict[str, str]
    ssl_verify: bool

    sema: ContextVar[asyncio.Semaphore]
    reporter: ContextVar[Optional[ProgressReporter]]
    all_updates: ContextVar[Dict[str, str]]

    def __init__(
        self,
        etcd: AsyncEtcd,
        registry_name: str,
        registry_info: Mapping[str, Any],
        *,
        max_concurrency_per_registry: int = 4,
        ssl_verify: bool = True,
    ) -> None:
        self.etcd = etcd
        self.registry_name = registry_name
        self.registry_info = registry_info
        self.registry_url = registry_info['']
        self.max_concurrency_per_registry = max_concurrency_per_registry
        self.base_hdrs = {
            'Accept': 'application/vnd.docker.distribution.manifest.v2+json',
        }
        self.credentials = {}
        self.ssl_verify = ssl_verify
        self.sema = ContextVar('sema')
        self.reporter = ContextVar('reporter', default=None)
        self.all_updates = ContextVar('all_updates')

    async def rescan_single_registry(
        self,
        reporter: ProgressReporter = None,
    ) -> None:
        self.all_updates.set({})
        self.sema.set(asyncio.Semaphore(self.max_concurrency_per_registry))
        self.reporter.set(reporter)
        username = self.registry_info['username']
        if username is not None:
            self.credentials['username'] = username
        password = self.registry_info['password']
        if password is not None:
            self.credentials['password'] = password
        non_kernel_words = (
            'common-', 'commons-', 'base-',
            'krunner', 'builder',
            'backendai', 'geofront',
        )
        ssl_ctx = None  # default
        if not self.registry_info['ssl-verify']:
            ssl_ctx = False
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as sess:
            async with aiotools.TaskGroup() as tg:
                async for image in self.fetch_repositories(sess):
                    if not any((w in image) for w in non_kernel_words):  # skip non-kernel images
                        tg.create_task(self._scan_image(sess, image))
        all_updates = self.all_updates.get()
        if not all_updates:
            log.info('No images found in registry {0}', self.registry_url)
        else:
            for kvlist in chunked(sorted(all_updates.items()), 16):
                await self.etcd.put_dict(dict(kvlist))

    async def _scan_image(
        self,
        sess: aiohttp.ClientSession,
        image: str,
    ) -> None:
        rqst_args = await registry_login(
            sess,
            self.registry_url,
            self.credentials,
            f'repository:{image}:pull',
        )
        rqst_args['headers'].update(**self.base_hdrs)
        tags = []
        tag_list_url: Optional[yarl.URL]
        tag_list_url = (self.registry_url / f'v2/{image}/tags/list').with_query(
            {'n': '10'},
        )
        while tag_list_url is not None:
            async with sess.get(tag_list_url, **rqst_args) as resp:
                data = json.loads(await resp.read())
                if 'tags' in data:
                    # sometimes there are dangling image names in the hub.
                    tags.extend(data['tags'])
                tag_list_url = None
                next_page_link = resp.links.get('next')
                if next_page_link:
                    next_page_url = cast(yarl.URL, next_page_link['url'])
                    tag_list_url = (
                        self.registry_url
                        .with_path(next_page_url.path)
                        .with_query(next_page_url.query)
                    )
        if (reporter := self.reporter.get()) is not None:
            reporter.total_progress += len(tags)
        async with aiotools.TaskGroup() as tg:
            for tag in tags:
                tg.create_task(self._scan_tag(sess, rqst_args, image, tag))

    async def _scan_tag(
        self,
        sess: aiohttp.ClientSession,
        rqst_args,
        image: str,
        tag: str,
    ) -> None:
        config_digest = None
        labels = {}
        skip_reason = None
        try:
            async with self.sema.get():
                async with sess.get(self.registry_url / f'v2/{image}/manifests/{tag}',
                                    **rqst_args) as resp:
                    if resp.status == 404:
                        # ignore missing tags
                        # (may occur after deleting an image from the docker hub)
                        skip_reason = "missing/deleted"
                        return
                    resp.raise_for_status()
                    data = await resp.json()
                    config_digest = data['config']['digest']
                    size_bytes = (sum(layer['size'] for layer in data['layers']) +
                                    data['config']['size'])

                async with sess.get(self.registry_url / f'v2/{image}/blobs/{config_digest}',
                                    **rqst_args) as resp:
                    # content-type may not be json...
                    resp.raise_for_status()
                    data = json.loads(await resp.read())
                    if 'container_config' in data:
                        raw_labels = data['container_config']['Labels']
                        if raw_labels:
                            labels.update(raw_labels)
                    else:
                        raw_labels = data['config']['Labels']
                        if raw_labels:
                            labels.update(raw_labels)
            if 'ai.backend.kernelspec' not in labels:
                # Skip non-Backend.AI kernel images
                skip_reason = "missing kernelspec"
                return
            if not (MIN_KERNELSPEC <= int(labels['ai.backend.kernelspec']) <= MAX_KERNELSPEC):
                # Skip unsupported kernelspec images
                skip_reason = "unsupported kernelspec"
                return

            updates = {}
            updates[f'images/{etcd_quote(self.registry_name)}/'
                    f'{etcd_quote(image)}'] = '1'
            tag_prefix = f'images/{etcd_quote(self.registry_name)}/' \
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
            self.all_updates.get().update(updates)
        finally:
            if skip_reason:
                log.warning('Skipped image - {}:{} ({})', image, tag, skip_reason)
                progress_msg = f"Skipped {image}:{tag} ({skip_reason})"
            else:
                log.info('Updated image - {0}:{1}', image, tag)
                progress_msg = f"Updated {image}:{tag}"
            if (reporter := self.reporter.get()) is not None:
                await reporter.update(1, message=progress_msg)

    @abstractmethod
    async def fetch_repositories(
        self,
        sess: aiohttp.ClientSession,
    ) -> AsyncIterator[str]:
        yield ""
