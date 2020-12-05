import logging
from typing import AsyncIterator, Optional, cast

import aiohttp
import yarl

from ai.backend.common.logging import BraceStyleAdapter

from .base import BaseContainerRegistry

log = BraceStyleAdapter(logging.getLogger(__name__))


class HarborRegistry_v1(BaseContainerRegistry):

    async def fetch_repositories(
        self,
        sess: aiohttp.ClientSession,
    ) -> AsyncIterator[str]:
        registry_projects = self.registry_info['project']
        rqst_args = {}
        if self.credentials:
            rqst_args['auth'] = aiohttp.BasicAuth(
                self.credentials['username'],
                self.credentials['password'],
            )
        project_list_url: Optional[yarl.URL]
        project_list_url = (self.registry_url / 'api/projects').with_query(
            {'page_size': '30'}
        )
        project_ids = []
        while project_list_url is not None:
            async with sess.get(project_list_url, allow_redirects=False, **rqst_args) as resp:
                projects = await resp.json()
                for item in projects:
                    if item['name'] in registry_projects:
                        project_ids.append(item['project_id'])
                project_list_url = None
                next_page_link = resp.links.get('next')
                if next_page_link:
                    next_page_url = cast(yarl.URL, next_page_link['url'])
                    project_list_url = (
                        self.registry_url
                        .with_path(next_page_url.path)
                        .with_query(next_page_url.query)
                    )
        if not project_ids:
            log.warning('There is no given project.')
            return
        repo_list_url: Optional[yarl.URL]
        for project_id in project_ids:
            repo_list_url = (self.registry_url / 'api/repositories').with_query(
                {'project_id': project_id, 'page_size': '30'}
            )
            while repo_list_url is not None:
                async with sess.get(repo_list_url, allow_redirects=False, **rqst_args) as resp:
                    items = await resp.json()
                    repos = [item['name'] for item in items]
                    for item in repos:
                        yield item
                    repo_list_url = None
                    next_page_link = resp.links.get('next')
                    if next_page_link:
                        next_page_url = cast(yarl.URL, next_page_link['url'])
                        repo_list_url = (
                            self.registry_url
                            .with_path(next_page_url.path)
                            .with_query(next_page_url.query)
                        )


class HarborRegistry_v2(BaseContainerRegistry):

    async def fetch_repositories(
        self,
        sess: aiohttp.ClientSession,
    ) -> AsyncIterator[str]:
        yield 'xx'
