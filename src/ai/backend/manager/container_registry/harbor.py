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
        registry_project = self.registry_info.get('project')
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
        project_id = None
        while project_list_url is not None:
            async with sess.get(project_list_url, allow_redirects=False, **rqst_args) as resp:
                projects = await resp.json()
                for item in projects:
                    if item['name'] == registry_project:
                        project_id = item['project_id']
                        break
                project_list_url = None
                next_page_link = resp.links.get('next')
                if next_page_link:
                    next_page_url = cast(yarl.URL, next_page_link['url'])
                    project_list_url = (
                        self.registry_url
                        .with_path(next_page_url.path)
                        .with_query(next_page_url.query)
                    )
        if project_id is None:
            log.warning('There is no given project.')
            return
        repo_list_url: Optional[yarl.URL]
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
