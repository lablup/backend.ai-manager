from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager as actxmgr
from contextvars import ContextVar
import itertools
import logging
from typing import (
    Any,
    AsyncIterator,
    Final,
    Iterable,
    List,
    Mapping,
    Sequence,
    Tuple,
    TypedDict,
    TYPE_CHECKING,
)
from uuid import UUID

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import HardwareMetadata

import aiohttp
import attr
import graphene
import yarl

from .base import (
    Item, PaginatedList,
)
from ..exceptions import InvalidArgument
if TYPE_CHECKING:
    from .gql import GraphQueryContext

__all__ = (
    'StorageProxyInfo',
    'VolumeInfo',
    'BrowserSessionManager',
    'StorageVolume',
)

log = BraceStyleAdapter(logging.getLogger(__name__))


@attr.s(auto_attribs=True, slots=True, frozen=True)
class StorageProxyInfo:
    session: aiohttp.ClientSession
    secret: str
    client_api_url: yarl.URL
    manager_api_url: yarl.URL


AUTH_TOKEN_HDR: Final = 'X-BackendAI-Storage-Auth-Token'

_ctx_volumes_cache: ContextVar[List[Tuple[str, VolumeInfo]]] = ContextVar('_ctx_volumes')


class VolumeInfo(TypedDict):
    name: str
    backend: str
    path: str
    fsprefix: str
    capabilities: List[str]


class BrowserSessionManager:

    _proxies: Mapping[str, StorageProxyInfo]

    def __init__(self, storage_config: Mapping[str, Any]) -> None:
        self.config = storage_config
        self._proxies = {}
        for proxy_name, proxy_config in self.config['proxies'].items():
            connector = aiohttp.TCPConnector(ssl=proxy_config['ssl_verify'])
            session = aiohttp.ClientSession(connector=connector)
            self._proxies[proxy_name] = StorageProxyInfo(
                session=session,
                secret=proxy_config['secret'],
                client_api_url=yarl.URL(proxy_config['client_api']),
                manager_api_url=yarl.URL(proxy_config['manager_api']),
            )

    async def aclose(self) -> None:
        close_aws = []
        for proxy_info in self._proxies.values():
            close_aws.append(proxy_info.session.close())
        await asyncio.gather(*close_aws, return_exceptions=True)

    @staticmethod
    def split_host(vfolder_host: str) -> Tuple[str, str]:
        proxy_name, _, volume_name = vfolder_host.partition(':')
        print("Split host result: ", proxy_name, volume_name)
        return proxy_name, volume_name

    async def get_mount_path(self, vfolder_host: str, vfolder_id: UUID) -> str:
        async with self.request(
            vfolder_host, 'GET', 'folder/mount',
            json={
                'volume': self.split_host(vfolder_host)[1],
                'vfid': str(vfolder_id),
            },
        ) as (_, resp):
            reply = await resp.json()
            return reply['path']

    @actxmgr
    async def request(
        self,
        vfolder_host_or_proxy_name: str,
        method: str,
        request_relpath: str,
        /,
        *args,
        **kwargs,
    ) -> AsyncIterator[Tuple[yarl.URL, aiohttp.ClientResponse]]:
        proxy_name, _ = self.split_host(vfolder_host_or_proxy_name)
        try:
            proxy_info = self._proxies[proxy_name]
            print("proxy info ", proxy_info)
        except KeyError:
            raise InvalidArgument('There is no such storage proxy', proxy_name)
        headers = kwargs.pop('headers', {})
        headers[AUTH_TOKEN_HDR] = proxy_info.secret
        print("sp requests: ", method, proxy_info.manager_api_url, request_relpath,  headers)
        print("client api: ", proxy_info.client_api_url)
        print('args ', args, kwargs)
        async with proxy_info.session.request(
            method, proxy_info.manager_api_url / request_relpath,
            *args,
            headers=headers,
            **kwargs,
        ) as client_resp:
            print("client response: ", proxy_info.client_api_url, client_resp)
            yield proxy_info.client_api_url, client_resp
