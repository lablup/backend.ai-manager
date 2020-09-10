import asyncio
from contextlib import asynccontextmanager as actxmgr
import itertools
from typing import (
    Any,
    AsyncIterator,
    Final,
    Iterable,
    Mapping,
    Tuple,
)
from uuid import UUID

import aiohttp
import attr
import yarl

from ..exceptions import InvalidArgument


@attr.s(auto_attribs=True, slots=True, frozen=True)
class StorageProxyInfo:
    session: aiohttp.ClientSession
    secret: str
    client_api_url: yarl.URL
    manager_api_url: yarl.URL


AUTH_TOKEN_HDR: Final = 'X-BackendAI-Storage-Auth-Token'


class StorageSessionManager:

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
        return proxy_name, volume_name

    async def get_all_volumes(self) -> Iterable[Tuple[str, Mapping[str, Any]]]:
        fetch_aws = []

        async def _fetch(
            proxy_name: str,
            proxy_info: StorageProxyInfo,
        ) -> Iterable[Tuple[str, Mapping[str, Any]]]:
            async with proxy_info.session.request(
                'GET', proxy_info.manager_api_url / 'volumes',
                raise_for_status=True,
                headers={AUTH_TOKEN_HDR: proxy_info.secret},
            ) as resp:
                reply = await resp.json()
                return ((proxy_name, volume_data) for volume_data in reply['volumes'])

        for proxy_name, proxy_info in self._proxies.items():
            fetch_aws.append(_fetch(proxy_name, proxy_info))
        results = await asyncio.gather(*fetch_aws)
        return itertools.chain(*results)

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
        except KeyError:
            raise InvalidArgument('There is no such storage proxy', proxy_name)
        headers = kwargs.pop('headers', {})
        headers[AUTH_TOKEN_HDR] = proxy_info.secret
        async with proxy_info.session.request(
            method, proxy_info.manager_api_url / request_relpath,
            *args,
            headers=headers,
            **kwargs,
        ) as client_resp:
            yield proxy_info.client_api_url, client_resp
