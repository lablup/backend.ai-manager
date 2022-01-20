import asyncio
from datetime import datetime
import functools
import json
import logging
import math
from pathlib import Path
import stat
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Set,
    TYPE_CHECKING,
    Tuple,
    Iterable
)
import uuid

import aiohttp
from aiohttp import web
import aiohttp_cors
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.logging import BraceStyleAdapter

from ..models import (
    agents,
    kernels,
    users, groups, keypairs,
    vfolders, vfolder_invitations, vfolder_permissions,
    AgentStatus,
    KernelStatus,
    VFolderInvitationState,
    VFolderOwnershipType,
    VFolderPermission,
    VFolderPermissionValidator,
    VFolderUsageMode,
    UserRole,
    query_accessible_vfolders,
    query_owned_dotfiles,
    get_allowed_vfolder_hosts_by_group,
    get_allowed_vfolder_hosts_by_user,
    verify_vfolder_name,
)
from .auth import admin_required, auth_required, superadmin_required
from .exceptions import (
    VFolderCreationFailed, VFolderNotFound, VFolderAlreadyExists, VFolderOperationFailed,
    GenericForbidden, GenericNotFound, InvalidAPIParameters, ServerMisconfiguredError,
    BackendAgentError, InternalServerError, GroupNotFound,
)
from .manager import (
    READ_ALLOWED, ALL_ALLOWED,
    server_status_required,
)
from .resource import get_watcher_info
from .utils import check_api_params

from .context import RootContext
from .types import CORSOptions, WebMiddleware

log = BraceStyleAdapter(logging.getLogger(__name__))

VFolderRow = Mapping[str, Any]

async def get_vfid(root_ctx: RootContext, name: str) -> str:
    async with root_ctx.db.begin() as conn:
        query = (
            sa.select([vfolders.c.id])
            .select_from(vfolders)
            .where(vfolders.c.name == name)
        )
        folder_id = await conn.scalar(query)

        query = (sa.delete(vfolders).where(vfolders.c.id == folder_id))

        return folder_id.hex


@auth_required
@server_status_required(READ_ALLOWED)
async def create_or_update_filebrowser(request: web.Request) -> web.Response:
    json = await request.json()
    print(request)

    root_ctx: RootContext = request.app['_root.context']
    access_key = request['keypair']['access_key']
    print(access_key)
    
    vfolders = []
    for vfolder_name in json['vfolders']:
        vfolders.append(
        {
            "name":vfolder_name,
            "vfid": await get_vfid(root_ctx, vfolder_name)
        
        }
        )


    print("Vfolder name with vfid", vfolders)

    try:      
        async with root_ctx.storage_manager.request('local:volume1', 'POST', 'browser/create',
            
            json = { "vfolders":  vfolders },
            raise_for_status=True,
        ):
            pass
    except aiohttp.ClientResponseError:
        raise
    return request

async def init(app: web.Application) -> None:
    pass

async def shutdown(app: web.Application) -> None:
    pass

def create_app(default_cors_options: CORSOptions) -> Tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app['prefix'] = 'browser'
    print("FileBrowser Server started...")
    app['api_versions'] = (2, 3, )
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)

    cors.add(app.router.add_route('POST',  r'/create', create_or_update_filebrowser))
    return app, []
