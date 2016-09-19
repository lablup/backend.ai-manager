'''
Kernel session management.
'''

import asyncio

from aiohttp import web
import asyncpg

from .auth import auth_required


@auth_required
async def create(request):
    raise NotImplementedError

@auth_required
async def destroy(request):
    raise NotImplementedError

@auth_required
async def get_info(request):
    raise NotImplementedError

@auth_required
async def restart(request):
    raise NotImplementedError


async def init(app):
    app.router.add_route('POST', '/v1/kernel/create', create)
    app.router.add_route('GET', '/v1/kernel/{kernel_id}', get_info)
    app.router.add_route('PATCH', '/v1/kernel/{kernel_id}', restart)
    app.router.add_route('DELETE', '/v1/kernel/{kernel_id}', destroy)
