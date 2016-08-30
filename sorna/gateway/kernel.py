'''
Kernel session management.
'''

import asyncio

from aiohttp import web
import asyncpg

from .auth import auth_required


@auth_required
async def create(request):
    return web.Response(body=b'Created')

@auth_required
async def destroy(request):
    return web.Response(body=b'Destroyed')


async def init(app):
    app.router.add_route('GET', '/kernel/create', create)
    app.router.add_route('GET', '/kernel/destroy', destroy)
