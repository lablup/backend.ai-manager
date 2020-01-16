# Type annotation helpers

from __future__ import annotations

from typing import (
    Any,
    Awaitable, Callable, Iterable,
    AsyncIterator,
    Tuple,
    Mapping,
)

from aiohttp import web
import aiohttp_cors


WebRequestHandler = Callable[
    [web.Request],
    Awaitable[web.StreamResponse]
]
WebMiddleware = Callable[
    [web.Request, WebRequestHandler],
    Awaitable[web.StreamResponse]
]

CORSOptions = Mapping[str, aiohttp_cors.ResourceOptions]
AppCreator = Callable[
    [CORSOptions],
    Tuple[web.Application, Iterable[WebMiddleware]]
]
PluginAppCreator = Callable[
    [Mapping[str, Any], CORSOptions],
    Tuple[web.Application, Iterable[WebMiddleware]]
]

CleanupContext = Callable[[web.Application], AsyncIterator[None]]


class Sentinel(object):
    pass
