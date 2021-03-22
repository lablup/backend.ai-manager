from __future__ import annotations

from typing import (
    Awaitable,
    Callable,
    Iterable,
    AsyncContextManager,
    # AsyncIterator,
    Tuple,
    Type,
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

# CleanupContext = Callable[[RootContext], AsyncIterator[None]]
CleanupContext = Type[AsyncContextManager]
