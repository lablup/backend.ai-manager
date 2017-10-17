import functools
import io
import re
import traceback

from aiohttp import web

_rx_sitepkg_path = re.compile(r'^.+/site-packages/')


def method_placeholder(orig_method):
    async def _handler(request):
        raise web.HTTPMethodNotAllowed(request.method, [orig_method])
    return _handler


def prettify_traceback(exc):
    # Make a compact stack trace string
    with io.StringIO() as buf:
        while exc is not None:
            print(f'Exception: {exc!r}', file=buf)
            if exc.__traceback__ is None:
                print('  (no traceback available)', file=buf)
            else:
                for frame in traceback.extract_tb(exc.__traceback__):
                    short_path = _rx_sitepkg_path.sub('<sitepkg>/', frame.filename)
                    print(f'  {short_path}:{frame.lineno} ({frame.name})', file=buf)
            exc = exc.__context__
        return f'Traceback:\n{buf.getvalue()}'


def catch_unexpected(log, raven=None):

    def _wrap(func):

        @functools.wraps(func)
        async def _wrapped(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except:
                if raven:
                    raven.captureException()
                log.exception('unexpected error!')
                raise

        return _wrapped

    return _wrap
