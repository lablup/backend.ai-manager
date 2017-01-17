import io
import re
import traceback

_rx_sitepkg_path = re.compile(r'^.+/site-packages/')


def prettify_traceback(exc):
    # Make a compact stack trace string
    with io.StringIO() as buf:
        while exc is not None:
            print(f'Exception: {exc!r}', file=buf)
            for frame in traceback.extract_tb(exc.__traceback__):
                short_path = self._rx_sitepkg_path.sub(
                    '<sitepkg>/', frame.filename)
                print(f'  {short_path}:{frame.lineno} ({frame.name})', file=buf)
            exc = exc.__context__
        return f'Traceback:\n{buf.getvalue()}'
