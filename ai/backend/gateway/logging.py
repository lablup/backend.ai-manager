import logging, logging.config
import threading


class ProcIdxLogAdapter(logging.LoggerAdapter):

    _tls = threading.local()

    @classmethod
    def set_pidx(cls, pidx):
        cls._tls.pidx = pidx

    def __init__(self, *args):
        super().__init__(*args, extra={})
        self.pidx = None

    def process(self, msg, kwargs):
        cls = type(self)
        if self.pidx is None and hasattr(cls._tls, 'pidx'):
            self.pidx = cls._tls.pidx
        if self.pidx is not None and self.pidx != -1:
            return f"[worker:{self.pidx}] {msg}", kwargs
        return f"[main] {msg}", kwargs


def init_logger(config):
    log_cfg = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
                'field_styles': {'levelname': {'color': 'black', 'bold': True},
                                 'name': {'color': 'black', 'bold': True},
                                 'asctime': {'color': 'black'}},
                'level_styles': {'info': {'color': 'cyan'},
                                 'debug': {'color': 'green'},
                                 'warning': {'color': 'yellow'},
                                 'error': {'color': 'red'},
                                 'critical': {'color': 'red', 'bold': True}},
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'colored',
                'stream': 'ext://sys.stderr',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'INFO',
            },
            'aiotools': {
                'handlers': ['console'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
            'aiopg': {
                'handlers': ['console'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
            'ai.backend': {
                'handlers': ['console'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
        },
    }
    logging.config.dictConfig(log_cfg)
