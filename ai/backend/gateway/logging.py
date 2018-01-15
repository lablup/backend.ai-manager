from datetime import datetime
import logging, logging.config
from pathlib import Path
import threading

from pythonjsonlogger.jsonlogger import JsonFormatter

_tls = threading.local()


class CustomJsonFormatter(JsonFormatter):

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level', record.levelname):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname


def init_logger(config):
    log_cfg = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s '
                          '[%(processName)s] %(message)s',
                'field_styles': {'levelname': {'color': 'black', 'bold': True},
                                 'name': {'color': 'black', 'bold': True},
                                 'processName': {'color': 'black', 'bold': True},
                                 'asctime': {'color': 'black'}},
                'level_styles': {'info': {'color': 'cyan'},
                                 'debug': {'color': 'green'},
                                 'warning': {'color': 'yellow'},
                                 'error': {'color': 'red'},
                                 'critical': {'color': 'red', 'bold': True}},
            },
            'json': {
                '()': CustomJsonFormatter,
                'format': '(timestamp) (level) (name) (processName) (message)',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'colored',
                'stream': 'ext://sys.stderr',
            },
            'jsonfile': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': 'DEBUG',
                'filename': Path.cwd() / 'backend.log',
                'backupCount': 10,
                'maxBytes': 10485760,  # 10 MiB
                'formatter': 'json',
                'encoding': 'utf-8',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console', 'jsonfile'],
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
                'handlers': ['console', 'jsonfile'],
                'propagate': False,
                'level': 'DEBUG' if config.debug else 'INFO',
            },
        },
    }
    logging.config.dictConfig(log_cfg)
