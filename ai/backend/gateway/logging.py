from datetime import datetime
import logging, logging.config
import multiprocessing as mp
from pathlib import Path
import signal

from pythonjsonlogger.jsonlogger import JsonFormatter


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


def log_args(parser):
    parser.add('--debug', env_var='BACKEND_DEBUG',
               action='store_true', default=False,
               help='Set the debug mode and verbose logging. (default: false)')
    parser.add('-v', '--verbose', env_var='BACKEND_VERBOSE',
               action='store_true', default=False,
               help='Set even more verbose logging which includes all SQL '
                    'statements issued. (default: false)')
    parser.add('--log-file', env_var='BACKEND_LOG_FILE',
               type=Path, default=None,
               help='If set to a file path, line-by-line JSON logs will be '
                    'recorded there.  It also automatically rotates the logs using '
                    'dotted number suffixes. (default: None)')
    parser.add('--log-file-count', env_var='BACKEND_LOG_FILE_COUNT',
               type=int, default=10,
               help='The maximum number of rotated log files (default: 10)')
    parser.add('--log-file-size', env_var='BACKEND_LOG_FILE_SIZE',
               type=float, default=10.0,
               help='The maximum size of each log file in MiB (default: 10 MiB)')


def log_worker(config, log_queue):
    if config.log_file is not None:
        fmt = '(timestamp) (level) (name) (processName) (message)'
        file_handler = logging.handlers.RotatingFileHandler(
            filename=config.log_file,
            backupCount=config.log_file_count,
            maxBytes=1048576 * config.log_file_size,
            encoding='utf-8',
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(CustomJsonFormatter(fmt))
    while True:
        rec = log_queue.get()
        if rec is None:
            break
        file_handler.emit(rec)


def log_configure(config):
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
    log_queue = mp.Queue()

    def _finalize():
        log_queue.put(None)
        log_queue.close()
        log_queue.join_thread()

    if config.log_file is not None:
        log_cfg['handlers']['fileq'] = {
            'class': 'logging.handlers.QueueHandler',
            'level': 'DEBUG',
            'queue': log_queue,
        }
        for l in log_cfg['loggers'].values():
            l['handlers'].append('fileq')

    logging.config.dictConfig(log_cfg)
    # block signals that may interrupt/corrupt logging
    stop_signals = {signal.SIGINT, signal.SIGTERM}
    signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
    proc = mp.Process(target=log_worker, name='Logger',
                      args=(config, log_queue))
    proc.start()
    signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
    return _finalize
