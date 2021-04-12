from __future__ import annotations

import atexit
import attr
import os
from pathlib import Path
from typing import TYPE_CHECKING

from ai.backend.common.logging import Logger

if TYPE_CHECKING:
    from ..config import LocalConfig


@attr.s(auto_attribs=True, frozen=True)
class CLIContext:
    logger: Logger
    local_config: LocalConfig


def init_logger(local_config: LocalConfig) -> Logger:
    if 'file' in local_config['logging']['drivers']:
        local_config['logging']['drivers'].remove('file')
    # log_endpoint = f'tcp://127.0.0.1:{find_free_port()}'
    log_sockpath = Path(f'/tmp/backend.ai/ipc/manager-cli-{os.getpid()}.sock')
    log_sockpath.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{log_sockpath}'

    def _clean_logger():
        try:
            os.unlink(log_sockpath)
        except FileNotFoundError:
            pass

    atexit.register(_clean_logger)
    return Logger(local_config['logging'], is_master=True, log_endpoint=log_endpoint)
