from __future__ import annotations

import attr
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ai.backend.common.logging import Logger

    from ..config import LocalConfig


@attr.s(auto_attribs=True, frozen=True)
class CLIContext:
    logger: Logger
    local_config: LocalConfig
