"""
Common definitions/constants used throughout the manager.
"""

import re
from typing import Final

from ai.backend.common.types import SlotName, SlotTypes


INTRINSIC_SLOTS: Final = {
    SlotName('cpu'): SlotTypes('count'),
    SlotName('mem'): SlotTypes('bytes'),
}


# The default container role name for multi-container sessions
DEFAULT_ROLE: Final = "main"

_RESERVED_VFOLDER_PATTERNS = [r'^\.[a-z0-9]+rc$', r'^\.[a-z0-9]+_profile$']
RESERVED_DOTFILES = ['.terminfo', '.jupyter', '.ssh', '.ssh/authorized_keys', '.local', '.config']
RESERVED_VFOLDERS = ['.terminfo', '.jupyter', '.tmux.conf', '.ssh']
RESERVED_VFOLDER_PATTERNS = [re.compile(x) for x in _RESERVED_VFOLDER_PATTERNS]
