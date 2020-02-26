"""
Common definitions/constants used throughout the manager.
"""

from typing import Final

from ai.backend.common.types import SlotName, SlotTypes


INTRINSIC_SLOTS: Final = {
    SlotName('cpu'): SlotTypes('count'),
    SlotName('mem'): SlotTypes('bytes'),
}
