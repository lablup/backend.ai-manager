from typing import (
    Sequence,
)
import uuid

import pytest  # noqa

from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (  # noqa
    AccessKey, AgentId, KernelId,
    ResourceSlot, SessionTypes,
)
from ai.backend.manager.scheduler import PendingSession, ExistingSession, AgentContext  # noqa
from ai.backend.manager.scheduler.dispatcher import load_scheduler
from ai.backend.manager.scheduler.fifo import FIFOSlotScheduler, LIFOSlotScheduler
from ai.backend.manager.scheduler.drf import DRFScheduler


def test_load_intrinsic():
    assert isinstance(load_scheduler('fifo', {}), FIFOSlotScheduler)
    assert isinstance(load_scheduler('lifo', {}), LIFOSlotScheduler)
    assert isinstance(load_scheduler('drf', {}), DRFScheduler)


example_group_id = uuid.uuid4()

example_total_capacity = ResourceSlot({'cpu': '4.0', 'mem': '4096'})

# example_pending_sessions = [
#     PendingSession(
#     ),
# ]

existing_kernel_ids: Sequence[KernelId] = [
    KernelId(uuid.uuid4()) for _ in range(3)
]

example_existing_sessions = [
    ExistingSession(
        kernel_id=existing_kernel_ids[0],
        access_key=AccessKey('user01'),
        sess_id='es01',
        sess_type=SessionTypes.BATCH,
        domain_name='default',
        group_id=example_group_id,
        scaling_group='sg01',
        image_ref=ImageRef('lablup/python:3.6-ubunt18.04'),
        occupying_slots=ResourceSlot({'cpu': '1.0', 'mem': '1024'}),
    ),
    ExistingSession(
        kernel_id=existing_kernel_ids[1],
        access_key=AccessKey('user02'),
        sess_id='es01',
        sess_type=SessionTypes.BATCH,
        domain_name='default',
        group_id=example_group_id,
        scaling_group='sg01',
        image_ref=ImageRef('lablup/python:3.6-ubunt18.04'),
        occupying_slots=ResourceSlot({'cpu': '1.0', 'mem': '1024'}),
    ),
    ExistingSession(
        kernel_id=existing_kernel_ids[2],
        access_key=AccessKey('user03'),
        sess_id='es01',
        sess_type=SessionTypes.BATCH,
        domain_name='default',
        group_id=example_group_id,
        scaling_group='sg01',
        image_ref=ImageRef('lablup/python:3.6-ubunt18.04'),
        occupying_slots=ResourceSlot({'cpu': '1.0', 'mem': '1024'}),
    ),
]
