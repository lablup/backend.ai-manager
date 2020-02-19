from __future__ import annotations

from decimal import Decimal
from typing import (
    Any,
    Mapping,
    Sequence,
)
import uuid
from pprint import pprint

import pytest

from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    AccessKey, AgentId, KernelId,
    ResourceSlot, SessionTypes,
)
from ai.backend.manager.scheduler import PendingSession, ExistingSession, AgentContext
from ai.backend.manager.scheduler.dispatcher import load_scheduler
from ai.backend.manager.scheduler.fifo import FIFOSlotScheduler, LIFOSlotScheduler
from ai.backend.manager.scheduler.drf import DRFScheduler
from ai.backend.manager.scheduler.mof import MOFScheduler


def test_load_intrinsic():
    assert isinstance(load_scheduler('fifo', {}), FIFOSlotScheduler)
    assert isinstance(load_scheduler('lifo', {}), LIFOSlotScheduler)
    assert isinstance(load_scheduler('drf', {}), DRFScheduler)
    assert isinstance(load_scheduler('mof', {}), MOFScheduler)


example_group_id = uuid.uuid4()

example_total_capacity = ResourceSlot({'cpu': '4.0', 'mem': '4096'})


@pytest.fixture
def example_agents():
    return [
        AgentContext(
            agent_id=AgentId('i-001'),
            agent_addr='10.0.1.1:6001',
            scaling_group='sg01',
            available_slots=ResourceSlot({
                'cpu': Decimal('4.0'),
                'mem': Decimal('4096'),
                'cuda.shares': Decimal('4.0'),
                'rocm.devices': Decimal('2'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
        ),
        AgentContext(
            agent_id=AgentId('i-101'),
            agent_addr='10.0.2.1:6001',
            scaling_group='sg02',
            available_slots=ResourceSlot({
                'cpu': Decimal('3.0'),
                'mem': Decimal('2560'),
                'cuda.shares': Decimal('1.0'),
                'rocm.devices': Decimal('8'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
        ),
    ]


@pytest.fixture
def example_mixed_agents():
    return [
        AgentContext(
            agent_id=AgentId('i-gpu'),
            agent_addr='10.0.1.1:6001',
            scaling_group='sg01',
            available_slots=ResourceSlot({
                'cpu': Decimal('4.0'),
                'mem': Decimal('4096'),
                'cuda.shares': Decimal('4.0'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
            }),
        ),
        AgentContext(
            agent_id=AgentId('i-cpu'),
            agent_addr='10.0.2.1:6001',
            scaling_group='sg02',
            available_slots=ResourceSlot({
                'cpu': Decimal('3.0'),
                'mem': Decimal('2560'),
                'cuda.shares': Decimal('0'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
            }),
        ),
    ]


@pytest.fixture
def example_agents_first_one_assigned():
    return [
        AgentContext(
            agent_id=AgentId('i-001'),
            agent_addr='10.0.1.1:6001',
            scaling_group='sg01',
            available_slots=ResourceSlot({
                'cpu': Decimal('2.0'),
                'mem': Decimal('2048'),
                'cuda.shares': Decimal('2.0'),
                'rocm.devices': Decimal('1'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('2.0'),
                'mem': Decimal('2048'),
                'cuda.shares': Decimal('2.0'),
                'rocm.devices': Decimal('1'),
            }),
        ),
        AgentContext(
            agent_id=AgentId('i-101'),
            agent_addr='10.0.2.1:6001',
            scaling_group='sg02',
            available_slots=ResourceSlot({
                'cpu': Decimal('3.0'),
                'mem': Decimal('2560'),
                'cuda.shares': Decimal('1.0'),
                'rocm.devices': Decimal('8'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
        ),
    ]


@pytest.fixture
def example_agents_no_valid():
    return [
        AgentContext(
            agent_id=AgentId('i-001'),
            agent_addr='10.0.1.1:6001',
            scaling_group='sg01',
            available_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('4.0'),
                'mem': Decimal('4096'),
                'cuda.shares': Decimal('4.0'),
                'rocm.devices': Decimal('2'),
            }),
        ),
        AgentContext(
            agent_id=AgentId('i-101'),
            agent_addr='10.0.2.1:6001',
            scaling_group='sg02',
            available_slots=ResourceSlot({
                'cpu': Decimal('0'),
                'mem': Decimal('0'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
            occupied_slots=ResourceSlot({
                'cpu': Decimal('3.0'),
                'mem': Decimal('2560'),
                'cuda.shares': Decimal('1.0'),
                'rocm.devices': Decimal('8'),
            }),
        ),
    ]


pending_kernel_ids: Sequence[KernelId] = [
    KernelId(uuid.uuid4()) for _ in range(3)
]

existing_kernel_ids: Sequence[KernelId] = [
    KernelId(uuid.uuid4()) for _ in range(3)
]

_common_dummy_for_pending_session: Mapping[str, Any] = dict(
    image_ref=ImageRef('lablup/python:3.6-ubunt18.04'),
    domain_name='default',
    group_id=example_group_id,
    resource_policy={},
    resource_opts={},
    mounts=[],
    mount_map={},
    environ={},
    bootstrap_script=None,
    startup_command=None,
    internal_data=None,
    preopen_ports=[],
)

_common_dummy_for_existing_session: Mapping[str, Any] = dict(
    image_ref=ImageRef('lablup/python:3.6-ubunt18.04'),
    domain_name='default',
    group_id=example_group_id,
)


@pytest.fixture
def example_pending_sessions():
    # lower indicies are enqueued first.
    return [
        PendingSession(  # rocm
            kernel_id=pending_kernel_ids[0],
            access_key=AccessKey('user01'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            scaling_group='sg01',
            requested_slots=ResourceSlot({
                'cpu': Decimal('2.0'),
                'mem': Decimal('1024'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('1'),
            }),
            target_sgroup_names=[],
            **_common_dummy_for_pending_session,
        ),
        PendingSession(  # cuda
            kernel_id=pending_kernel_ids[1],
            access_key=AccessKey('user02'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            scaling_group='sg01',
            requested_slots=ResourceSlot({
                'cpu': Decimal('1.0'),
                'mem': Decimal('2048'),
                'cuda.shares': Decimal('0.5'),
                'rocm.devices': Decimal('0'),
            }),
            target_sgroup_names=[],
            **_common_dummy_for_pending_session,
        ),
        PendingSession(  # cpu-only
            kernel_id=pending_kernel_ids[2],
            access_key=AccessKey('user03'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            scaling_group='sg01',
            requested_slots=ResourceSlot({
                'cpu': Decimal('1.0'),
                'mem': Decimal('1024'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
            target_sgroup_names=[],
            **_common_dummy_for_pending_session,
        ),
    ]


@pytest.fixture
def example_existing_sessions():
    return [
        ExistingSession(
            kernel_id=existing_kernel_ids[0],
            access_key=AccessKey('user01'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            occupying_slots=ResourceSlot({
                'cpu': Decimal('3.0'),
                'mem': Decimal('1024'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('1'),
            }),
            scaling_group='sg01',
            **_common_dummy_for_existing_session,
        ),
        ExistingSession(
            kernel_id=existing_kernel_ids[1],
            access_key=AccessKey('user02'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            occupying_slots=ResourceSlot({
                'cpu': Decimal('1.0'),
                'mem': Decimal('2048'),
                'cuda.shares': Decimal('0.5'),
                'rocm.devices': Decimal('0'),
            }),
            scaling_group='sg01',
            **_common_dummy_for_existing_session,
        ),
        ExistingSession(
            kernel_id=existing_kernel_ids[2],
            access_key=AccessKey('user03'),
            session_name='es01',
            session_type=SessionTypes.BATCH,
            occupying_slots=ResourceSlot({
                'cpu': Decimal('4.0'),
                'mem': Decimal('4096'),
                'cuda.shares': Decimal('0'),
                'rocm.devices': Decimal('0'),
            }),
            scaling_group='sg01',
            **_common_dummy_for_existing_session,
        ),
    ]


def _find_and_pop_picked_session(pending_sessions, picked_session_id):
    for picked_idx, pending_sess in enumerate(pending_sessions):
        if pending_sess.kernel_id == picked_session_id:
            break
    else:
        # no matching entry for picked session?
        raise RuntimeError('should not reach here')
    return pending_sessions.pop(picked_idx)


def test_fifo_scheduler(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = FIFOSlotScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(example_agents, picked_session)
    assert agent_id == AgentId('i-001')


def test_lifo_scheduler(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = LIFOSlotScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[2].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(example_agents, picked_session)
    assert agent_id == 'i-001'


def test_fifo_scheduler_favor_cpu_for_requests_without_accelerators(
    example_mixed_agents,
    example_pending_sessions,
):
    scheduler = FIFOSlotScheduler({})
    for idx in range(3):
        picked_session_id = scheduler.pick_session(
            example_total_capacity,
            example_pending_sessions,
            [])
        assert picked_session_id == example_pending_sessions[0].kernel_id
        picked_session = _find_and_pop_picked_session(
            example_pending_sessions, picked_session_id)
        agent_id = scheduler.assign_agent(example_mixed_agents, picked_session)
        if idx == 0:
            # example_mixed_agents do not have any agent with ROCM accelerators.
            assert agent_id is None
        elif idx == 1:
            assert agent_id == AgentId('i-gpu')
        elif idx == 2:
            # It should favor the CPU-only agent if the requested slots
            # do not include accelerators.
            assert agent_id == AgentId('i-cpu')


def test_lifo_scheduler_favor_cpu_for_requests_without_accelerators(
    example_mixed_agents,
    example_pending_sessions,
):
    # Check the reverse with the LIFO scheduler.
    # The result must be same.
    scheduler = LIFOSlotScheduler({})
    for idx in range(3):
        picked_session_id = scheduler.pick_session(
            example_total_capacity,
            example_pending_sessions,
            [])
        assert picked_session_id == example_pending_sessions[-1].kernel_id
        picked_session = _find_and_pop_picked_session(
            example_pending_sessions, picked_session_id)
        agent_id = scheduler.assign_agent(example_mixed_agents, picked_session)
        if idx == 2:
            # example_mixed_agents do not have any agent with ROCM accelerators.
            assert agent_id is None
        elif idx == 1:
            assert agent_id == AgentId('i-gpu')
        elif idx == 0:
            # It should favor the CPU-only agent if the requested slots
            # do not include accelerators.
            assert agent_id == AgentId('i-cpu')


def test_drf_scheduler(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = DRFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    pprint(example_pending_sessions)
    assert picked_session_id == example_pending_sessions[1].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(example_agents, picked_session)
    assert agent_id == 'i-001'


def test_mof_scheduler_first_assign(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(example_agents, picked_session)
    assert agent_id == 'i-001'


def test_mof_scheduler_second_assign(example_agents_first_one_assigned, example_pending_sessions,
                                     example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(
        example_agents_first_one_assigned, picked_session)
    assert agent_id == 'i-101'


def test_mof_scheduler_no_valid_agent(example_agents_no_valid, example_pending_sessions,
                                      example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].kernel_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent(example_agents_no_valid, picked_session)
    assert agent_id is None


# TODO: write tests for multiple agents and scaling groups
