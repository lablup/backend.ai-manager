from __future__ import annotations

from decimal import Decimal
from typing import (
    Any,
    Mapping,
    Sequence,
)
from unittest import mock
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4, UUID
from pprint import pprint

import attr
from dateutil.parser import parse as dtparse
import pytest

from ai.backend.common.docker import ImageRef
from ai.backend.common.types import (
    AccessKey, AgentId, KernelId,
    ResourceSlot, SessionTypes,
)

from ai.backend.manager.defs import DEFAULT_ROLE
from ai.backend.manager.scheduler import (
    KernelInfo,
    PendingSession,
    ExistingSession,
    AgentContext,
)
from ai.backend.manager.scheduler.dispatcher import load_scheduler
from ai.backend.manager.scheduler.fifo import FIFOSlotScheduler, LIFOSlotScheduler
from ai.backend.manager.scheduler.drf import DRFScheduler
from ai.backend.manager.scheduler.mof import MOFScheduler
from ai.backend.manager.scheduler.predicates import check_reserved_batch_session


def test_load_intrinsic():
    assert isinstance(load_scheduler('fifo', {}), FIFOSlotScheduler)
    assert isinstance(load_scheduler('lifo', {}), LIFOSlotScheduler)
    assert isinstance(load_scheduler('drf', {}), DRFScheduler)
    assert isinstance(load_scheduler('mof', {}), MOFScheduler)


example_group_id = uuid4()

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


@attr.s(auto_attribs=True, slots=True)
class SessionKernelIdPair:
    session_id: UUID
    kernel_ids: Sequence[KernelId]


pending_session_kernel_ids = [
    SessionKernelIdPair(
        session_id=UUID('251907d9-1290-4126-bc6c-000000000100'),
        kernel_ids=[KernelId(UUID('251907d9-1290-4126-bc6c-000000000100'))]),
    SessionKernelIdPair(
        session_id=UUID('251907d9-1290-4126-bc6c-000000000200'),
        kernel_ids=[KernelId(UUID('251907d9-1290-4126-bc6c-000000000200'))]),
    SessionKernelIdPair(
        # single-node mode multi-container session
        session_id=UUID('251907d9-1290-4126-bc6c-000000000300'),
        kernel_ids=[
            KernelId(UUID('251907d9-1290-4126-bc6c-000000000300')),
            KernelId(UUID('251907d9-1290-4126-bc6c-000000000301')),
            KernelId(UUID('251907d9-1290-4126-bc6c-000000000302')),
        ]),
]

existing_session_kernel_ids = [
    SessionKernelIdPair(
        session_id=UUID('251907d9-1290-4126-bc6c-100000000100'),
        kernel_ids=[
            KernelId(UUID('251907d9-1290-4126-bc6c-100000000100')),
            KernelId(UUID('251907d9-1290-4126-bc6c-100000000101')),
        ]),
    SessionKernelIdPair(
        session_id=UUID('251907d9-1290-4126-bc6c-100000000200'),
        kernel_ids=[KernelId(UUID('251907d9-1290-4126-bc6c-100000000200'))]),
    SessionKernelIdPair(
        # single-node mode multi-container session
        session_id=UUID('251907d9-1290-4126-bc6c-100000000300'),
        kernel_ids=[KernelId(UUID('251907d9-1290-4126-bc6c-100000000300'))]),
]

common_image_ref = ImageRef('lablup/python:3.6-ubunt18.04'),

_common_dummy_for_pending_session: Mapping[str, Any] = dict(
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
    domain_name='default',
    group_id=example_group_id,
)


@pytest.fixture
def example_pending_sessions():
    # lower indicies are enqueued first.
    return [
        PendingSession(  # rocm
            kernels=[
                KernelInfo(
                    kernel_id=pending_session_kernel_ids[0].kernel_ids[0],
                    session_id=pending_session_kernel_ids[0].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('2.0'),
                        'mem': Decimal('1024'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('1'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user01'),
            session_id=pending_session_kernel_ids[0].session_id,
            session_name='es01',
            session_type=SessionTypes.BATCH,
            cluster_mode='single-node',
            cluster_size=1,
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
            kernels=[
                KernelInfo(
                    kernel_id=pending_session_kernel_ids[1].kernel_ids[0],
                    session_id=pending_session_kernel_ids[1].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('1.0'),
                        'mem': Decimal('2048'),
                        'cuda.shares': Decimal('0.5'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user02'),
            session_id=pending_session_kernel_ids[1].session_id,
            session_name='es01',
            session_type=SessionTypes.BATCH,
            cluster_mode='single-node',
            cluster_size=1,
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
            kernels=[
                KernelInfo(
                    kernel_id=pending_session_kernel_ids[2].kernel_ids[0],
                    session_id=pending_session_kernel_ids[2].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('0.4'),
                        'mem': Decimal('512'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
                KernelInfo(
                    kernel_id=pending_session_kernel_ids[2].kernel_ids[1],
                    session_id=pending_session_kernel_ids[2].session_id,
                    access_key='dummy-access-key',
                    cluster_role='sub',
                    cluster_idx=1,
                    cluster_hostname="sub1",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('0.3'),
                        'mem': Decimal('256'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
                KernelInfo(
                    kernel_id=pending_session_kernel_ids[2].kernel_ids[2],
                    session_id=pending_session_kernel_ids[2].session_id,
                    access_key='dummy-access-key',
                    cluster_role='sub',
                    cluster_idx=2,
                    cluster_hostname="sub2",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('0.3'),
                        'mem': Decimal('256'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user03'),
            session_id=pending_session_kernel_ids[2].session_id,
            session_name='es01',
            session_type=SessionTypes.BATCH,
            cluster_mode='single-node',
            cluster_size=3,
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
            kernels=[
                KernelInfo(
                    kernel_id=existing_session_kernel_ids[0].kernel_ids[0],
                    session_id=existing_session_kernel_ids[0].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('1.0'),
                        'mem': Decimal('512'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
                KernelInfo(
                    kernel_id=existing_session_kernel_ids[0].kernel_ids[1],
                    session_id=existing_session_kernel_ids[0].session_id,
                    access_key='dummy-access-key',
                    cluster_role='sub',
                    cluster_idx=1,
                    cluster_hostname="sub1",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('2.0'),
                        'mem': Decimal('512'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('1'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user01'),
            session_id=existing_session_kernel_ids[0].session_id,
            session_name='es01',
            session_type=SessionTypes.BATCH,
            cluster_mode='single-node',
            cluster_size=2,
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
            kernels=[
                KernelInfo(
                    kernel_id=existing_session_kernel_ids[1].kernel_ids[0],
                    session_id=existing_session_kernel_ids[1].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('1.0'),
                        'mem': Decimal('2048'),
                        'cuda.shares': Decimal('0.5'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user02'),
            session_id=existing_session_kernel_ids[1].session_id,
            session_type=SessionTypes.BATCH,
            session_name='es01',
            cluster_mode='single-node',
            cluster_size=1,
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
            kernels=[
                KernelInfo(
                    kernel_id=existing_session_kernel_ids[2].kernel_ids[0],
                    session_id=existing_session_kernel_ids[2].session_id,
                    access_key='dummy-access-key',
                    cluster_role=DEFAULT_ROLE,
                    cluster_idx=0,
                    cluster_hostname=f"{DEFAULT_ROLE}0",
                    image_ref=common_image_ref,
                    resource_opts={},
                    requested_slots=ResourceSlot({
                        'cpu': Decimal('4.0'),
                        'mem': Decimal('4096'),
                        'cuda.shares': Decimal('0'),
                        'rocm.devices': Decimal('0'),
                    }),
                    bootstrap_script=None,
                    startup_command=None,
                ),
            ],
            access_key=AccessKey('user03'),
            session_id=existing_session_kernel_ids[2].session_id,
            session_type=SessionTypes.BATCH,
            session_name='es01',
            cluster_mode='single-node',
            cluster_size=1,
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
        if pending_sess.session_id == picked_session_id:
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
    assert picked_session_id == example_pending_sessions[0].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(example_agents, picked_session)
    assert agent_id == AgentId('i-001')


def test_lifo_scheduler(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = LIFOSlotScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[2].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(example_agents, picked_session)
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
        assert picked_session_id == example_pending_sessions[0].session_id
        picked_session = _find_and_pop_picked_session(
            example_pending_sessions, picked_session_id)
        agent_id = scheduler.assign_agent_for_session(example_mixed_agents, picked_session)
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
        assert picked_session_id == example_pending_sessions[-1].session_id
        picked_session = _find_and_pop_picked_session(
            example_pending_sessions, picked_session_id)
        agent_id = scheduler.assign_agent_for_session(example_mixed_agents, picked_session)
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
    assert picked_session_id == example_pending_sessions[1].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(example_agents, picked_session)
    assert agent_id == 'i-001'


def test_mof_scheduler_first_assign(example_agents, example_pending_sessions, example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(example_agents, picked_session)
    assert agent_id == 'i-001'


def test_mof_scheduler_second_assign(example_agents_first_one_assigned, example_pending_sessions,
                                     example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(
        example_agents_first_one_assigned, picked_session)
    assert agent_id == 'i-101'


def test_mof_scheduler_no_valid_agent(example_agents_no_valid, example_pending_sessions,
                                      example_existing_sessions):
    scheduler = MOFScheduler({})
    picked_session_id = scheduler.pick_session(
        example_total_capacity,
        example_pending_sessions,
        example_existing_sessions)
    assert picked_session_id == example_pending_sessions[0].session_id
    picked_session = _find_and_pop_picked_session(
        example_pending_sessions, picked_session_id)

    agent_id = scheduler.assign_agent_for_session(example_agents_no_valid, picked_session)
    assert agent_id is None


@pytest.mark.asyncio
@mock.patch('ai.backend.manager.scheduler.predicates.datetime')
async def test_multiple_timezones_for_reserved_batch_session_predicate(mock_dt):
    mock_db_conn = MagicMock()
    mock_sched_ctx = MagicMock()
    mock_sess_ctx = MagicMock()
    mock_sess_ctx.session_type = SessionTypes.BATCH
    mock_sess_ctx.kernel_id = 'fake-kernel-id'

    now = '2020-06-29T00:00:00+00:00'
    mock_dt.now = MagicMock(return_value=dtparse(now))

    # Start time is not yet reached (now < start time)
    start_time = '2020-06-29T00:00:01+00:00'
    mock_db_conn.scalar = AsyncMock(return_value=dtparse(start_time))
    result = await check_reserved_batch_session(mock_db_conn, mock_sched_ctx, mock_sess_ctx)
    assert not result.passed, (now, start_time)

    # Start time is reached (now > start time)
    start_time = '2020-06-28T23:59:59+00:00'
    mock_db_conn.scalar = AsyncMock(return_value=dtparse(start_time))
    result = await check_reserved_batch_session(mock_db_conn, mock_sched_ctx, mock_sess_ctx)
    assert result.passed, (now, start_time)

    # Start time is not yet reached by timezone (now < start time)
    # Note that 6/29 00:00 (UTC) < 6/29 00:00 (-09:00) == 6/29 09:00 (UTC)
    for i in range(1, 12):
        start_time = f'2020-06-29T00:00:00-{i:02d}:00'
        mock_db_conn.scalar = AsyncMock(return_value=dtparse(start_time))
        result = await check_reserved_batch_session(mock_db_conn, mock_sched_ctx, mock_sess_ctx)
        assert not result.passed, (now, start_time)

    # Start time is reached by timezone (now > start time)
    # Note that 6/29 00:00 (UTC) > 6/29 00:00 (+09:00) == 6/28 15:00 (UTC)
    for i in range(1, 12):
        start_time = f'2020-06-29T00:00:00+{i:02d}:00'
        mock_db_conn.scalar = AsyncMock(return_value=dtparse(start_time))
        result = await check_reserved_batch_session(mock_db_conn, mock_sched_ctx, mock_sess_ctx)
        assert result.passed, (now, start_time)

    # Should pass if start time is not specified (start immediately).
    mock_db_conn.scalar = AsyncMock(return_value=None)
    result = await check_reserved_batch_session(mock_db_conn, mock_sched_ctx, mock_sess_ctx)
    assert result.passed


# TODO: write tests for multiple agents and scaling groups
