import asyncio

import pytest

from ai.backend.manager.models import verify_dotfile_name, verify_vfolder_name
from ai.backend.gateway.utils import (
    call_non_bursty,
)


@pytest.mark.asyncio
async def test_call_non_bursty():
    key = 'x'
    execution_count = 0

    async def execute():
        nonlocal execution_count
        await asyncio.sleep(0)
        execution_count += 1

    # ensure reset
    await asyncio.sleep(0.11)

    # check run as coroutine
    execution_count = 0
    with pytest.raises(TypeError):
        await call_non_bursty(key, execute())

    # check run as coroutinefunction
    execution_count = 0
    await call_non_bursty(key, execute)
    assert execution_count == 1
    await asyncio.sleep(0.11)

    # check burstiness control
    execution_count = 0
    for _ in range(129):
        await call_non_bursty(key, execute)
    assert execution_count == 3
    await asyncio.sleep(0.01)
    await call_non_bursty(key, execute)
    assert execution_count == 3
    await asyncio.sleep(0.11)
    await call_non_bursty(key, execute)
    assert execution_count == 4
    for _ in range(64):
        await call_non_bursty(key, execute)
    assert execution_count == 5


def test_vfolder_name_validator():
    assert not verify_vfolder_name('.bashrc')
    assert not verify_vfolder_name('.terminfo')
    assert verify_vfolder_name('bashrc')
    assert verify_vfolder_name('.config')


def test_dotfile_name_validator():
    assert not verify_dotfile_name('.terminfo')
    assert not verify_dotfile_name('.config')
    assert not verify_dotfile_name('.ssh/authorized_keys')
    assert verify_dotfile_name('.bashrc')
    assert verify_dotfile_name('.ssh/id_rsa')
