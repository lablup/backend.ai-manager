import asyncio
import trafaret as t

import pytest

from ai.backend.gateway.utils import (
    call_non_bursty,
    AliasedKey
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


def test_trafaret_extension():
    iv = t.Dict({
        t.Key('x', default=0): t.Int,
        AliasedKey(['y', 'Y'], default=1): t.Int,
    })

    assert iv.check({'x': 5, 'Y': 6}) == {'x': 5, 'y': 6}
    assert iv.check({'x': 5, 'y': 6}) == {'x': 5, 'y': 6}
    assert iv.check({'y': 3}) == {'x': 0, 'y': 3}
    assert iv.check({'Y': 3}) == {'x': 0, 'y': 3}
    assert iv.check({'x': 3}) == {'x': 3, 'y': 1}
    assert iv.check({}) == {'x': 0, 'y': 1}
    with pytest.raises(t.DataError):
        iv.check({'z': 99})
