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
        t.Key('x') >> 'z': t.Int,
        AliasedKey(['y', 'Y']): t.Int,
    })
    assert iv.check({'x': 1, 'y': 2}) == {'z': 1, 'y': 2}

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1})
    err_data = e.value.as_dict()
    assert 'y' in err_data
    assert "is required" in err_data['y']

    with pytest.raises(t.DataError) as e:
        iv.check({'y': 2})
    err_data = e.value.as_dict()
    assert 'x' in err_data
    assert "is required" in err_data['x']

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1, 'y': 'string'})
    err_data = e.value.as_dict()
    assert 'y' in err_data
    assert "can't be converted to int" in err_data['y']

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1, 'Y': 'string'})
    err_data = e.value.as_dict()
    assert 'Y' in err_data
    assert "can't be converted to int" in err_data['Y']

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

    with pytest.raises(t.DataError) as e:
        iv.check({'z': 99})
    err_data = e.value.as_dict()
    assert 'z' in err_data
    assert "not allowed key" in err_data['z']
