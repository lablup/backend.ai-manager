import pytest

from ai.backend.gateway.exceptions import InstanceNotAvailable
# from ai.backend.manager.registry import AgentRegistry


'''
TODO: Rewrite!
'''


@pytest.mark.skip
async def test_01_create_kernel_with_no_instance(self):
    with self.assertRaises(InstanceNotAvailable):
        instance, kernel = await self.registry.create_kernel(image='python3')


@pytest.mark.skip
async def test_02_create_kernel(self):
    instance, kernel = await self.registry.create_kernel(image='python3')
    assert kernel.instance == instance.id
    assert kernel.addr == instance.addr
    await self.registry.destroy_kernel(kernel)
