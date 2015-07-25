#! /usr/bin/env python3

import unittest
from subprocess import call
from .instance import Instance, Kernel, InstanceRegistry, InstanceNotAvailableError

class InstanceRegistryTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_add_instance(self):
        pass

    def test_delete_instance(self):
        pass

    def test_delete_instance_with_running_kernels(self):
        # Running kernels must be destroyed along with the instance.
        pass

    def test_create_kernel(self):
        # A single front-end server creates a kernel when therne is no instances.
        # A single front-end server creates a kernel when there are instances but with no capacity.
        # A single front-end server creates a kernel when there are instance with available capactiy.
        pass

    def test_create_kernel_race_condition(self):
        # Two front-end servers create kernels in an interleaved manner.
        pass

    def test_destroy_kernel(self):
        pass

    def test_destroy_kernel_race_condition(self):
        pass

class InstanceRegistryIntegrationTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
