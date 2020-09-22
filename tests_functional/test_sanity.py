#!/usr/bin/env python3

import unittest
from . import DoozerRunnerTestCase

import koji
import logging

from doozerlib import image, exectools, model
from doozerlib import brew


class TestSanity(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_data_dir_lock(self):
        """
        If --group specifies a @commitish, that commitish should be checked out in
        <working_dir>/ocp-build-data,
        """
        target_ocp_build_data_commitish = 'e4cb33b93b1c1364aa9fa1cd5fa3e6a1ae1e065b'
        group_name, _ = self.run_doozer(
            '--group', f'openshift-4.7@{target_ocp_build_data_commitish}',
            'config:read-group',
            'name'
        )
        self.assertEqual(group_name, 'openshift-4.7')

        checkout_commitish, _ = exectools.cmd_assert(f'git -C {self.dz_working_dir}/ocp-build-data rev-parse HEAD', strip=True)
        self.assertEqual(checkout_commitish, target_ocp_build_data_commitish)


if __name__ == "__main__":
    unittest.main()
