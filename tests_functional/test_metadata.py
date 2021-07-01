#!/usr/bin/env python3

import unittest

from mock import MagicMock

from tests_functional import DoozerRunnerTestCase
from doozerlib import metadata


class TestMetadata(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_cgit_atom(self):
        data_obj = MagicMock(key="cluster-etcd-operator", filename="cluster-etcd-operator.yml", data={"name": "cluster-etcd-operator"})
        runtime = MagicMock()
        runtime.group_config.urls.cgit = "http://pkgs.devel.redhat.com/cgit"
        meta = metadata.Metadata("image", runtime, data_obj)
        entry_list = meta.cgit_atom_feed(commit_hash='35ecfa4436139442edc19585c1c81ebfaca18550')
        entry = entry_list[0]
        self.assertEqual(entry.updated, '2019-07-09T18:01:53+00:00')
        self.assertEqual(entry.id, '81597b027a3cb2c38865273b13ab320b361feca6')

        entry_list = meta.cgit_atom_feed(branch='rhaos-4.8-rhel-8')
        self.assertTrue(len(entry_list) > 1)
        self.assertIsNotNone(entry_list[0].id)


if __name__ == "__main__":
    unittest.main()
