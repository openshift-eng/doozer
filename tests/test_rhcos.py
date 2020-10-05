#!/usr/bin/env python

from __future__ import absolute_import, print_function, unicode_literals
import json
import unittest
from flexmock import flexmock
from unittest.mock import patch, MagicMock

from doozerlib import rhcos


def _urlopen_json_cm(mock_urlopen, content, rc=200):
    # serializes content as json and has the urlopen context manager return it
    cm = MagicMock()
    cm.getcode.return_value = rc
    cm.read.return_value = bytes(json.dumps(content), 'utf-8')
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm


class TestRhcos(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_release_url(self):
        self.assertIn("4.6-s390x", rhcos.rhcos_release_url("4.6", "s390x"))
        self.assertNotIn("x86_64", rhcos.rhcos_release_url("4.6", "x86_64"))

    @patch('urllib.request.urlopen')
    def test_build_id(self, mock_urlopen):
        _urlopen_json_cm(mock_urlopen, dict(builds=['id-1', 'id-2']))
        self.assertEqual('id-1', rhcos._latest_rhcos_build_id("4.4"))
        self.assertIn('/rhcos-4.4/', mock_urlopen.call_args_list[0][0][0])

        _urlopen_json_cm(mock_urlopen, dict(builds=[]))
        self.assertIsNone(rhcos._latest_rhcos_build_id("4.2", "ppc64le"))
        self.assertIn('/rhcos-4.2-ppc64le/', mock_urlopen.call_args_list[1][0][0])

    @patch('doozerlib.rhcos.latest_rhcos_build_id')
    @patch('doozerlib.rhcos.rhcos_build_meta')
    def test_build_meta(self, meta_mock, id_mock):
        id_mock.return_value = "dummy"
        meta_mock.return_value = dict(oscontainer=dict(image="test", digest="sha256:1234abcd"))
        self.assertEqual(("dummy", "test@sha256:1234abcd"), rhcos.latest_machine_os_content("4.4"))

        id_mock.return_value = None
        self.assertEqual((None, None), rhcos.latest_machine_os_content("4.4"))


if __name__ == "__main__":
    unittest.main()
