#!/usr/bin/env python3

from __future__ import absolute_import, print_function, unicode_literals

import logging
import json
import unittest
import os
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

from doozerlib import rhcos
from doozerlib.model import Model


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger
        self.group_config = Model({})


def _urlopen_json_cm(mock_urlopen, content, rc=200):
    # serializes content as json and has the urlopen context manager return it
    cm = MagicMock()
    cm.getcode.return_value = rc
    cm.read.return_value = bytes(json.dumps(content), 'utf-8')
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm


class TestRhcos(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock(spec=logging.Logger)

        runtime = MockRuntime(self.logger)
        koji_mock = Mock()
        koji_mock.__enter__ = Mock()
        koji_mock.__enter__.return_value = koji_mock
        koji_mock.__exit__ = Mock()

        runtime.pooled_koji_client_session = Mock()
        runtime.pooled_koji_client_session.return_value = koji_mock
        self.runtime = runtime
        self.koji_mock = koji_mock
        self.respath = Path(os.path.dirname(__file__), 'resources')

    def tearDown(self):
        pass

    def test_release_url(self):
        self.assertIn("4.6-s390x", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "s390x").rhcos_release_url())
        self.assertNotIn("x86_64", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "x86_64").rhcos_release_url())
        self.assertIn("4.9-aarch64", rhcos.RHCOSBuildFinder(self.runtime, "4.9", "aarch64").rhcos_release_url())

    @patch('urllib.request.urlopen')
    def test_build_id(self, mock_urlopen):
        _urlopen_json_cm(mock_urlopen, dict(builds=['id-1', 'id-2']))
        self.assertEqual('id-1', rhcos.RHCOSBuildFinder(self.runtime, "4.4")._latest_rhcos_build_id())
        self.assertIn('/rhcos-4.4/', mock_urlopen.call_args_list[0][0][0])

        _urlopen_json_cm(mock_urlopen, dict(builds=[]))
        self.assertIsNone(rhcos.RHCOSBuildFinder(self.runtime, "4.2", "ppc64le")._latest_rhcos_build_id())
        self.assertIn('/rhcos-4.2-ppc64le/', mock_urlopen.call_args_list[1][0][0])

    @patch('doozerlib.rhcos.RHCOSBuildFinder.latest_rhcos_build_id')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_build_meta(self, meta_mock, id_mock):
        id_mock.return_value = "dummy"
        meta_mock.return_value = dict(oscontainer=dict(image="test", digest="sha256:1234abcd"))
        self.assertEqual(("dummy", "test@sha256:1234abcd"), rhcos.RHCOSBuildFinder(self.runtime, "4.4").latest_machine_os_content())

        id_mock.return_value = None
        self.assertEqual((None, None), rhcos.RHCOSBuildFinder(self.runtime, "4.4").latest_machine_os_content())

    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_rhcos_build_inspector(self, rhcos_build_meta_mock):
        """
        Tests the RHCOS build inspector abstraction to ensure it correctly parses and utilizes
        pre-canned data.
        """
        # Data source: https://releases-rhcos-art.cloud.privileged.psi.redhat.com/?stream=releases/rhcos-4.7-s390x&release=47.83.202107261211-0#47.83.202107261211-0
        rhcos_meta = json.loads(self.respath.joinpath('rhcos1', '47.83.202107261211-0.meta.json').read_text())
        rhcos_commitmeta = json.loads(self.respath.joinpath('rhcos1', '47.83.202107261211-0.commitmeta.json').read_text())
        rpm_defs = yaml.safe_load(self.respath.joinpath('rhcos1', '47.83.202107261211-0.rpm_defs.yaml').read_text())
        pkg_build_dicts = yaml.safe_load(self.respath.joinpath('rhcos1', '47.83.202107261211-0.pkg_builds.yaml').read_text())

        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]
        rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, '47.83.202107261211-0', 's390x')
        self.assertEqual(rhcos_build.brew_arch, 's390x')
        self.assertEqual(rhcos_build.get_image_pullspec(), 'quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:d51ca4e301cfdbc98e16ace0bcbee02b143a8be9e454ce5fb196467981141f59')

        self.assertEqual(rhcos_build.stream_version, '4.7')
        self.assertEqual(rhcos_build.get_rhel_base_version(), 8)

        def canned_getRPM(nvra, *_, **__):
            return rpm_defs[nvra]

        def canned_getBuild(build_id, *_, **__):
            return pkg_build_dicts[build_id]

        self.koji_mock.getRPM.side_effect = canned_getRPM
        self.koji_mock.getBuild.side_effect = canned_getBuild

        self.assertIn("util-linux-2.32.1-24.el8.s390x", rhcos_build.get_rpm_nvras())
        self.assertIn("util-linux-2.32.1-24.el8", rhcos_build.get_rpm_nvrs())
        self.assertEqual(rhcos_build.get_package_build_objects()['dbus']['nvr'], 'dbus-1.12.8-12.el8_3')
        self.assertEqual(rhcos_build.get_machine_os_content_digest(), 'sha256:d51ca4e301cfdbc98e16ace0bcbee02b143a8be9e454ce5fb196467981141f59')


if __name__ == "__main__":
    unittest.main()
