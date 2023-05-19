#!/usr/bin/env python

from pathlib import Path
import os
import yaml
import json
import logging

import unittest
from unittest.mock import MagicMock, Mock, patch

from doozerlib.image import BrewBuildImageInspector


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger


class TestBrewBuildImageInspector(unittest.TestCase):

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
        pass

    def tearDown(self):
        pass

    @patch("doozerlib.exectools.cmd_assert")
    def test_info_parsing(self, fake_cmd_assert):
        """
        Tests the brew build inspector abstraction to ensure it correctly parses and utilizes
        pre-canned data.
        """

        image_res = self.respath.joinpath('sriov-operator-must-gather')
        image_build_dict = yaml.safe_load(image_res.joinpath('container_image_brew_build_dict.yaml').read_text())
        image_archives = yaml.safe_load(image_res.joinpath('container_image_list_archives.yaml').read_text())
        image_archives_list_rpms = yaml.safe_load(image_res.joinpath('container_image_archive_rpms.yaml').read_text())  # dict[imageID] => koji.listRPMs(imageID)
        image_archives_builds_used = yaml.safe_load(image_res.joinpath('build_dicts.yaml').read_text())  # dict[buildID] => koji.getBuild(buildID)
        oc_info_dict = json.loads(image_res.joinpath('container_image_oc_info.json').read_text())

        self.koji_mock.getBuild.return_value = image_build_dict
        self.koji_mock.listArchives.return_value = image_archives
        fake_cmd_assert.return_value = (json.dumps(oc_info_dict), "")
        bbii = BrewBuildImageInspector(self.runtime, image_build_dict['nvr'])

        self.assertEqual(bbii.get_nvr(), image_build_dict['nvr'])
        self.assertEqual(bbii.get_source_git_url(), 'https://github.com/openshift/sriov-network-operator')
        self.assertEqual(bbii.get_labels()["io.openshift.build.commit.url"], "https://github.com/openshift/sriov-network-operator/commit/51a98df728d6712c469946b6ed5973c2fba1454e")

        self.assertEqual(len(bbii.get_all_archive_dicts()), 5)
        self.assertEqual(len(bbii.get_image_archive_dicts()), 4)  # filters out non-image archive types

        def canned_listRPMs(_, imageID, *__, **___):
            return image_archives_list_rpms[imageID]

        def canned_getBuild(build_id, *_, **__):
            return image_archives_builds_used[build_id]

        self.koji_mock.getBuild = MagicMock()  # Get rid of old return_value
        self.koji_mock.getBuild.side_effect = canned_getBuild
        self.koji_mock.listRPMs.side_effect = canned_listRPMs
        archive_inspectors = bbii.get_image_archive_inspectors()
        self.assertEqual(len(archive_inspectors), 4)  # One per image archive

        for ai in archive_inspectors:
            self.assertIn(ai.get_archive_id(), image_archives_list_rpms.keys(), 'ArchiveIDs not reported corrected from abstraction class')
            self.assertTrue(len(ai.get_installed_rpm_dicts()) > 0)
            bd = ai.get_installed_package_build_dicts()
            self.assertEqual(bd['grep']['nvr'], 'grep-3.1-6.el8')

        self.assertIsNone(bbii.get_image_archive_inspector('s390x').get_installed_package_build_dicts().get('tzdata', None))  # Remove this from the resource data by hand for this test
        self.assertIsNotNone(bbii.get_image_archive_inspector('x86_64').get_installed_package_build_dicts().get('tzdata', None))  # Remove this from the resource data by hand for this test
        self.assertIsNotNone(bbii.get_all_installed_package_build_dicts().get('tzdata', None))  # Even though not in s390x, it should be in aggregate
        self.assertEqual(bbii.get_all_installed_package_build_dicts()['grep']['nvr'], 'grep-3.1-6.el8')  # Ensure aggregate has the same nvr for grep

        self.assertEqual(bbii.get_image_archive_inspector('s390x').get_archive_pullspec(), 'registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-sriov-operator-must-gather:rhaos-4.9-rhel-8-containers-candidate-98861-20210726154614-s390x')
        self.assertEqual(bbii.get_image_archive_inspector('s390x').get_archive_digest(), 'sha256:1f3ebef02669eca018dbfd2c5a65575a21e4920ebe6a5328029a5000127aaa4b')


if __name__ == "__main__":
    unittest.main()
