#!/usr/bin/env python

from __future__ import unicode_literals
import unittest
import mock
import os
import tempfile
import logging
import shutil
from doozerlib import metadata

TEST_YAML = """---
name: 'test'
distgit:
  namespace: 'hello'"""


class TestMetadataModule(unittest.TestCase):
    def test_tag_exists(self):
        with mock.patch("requests.get") as mocked_get:
            mocked_get.return_value = mock.MagicMock(status_code=200)
            registry = "https://registry.example.com"
            namespace = "fake_namespace"
            image_name = "fake_image"
            tag = "fake_tag"
            expected_url = "https://registry.example.com/v1/repositories/fake_namespace/fake_image/tags/fake_tag"
            actual = metadata.tag_exists(registry, namespace, image_name, tag)
            mocked_get.assert_called_with(expected_url)
            self.assertEqual(True, actual)
            mocked_get.return_value = mock.MagicMock(status_code=404)
            actual = metadata.tag_exists(registry, namespace, image_name, tag)
            self.assertEqual(False, actual)
            mocked_get.return_value = mock.MagicMock(status_code=403)
            with self.assertRaises(IOError) as cm:
                metadata.tag_exists(registry, namespace, image_name, tag)
                self.assertIn("HTTP 403", str(cm.exception))


class TestMetadataClass(unittest.TestCase):
    def test_get_brew_image_name_short(self):
        with mock.patch("doozerlib.metadata.Metadata.__init__", return_value=None):
            obj = metadata.Metadata()
            obj.image_name = "openshift3/ose-ansible"
            expected = "openshift3-ose-ansible"
            actual = metadata.Metadata.get_brew_image_name_short.__call__(obj)
            self.assertEqual(expected, actual)

    def test_get_brew_image_name_short(self):
        with mock.patch("doozerlib.metadata.Metadata.__init__", return_value=None):
            obj = metadata.Metadata()
            obj.image_name = "openshift3/ose-ansible"
            expected = "openshift3-ose-ansible"
            actual = metadata.Metadata.get_brew_image_name_short.__call__(obj)
            self.assertEqual(expected, actual)

    def test_tag_exists(self):
        with mock.patch("doozerlib.metadata.Metadata.__init__", return_value=None), mock.patch("doozerlib.metadata.tag_exists", return_value=True) as mocked_module_tag_exists:
            obj = metadata.Metadata()
            obj.runtime = mock.MagicMock()
            obj.runtime.group_config.urls.brew_image_host = "registry.example.com"
            obj.runtime.group_config.urls.brew_image_namespace = "fake_namespace"
            obj.get_brew_image_name_short = mock.MagicMock(return_value="fake_image_name")
            actual = obj.tag_exists("fake_tag")
            self.assertTrue(actual)
            mocked_module_tag_exists.assert_called_with("https://registry.example.com", "fake_namespace", "fake_image_name", "fake_tag")


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger


class TestMetadata(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ocp-cd-test-logs")

        self.test_file = os.path.join(self.test_dir, "test_file")
        logging.basicConfig(filename=self.test_file, level=logging.DEBUG)
        self.logger = logging.getLogger()

        self.cwd = os.getcwd()
        os.chdir(self.test_dir)

        test_yml = open('test.yml', 'w')
        test_yml.write(TEST_YAML)
        test_yml.close()

    def tearDown(self):
        os.chdir(self.cwd)

        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.test_dir)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_init(self):
        """
        The metadata object appears to need to be created while CWD is
        in the root of a git repo containing a file called '<name>.yml'
        This file must contain a structure:
           {'distgit': {'namespace': '<value>'}}

        The metadata object requires:
          a type string <image|rpm>
          a Runtime object placeholder

        """

        #
        # Check the logs
        #
        logs = [l.rstrip() for l in open(self.test_file).readlines()]

        expected = 6
        actual = len(logs)
        self.assertEqual(
            expected, actual,
            "logging lines - expected: {}, actual: {}".
            format(expected, actual))


if __name__ == "__main__":
    unittest.main()
