#!/usr/bin/env python

from __future__ import absolute_import, print_function, unicode_literals
import unittest
import mock
import os
import tempfile
import logging
import shutil
from flexmock import flexmock
from doozerlib import metadata
try:
    from importlib import reload
except ImportError:
    pass

TEST_YAML = """---
name: 'test'
distgit:
  namespace: 'hello'"""


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
        logs = [log.rstrip() for log in open(self.test_file).readlines()]

        expected = 6
        actual = len(logs)
        self.assertEqual(
            expected, actual,
            "logging lines - expected: {}, actual: {}".
            format(expected, actual))


if __name__ == "__main__":
    unittest.main()
