#!/usr/bin/env
"""
Test the management of distgit repositories for RPM and image builds

$ python -m doozerlib.distgit_test
"""
import unittest
import flexmock
from mock import patch, ANY, call, Mock

import errno
import StringIO
import logging
import tempfile
import shutil
import re
from datetime import datetime, timedelta

from dockerfile_parse import DockerfileParser
import distgit
from model import Model
from .mocks import *


class TestDistgitRecursiveOverwrite(unittest.TestCase):
    """
    Mocking exectools.cmd_assert to prevent actual command executions, since
    the only purpose of these tests is to ensure that the correct command
    string is being generated.
    """

    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_without_ignore_set(self, cmd_assert_mock):
        expected_cmd = ("rsync -av "
                        " --exclude .git/ "
                        " my-source/ my-dest/")

        distgit.recursive_overwrite("my-source", "my-dest")
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=ANY)

    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_with_ignore_set(self, cmd_assert_mock):
        expected_cmd = ("rsync -av "
                        " --exclude .git/ "
                        " --exclude=\"me\" "
                        " --exclude=\"ignore\" "
                        " my-source/ my-dest/")

        distgit.recursive_overwrite("my-source", "my-dest", {"ignore", "me"})
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=ANY)


class TestDistgitPullImage(unittest.TestCase):
    """
    Mocking exectools.cmd_gather to prevent actual command executions.
    Also mocking time.sleep for faster tests.
    """

    @patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    def test_generate_podman_command(self, cmd_gather_mock):
        expected_cmd = ["podman", "pull", "my-image"]

        distgit.pull_image("my-image")
        cmd_gather_mock.assert_called_with(expected_cmd)

    @patch("distgit.time.sleep", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(1, "", ""))
    def test_pull_fails_more_than_3_times(self, _, __):
        expected_exception = distgit.exectools.RetryException
        self.assertRaises(expected_exception, distgit.pull_image, "my-image")

    @patch("distgit.logger.info", return_value=None)
    @patch("distgit.time.sleep", return_value=None)
    @patch("distgit.exectools.cmd_gather")
    def test_custom_logging(self, cmd_gather_mock, _, logger_info_mock):
        # pretending the cmd failed twice and succeeded on the third attempt.
        cmd_gather_mock.side_effect = [(1, "", ""), (1, "", ""), (0, "", "")]

        expected_logging_calls = [
            call("Pulling image: my-image"),
            call("Error pulling image my-image -- retrying in 60 seconds"),
            call("Error pulling image my-image -- retrying in 60 seconds"),
        ]

        distgit.pull_image("my-image")
        self.assertEqual(expected_logging_calls, logger_info_mock.mock_calls)


class TestDistgit(unittest.TestCase):
    """
    Test the methods and functions used to manage and update distgit repos
    """

    def setUp(self):
        """
        Define and provide mock logging for test/response
        """
        self.stream = StringIO.StringIO()
        logging.basicConfig(level=logging.DEBUG, stream=self.stream)
        self.logger = logging.getLogger()
        self.logs_dir = tempfile.mkdtemp()
        self.md = MockMetadata(MockRuntime(self.logger))

    def tearDown(self):
        """
        Reset logging for each test.
        """
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.logs_dir)


class TestRPMDistGit(TestDistgit):
    def setUp(self):
        super(TestRPMDistGit, self).setUp()
        self.rpm_dg = distgit.RPMDistGitRepo(self.md, autoclone=False)
        self.rpm_dg.runtime.group_config = Model()

    def test_pkg_find_in_spec(self):
        """ Test RPMDistGitRepo._find_in_spec """
        self.assertEqual("", self.rpm_dg._find_in_spec("spec", "(?mx) nothing", "thing"))
        self.assertIn("No thing found", self.stream.getvalue())
        self.assertEqual("SOMEthing", self.rpm_dg._find_in_spec("no\nSOMEthing", "(?imx) ^ (something)", "thing"))

    def test_pkg_distgit_matches_commit(self):
        """
        Check the logic for matching a commit from cgit
        """
        flexmock(self.md).should_receive("fetch_cgit_file").once().and_raise(Exception(""))
        self.assertFalse(self.rpm_dg._matches_commit("anything", {}))

        test_file = u"""
            Version: 42
            Release: 201901020304.git.1.12spam7%{?dist}
            %global commit 5p4mm17y5p4m
        """
        flexmock(self.md).should_receive("fetch_cgit_file").and_return(test_file)
        flexmock(self.rpm_dg).should_receive("_built_or_recent").and_return(None)
        self.assertIsNone(self.rpm_dg._matches_commit("12spam78", {}))  # matches, falls through
        self.assertIsNone(self.rpm_dg._matches_commit("5p4mm17y5p4m", {}))  # matches, falls through
        self.assertFalse(self.rpm_dg._matches_commit("11eggs11", {}))  # doesn't match, returns

        self.assertNotIn("No Release: field found", self.stream.getvalue())
        flexmock(self.md).should_receive("fetch_cgit_file").once().and_return("nothing")
        self.assertFalse(self.rpm_dg._matches_commit("nothing", {}))
        self.assertIn("No Release: field found", self.stream.getvalue())

    def test_pkg_build_or_recent(self):
        flexmock(self.rpm_dg).should_receive("release_is_recent").and_return(None)
        self.rpm_dg.name = "mypkg"

        builds = dict(mypkg=("v1", "r1"))
        self.assertTrue(self.rpm_dg._built_or_recent("v1", "r1", builds))

        builds = dict(mypkg=("v1", "r1.el7"))
        self.assertTrue(self.rpm_dg._built_or_recent("v1", "r1%{?dist}", builds))
        self.assertIsNone(self.rpm_dg._built_or_recent("v2", "r1", builds))
        

if __name__ == "__main__":
    unittest.main()
