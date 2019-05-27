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


class TestImageDistGit(TestDistgit):
    def setUp(self):
        super(TestImageDistGit, self).setUp()
        self.img_dg = distgit.ImageDistGitRepo(self.md, autoclone=False)
        self.img_dg.runtime.group_config = Model()

    def test_detect_permanent_build_failures(self):
        self.img_dg._logs_dir = lambda: self.logs_dir
        task_dir = tempfile.mkdtemp(dir=self.logs_dir)
        with open(task_dir + "/x86_64.log", "w") as file:
            msg1 = "No package fubar available"
            file.write(msg1)
        with open(task_dir + "/task_failed.log", "w") as file:
            msg2 = "Failed component comparison for components: fubar"
            file.write(msg2)

        scanner = MockScanner()
        scanner.matches = [
            # note: we read log lines, so these can only match in a single line
            re.compile("No package .* available"),
            re.compile("Failed component comparison for components:.*"),
        ]
        scanner.files = [ "x86_64.log", "task_failed.log" ]
        fails = self.img_dg._detect_permanent_build_failures(scanner)
        self.assertIn(msg1, fails)
        self.assertIn(msg2, fails)

    def test_detect_permanent_build_failures_borkage(self):
        scanner = MockScanner()
        scanner.matches = []
        scanner.files = [ "x86_64.log", "task_failed.log" ]

        self.assertIsNone(self.img_dg._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("Log file scanning not specified", output)

        scanner.matches.append(None)

        self.img_dg._logs_dir = lambda: self.logs_dir  # search an empty log dir
        self.assertIsNone(self.img_dg._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("No task logs found under", output)

        self.img_dg._logs_dir = lambda: str(tempfile.TemporaryFile())  # search a dir that won't be there
        self.assertIsNone(self.img_dg._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("Exception while trying to analyze build logs", output)

    def test_mangle_yum_cmds_unchanged(self):
        unchanging = [
            "yum install foo",
            "ignore yum-config-manager here",
            "foo && yum install && bar",
        ]
        for cmd in unchanging:
            self.assertFalse(distgit.ImageDistGitRepo._mangle_yum(cmd)[0])

    def test_mangle_yum_cmds_changed(self):
        # note: adjacent spaces are not removed, so removals may result in redundant spaces
        changes = {
            "yum-config-manager foo bar baz": ": 'removed yum-config-manager'",
            "yum --enablerepo=bar install foo --disablerepo baz": "yum  install foo  ",

            "yum-config-manager foo bar baz && yum --enablerepo=bar install foo && build stuff":
                ": 'removed yum-config-manager' \\\n && yum  install foo \\\n && build stuff",

            """yum repolist && yum-config-manager\
               --enable blah\
               && yum install 'foo < 42' >& whatever\
               && build some stuff""":
            """yum repolist \\\n && : 'removed yum-config-manager'\
               \\\n && yum install 'foo < 42' >& whatever\
               \\\n && build some stuff""",

            """yum repolist && \
               ( foo || yum-config-manager --enable blah && verify-something )""":
            """yum repolist \\\n && \
               ( foo \\\n || : 'removed yum-config-manager' \\\n && verify-something )"""
        }
        for cmd, expect in changes.items():
            changed, result = distgit.ImageDistGitRepo._mangle_yum(cmd)
            self.assertTrue(changed)
            self.assertEquals(result, expect)

    def test_mangle_yum_parse_err(self):
        with self.assertRaises(IOError) as e:
            distgit.ImageDistGitRepo._mangle_yum("! ( && || $ ) totally broken")
        self.assertIn("totally broken", str(e.exception))

    def test_image_distgit_matches_commit(self):
        """
        Check the logic for matching a commit from cgit
        """
        test_file = u"""
            from foo
            label "{}"="spam"
        """.format(distgit.ImageDistGitRepo.source_labels['now']['sha'])
        flexmock(self.md).should_receive("fetch_cgit_file").and_return(test_file)
        flexmock(self.img_dg).should_receive("_built_or_recent").and_return(None)  # hit this on match

        self.assertIsNone(self.img_dg._matches_commit("spam", {}))  # commit matches, falls through
        self.assertNotIn("ERROR", self.stream.getvalue())
        self.assertFalse(self.img_dg._matches_commit("eggs", {}))

        flexmock(self.md).should_receive("fetch_cgit_file").and_raise(Exception("bogus!!"))
        self.assertFalse(self.img_dg._matches_commit("bacon", {}))
        self.assertIn("bogus", self.stream.getvalue())

    def test_image_distgit_matches_source(self):
        """
        Check the logic for matching a commit from source repo
        """
        self.img_dg.config.content = Model()

        # no source, dist-git only; should go on to check if built/stale
        flexmock(self.img_dg).should_receive("_built_or_recent").once().and_return(None)
        self.assertIsNone(self.img_dg.matches_source_commit({}))

        # source specified and matches Dockerfile in dist-git
        self.img_dg.config.content.source = Model()
        self.img_dg.config.content.source.git = dict()
        test_file = u"""
            from foo
            label "{}"="spam"
        """.format(distgit.ImageDistGitRepo.source_labels['now']['sha'])
        flexmock(self.md).should_receive("fetch_cgit_file").and_return(test_file)
        flexmock(self.img_dg).should_receive("_built_or_recent").and_return(None)  # hit this on match
        flexmock(self.img_dg.runtime).should_receive("detect_remote_source_branch").and_return(("branch", "spam"))
        self.assertIsNone(self.img_dg.matches_source_commit({}))  # matches, falls through

        # source specified and doesn't match Dockerfile in dist-git
        flexmock(self.img_dg.runtime).should_receive("detect_remote_source_branch").and_return(("branch", "eggs"))
        self.assertFalse(self.img_dg.matches_source_commit({}))

    def test_img_build_or_recent(self):
        flexmock(self.img_dg).should_receive("release_is_recent").and_return(None)
        self.img_dg.name = "spam-a-lot"
        flexmock(self.img_dg.metadata).should_receive("get_component_name").and_return("spam-a-lot-container")

        builds = {"spam-a-lot-container": ("v1", "r1")}
        self.assertTrue(self.img_dg._built_or_recent("v1", "r1", builds))
        self.assertIsNone(self.img_dg._built_or_recent("v2", "r1", builds))


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
