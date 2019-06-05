import re
import tempfile
import unittest

import flexmock
import mock

import distgit
from model import Model

from ..support import MockScanner, TestDistgit


class TestImageDistGit(TestDistgit):
    def setUp(self):
        super(TestImageDistGit, self).setUp()
        self.img_dg = distgit.ImageDistGitRepo(self.md, autoclone=False)
        self.img_dg.runtime.group_config = Model()

    @mock.patch("distgit.DistGitRepo.clone", return_value=None)
    def test_clone_invokes_read_master_data(self, *_):
        """
        Mocking `clone` method of parent class, since we are only interested
        in validating that `_read_master_data` is called in the child class.
        """
        repo = distgit.ImageDistGitRepo(mock.Mock(), autoclone=False)
        repo._read_master_data = mock.Mock()
        repo.clone("distgits_root_dir", "distgit_branch")

        repo._read_master_data.assert_called_once()

    def test_image_build_method_default(self):
        metadata = mock.Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config.distgit = mock.Mock()
        metadata.config.image_build_method = distgit.Missing

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("default-method", repo.image_build_method)

    def test_image_build_method_imagebuilder(self):
        metadata = mock.Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["from"] = {"builder": "..."}
        metadata.config.distgit = mock.Mock()
        metadata.config.image_build_method = distgit.Missing

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("imagebuilder", repo.image_build_method)

    def test_image_build_method_from_config(self):
        metadata = mock.Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config.distgit = mock.Mock()
        metadata.config.image_build_method = "config-method"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("config-method", repo.image_build_method)

    def test_wait_for_build_with_build_status_true(self):
        metadata = mock.Mock()
        metadata.qualified_name = "my-qualified-name"
        info_log = mock.Mock(return_value=None)
        metadata.logger.info = info_log

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        repo.build_lock = mock.MagicMock()  # we don't want tests to be blocked
        repo.build_status = True

        repo.wait_for_build("i-am-waiting")

        expected_info_log_calls = [
            mock.call("Member waiting for me to build: i-am-waiting"),
            mock.call("Member successfully waited for me to build: i-am-waiting"),
        ]
        self.assertEqual(expected_info_log_calls, info_log.mock_calls)

    def test_wait_for_build_with_build_status_false(self):
        metadata = mock.Mock()
        metadata.qualified_name = "my-qualified-name"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        repo.build_lock = mock.MagicMock()  # we don't want tests to be blocked
        repo.build_status = False

        try:
            repo.wait_for_build("i-am-waiting")
            self.fail("Should have raised IOError")
        except IOError as e:
            expected = "Error building image: my-qualified-name (i-am-waiting was waiting)"
            actual = e.message
            self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=None)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_push(self, cmd_assert_mock, cmd_gather_mock, *_):
        metadata = mock.Mock()
        metadata.runtime.global_opts = {"rhpkg_push_timeout": 999}

        expected = (metadata, True)
        actual = distgit.ImageDistGitRepo(metadata, autoclone=False).push()

        self.assertEqual(expected, actual)
        cmd_assert_mock.assert_called_once_with("timeout 999 rhpkg push", retries=mock.ANY)
        cmd_gather_mock.assert_called_once_with(["timeout", "60", "git", "push", "--tags"])

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=None)
    @mock.patch("distgit.exectools.cmd_assert", side_effect=IOError("io-error"))
    def test_push_with_io_error(self, cmd_assert_mock, cmd_gather_mock, *_):
        metadata = mock.Mock()
        metadata.runtime.global_opts = {"rhpkg_push_timeout": mock.ANY}

        expected = (metadata, "IOError('io-error',)")
        actual = distgit.ImageDistGitRepo(metadata, autoclone=False).push()

        self.assertEqual(expected, actual)

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
        scanner.files = ["x86_64.log", "task_failed.log"]
        fails = self.img_dg._detect_permanent_build_failures(scanner)
        self.assertIn(msg1, fails)
        self.assertIn(msg2, fails)

    def test_detect_permanent_build_failures_borkage(self):
        scanner = MockScanner()
        scanner.matches = []
        scanner.files = ["x86_64.log", "task_failed.log"]

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

    @unittest.skip("raising IOError: [Errno 2] No such file or directory: '/tmp/ocp-cd-test-logsMpNctA/test_file'")
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

    @unittest.skip("raising IOError: [Errno 2] No such file or directory: u'./Dockerfile'")
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


if __name__ == "__main__":
    unittest.main()
