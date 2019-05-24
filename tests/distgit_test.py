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


class MockDistgit(object):
    def __init__(self):
        self.branch = None


class MockContent(object):
    def __init__(self):
        self.branch = None


class MockConfig(object):

    def __init__(self):
        self.distgit = MockDistgit()
        self.content = Model()
        self.content.source = Model()
        self.content.source.specfile = "test-dummy.spec"


class SimpleMockLock(object):

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockRuntime(object):

    def __init__(self, logger):
        self.branch = None
        self.distgits_dir = "distgits_dir"
        self.logger = logger
        self.mutex = SimpleMockLock()
        self.missing_pkgs = set()

    def detect_remote_source_branch(self, _):
        pass

class MockMetadata(object):

    def __init__(self, runtime):
        self.config = MockConfig()
        self.runtime = runtime
        self.logger = runtime.logger
        self.name = "test"
        self.namespace = "namespace"
        self.distgit_key = "distgit_key"

    def fetch_cgit_file(self, file):
        pass

    def get_component_name(self):
        pass

class MockScanner(object):

    def __init__(self):
        self.matches = []
        self.files = []


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


class TestGenericDistGit(TestDistgit):
    def setUp(self):
        super(TestGenericDistGit, self).setUp()
        self.dg = distgit.DistGitRepo(self.md, autoclone=False)
        self.dg.runtime.group_config = Model()

    def test_init(self):
        """
        Ensure that init creates the object expected
        """
        self.assertIsInstance(self.dg, distgit.DistGitRepo)

    def test_init_branch_override(self):
        metadata = Mock()
        metadata.runtime.branch = "original-branch"

        metadata.config.distgit.branch = distgit.Missing
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("original-branch", repo.branch)

        metadata.config.distgit.branch = "new-branch"
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("new-branch", repo.branch)

    @patch("distgit.DistGitRepo.clone")
    def test_init_with_autoclone(self, clone_mock):
        """
        Mocking `clone` method, since only `init` is what is under test here.
        """
        distgit.DistGitRepo(self.md)
        clone_mock.assert_called_once()

    @patch("distgit.Dir")
    @patch("distgit.os.path.isdir", return_value=True)
    def test_clone_already_cloned(self, _, __):
        metadata = Mock()
        metadata.runtime.local = False
        metadata.logger.info.return_value = None
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected = ("Distgit directory already exists; "
                    "skipping clone: my-root-dir/my-namespace/my-distgit-key")
        metadata.logger.info.assert_called_once_with(expected)

    @patch("distgit.Dir")
    @patch("distgit.os.path.isdir", return_value=False)
    @patch("distgit.os.mkdir")
    def test_clone_fails_to_create_namespace_dir(self, mkdir_mock, _, __):
        metadata = Mock()
        metadata.runtime.local = True

        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["content"] = "..."
        metadata.config.distgit = Mock()

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        try:
            mkdir_mock.side_effect = OSError(errno.EEXIST, "strerror")
            repo.clone("my-root-dir", "my-branch")
        except OSError:
            self.fail("Should not have raised a \"dir already exists\" exception")
        except:
            pass # doesn't matter if something fails at a later point

        mkdir_mock.side_effect = OSError("some other error", "strerror")
        self.assertRaises(OSError, repo.clone, "my-root-dir", "my-branch")

    @patch("distgit.Dir")
    @patch("distgit.os.path.isdir", return_value=False)
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_with_fake_distgit(self, cmd_assert_mock, _, __, ___):
        metadata = Mock()
        metadata.runtime.local = True
        metadata.logger.info.return_value = None
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        metadata.runtime.command = "images:rebase"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["content"] = "..."
        metadata.config.distgit = Mock()

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected_cmd = ["mkdir", "-p", "my-root-dir/my-namespace/my-distgit-key"]
        cmd_assert_mock.assert_called_once_with(expected_cmd)

        expected_log = "Creating local build dir: my-root-dir/my-namespace/my-distgit-key"
        metadata.logger.info.assert_called_once_with(expected_log)

    @patch("distgit.Dir")
    @patch("distgit.os.path.isdir", return_value=False)
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.yellow_print", return_value=None)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_images_build_command(self, cmd_assert_mock, yellow_print_mock, _, __, ___):
        metadata = Mock()
        metadata.runtime.local = False
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        metadata.runtime.command = "images:build"
        metadata.runtime.global_opts = {"rhpkg_clone_timeout": 999}
        metadata.runtime.user = None
        metadata.qualified_name = "my-qualified-name"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected_cmd = (
            "timeout 999 rhpkg clone my-qualified-name "
            "my-root-dir/my-namespace/my-distgit-key --branch my-branch"
        ).split(" ")
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=ANY)
        yellow_print_mock.assert_called_once()

    @patch("distgit.Dir")
    @patch("distgit.os.path.isdir", return_value=False)
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.yellow_print", return_value=None)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_cmd_with_user(self, cmd_assert_mock, _, __, ___, ____):
        metadata = Mock()
        metadata.runtime.local = False
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        metadata.runtime.command = "images:build"
        metadata.runtime.global_opts = {"rhpkg_clone_timeout": 999}
        metadata.runtime.user = "my-user"
        metadata.qualified_name = "my-qualified-name"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected_cmd = (
            "timeout 999 rhpkg --user=my-user clone my-qualified-name "
            "my-root-dir/my-namespace/my-distgit-key --branch my-branch"
        ).split(" ")
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=ANY)

    @patch("distgit.os.path.isfile", return_value=False)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch(self, cmd_assert_mock, _):
        metadata = Mock()
        metadata.logger.info.return_value = None
        metadata.config.distgit.branch = "my-branch"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.merge_branch("my-target")

        expected_cmd_calls = [
            call(["rhpkg", "switch-branch", "my-target"], retries=ANY),
            call(cmd=["git", "merge", "--allow-unrelated-histories", "-m", "Merge branch my-branch into my-target", "my-branch"],
                 on_retry=["git", "reset", "--hard", "my-target"],
                 retries=ANY)
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

        expected_log_calls = [
            call("Switching to branch: my-target"),
            call("Merging source branch history over current branch"),
        ]
        self.assertEqual(expected_log_calls, metadata.logger.info.mock_calls)

    @patch("distgit.os.path.isfile", return_value=False)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch_allow_overwrite(self, cmd_assert_mock, _):
        metadata = Mock()
        metadata.logger.info.return_value = None
        metadata.config.distgit.branch = "my-branch"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.merge_branch("my-target", allow_overwrite=True)

        expected_cmd_calls = [
            call(["rhpkg", "switch-branch", "my-target"], retries=ANY),
            call(cmd=["git", "merge", "--allow-unrelated-histories", "-m", "Merge branch my-branch into my-target", "my-branch"],
                 on_retry=["git", "reset", "--hard", "my-target"],
                 retries=ANY)
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

        expected_log_calls = [
            call("Switching to branch: my-target"),
            call("Merging source branch history over current branch"),
        ]
        self.assertEqual(expected_log_calls, metadata.logger.info.mock_calls)

    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch_dockerfile_or_oit_dir_already_present(self, _, __):
        metadata = Mock()
        metadata.config.distgit.branch = "my-branch"

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        try:
            repo.merge_branch("my-target")
            self.fail()
        except IOError as e:
            expected_msg = ("Unable to continue merge. "
                            "Dockerfile found in target branch. "
                            "Use --allow-overwrite to force.")
            self.assertEqual(expected_msg, e.message)

    @patch("distgit.assertion.isdir", return_value=True)
    def test_source_path(self, _):
        metadata = Mock()
        metadata.runtime.resolve_source.return_value = "source-root"
        metadata.config.content.source.path = "sub-path"
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root/sub-path", repo.source_path())

    @patch("distgit.assertion.isdir", return_value=True)
    def test_source_path_without_sub_path(self, _):
        metadata = Mock()
        metadata.runtime.resolve_source.return_value = "source-root"
        metadata.config.content.source.path = distgit.Missing
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root", repo.source_path())

    @patch("distgit.exectools.cmd_assert", return_value=None)
    def test_commit_local(self, cmd_assert_mock):
        metadata = Mock()
        metadata.runtime.local = True
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("", repo.commit("commit msg"))
        self.assertFalse(cmd_assert_mock.called)

    @patch("distgit.Dir")
    @patch("distgit.exectools.cmd_gather", return_value=(1, "", ""))
    def test_commit_log_diff_failed(self, cmd_gather_mock, _):
        metadata = Mock()
        metadata.runtime.local = False

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        try:
            repo.commit("commit msg", log_diff=True)
            self.fail()
        except IOError as e:
            expected_msg = ("Command returned non-zero exit status: "
                            "Failed fetching distgit diff")
            self.assertEqual(expected_msg, e.message)

    @patch("distgit.Dir")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout", ""))
    def test_commit_log_diff_succeeded(self, cmd_gather_mock, _):
        metadata = Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.commit("commit msg", log_diff=True)
        metadata.runtime.add_distgits_diff.assert_called_once_with("name", "stdout")

    @patch("distgit.Dir")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout-sha", ""))
    @patch("distgit.exectools.cmd_assert")
    def test_commit_with_source_sha(self, cmd_assert_mock, _, __):
        metadata = Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        # @TODO: find out how/when source_sha gets assigned
        repo.source_sha = "my-source-sha"

        self.assertEqual("stdout-sha", repo.commit("commit msg"))

        expected_cmd_calls = [
            call(["git", "add", "-A", "."]),
            call(["git", "commit", "--allow-empty", "-m", "commit msg - my-source-sha\n- MaxFileSize: 50000000"]),
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

    @patch("distgit.Dir")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout-sha", ""))
    @patch("distgit.exectools.cmd_assert")
    def test_commit_without_source_sha(self, cmd_assert_mock, _, __):
        metadata = Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("stdout-sha", repo.commit("commit msg"))

        expected_cmd_calls = [
            call(["git", "add", "-A", "."]),
            call(["git", "commit", "--allow-empty", "-m", "commit msg\n- MaxFileSize: 50000000"]),
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

    @patch("distgit.Dir")
    @patch("distgit.exectools.cmd_gather", return_value=(1, "stdout-sha", ""))
    @patch("distgit.exectools.cmd_assert")
    def test_commit_failed_fetching_sha(self, cmd_assert_mock, _, __):
        metadata = Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.distgit_dir = "my-distgit-dir"

        try:
            repo.commit("commit msg")
            self.fail()
        except IOError as e:
            expected_msg = ("Command returned non-zero exit status: "
                            "Failure fetching commit SHA for my-distgit-dir")
            self.assertEqual(expected_msg, e.message)

    def test_logging(self):
        """
        Ensure that logs work
        """
        msg = "Hey there!"
        self.dg.logger.info(msg)

        actual = self.stream.getvalue()

        self.assertIn(msg, actual)

    def test_add_missing_pkgs_succeed(self):
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.ImageDistGitRepo(md, autoclone=False)
        d._add_missing_pkgs("haproxy")

        self.assertEqual(1, len(d.runtime.missing_pkgs))
        self.assertIn("test image is missing package haproxy", d.runtime.missing_pkgs)

    def test_distgit_is_recent(self):
        scan_freshness = self.dg.runtime.group_config.scan_freshness = Model()
        self.assertFalse(self.dg.release_is_recent("201901020304"))  # not configured

        scan_freshness.release_regex = r'^(....)(..)(..)(..)'
        scan_freshness.threshold_hours = 24
        self.assertFalse(self.dg.release_is_recent("2019"))  # no match by regex

        too_soon = datetime.now() - timedelta(hours=4)
        self.assertTrue(self.dg.release_is_recent(too_soon.strftime('%Y%m%d%H')))
        too_stale = datetime.now() - timedelta(hours=25)
        self.assertFalse(self.dg.release_is_recent(too_stale.strftime('%Y%m%d%H')))


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
