import errno
import unittest
import mock
from datetime import datetime, timedelta

import distgit
from model import Model

from .support import MockMetadata, MockRuntime, TestDistgit


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

    def test_init_with_branch_override(self):
        metadata = mock.Mock()
        metadata.runtime.branch = "original-branch"

        metadata.config.distgit.branch = distgit.Missing
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("original-branch", repo.branch)

        metadata.config.distgit.branch = "new-branch"
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("new-branch", repo.branch)

    @mock.patch("distgit.DistGitRepo.clone")
    def test_init_with_autoclone(self, clone_mock):
        """
        Mocking `clone` method, since only `init` is what is under test here.
        """
        distgit.DistGitRepo(self.md)
        clone_mock.assert_called_once()

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.path.isdir", return_value=True)
    def test_clone_already_cloned(self, *_):
        metadata = mock.Mock()
        metadata.runtime.local = False
        metadata.logger.info.return_value = None
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected = ("Distgit directory already exists; "
                    "skipping clone: my-root-dir/my-namespace/my-distgit-key")
        metadata.logger.info.assert_called_once_with(expected)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.mkdir")
    def test_clone_fails_to_create_namespace_dir(self, mkdir_mock, *__):
        metadata = mock.Mock()
        metadata.runtime.local = True

        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["content"] = "..."
        metadata.config.distgit = mock.Mock()

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        try:
            mkdir_mock.side_effect = OSError(errno.EEXIST, "strerror")
            repo.clone("my-root-dir", "my-branch")
        except OSError:
            self.fail("Should not have raised a \"dir already exists\" exception")
        except:
            pass  # doesn't matter if something fails at a later point

        mkdir_mock.side_effect = OSError("some other error", "strerror")
        self.assertRaises(OSError, repo.clone, "my-root-dir", "my-branch")

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_with_fake_distgit(self, cmd_assert_mock, *_):
        metadata = mock.Mock()
        metadata.runtime.local = True
        metadata.logger.info.return_value = None
        metadata.namespace = "my-namespace"
        metadata.distgit_key = "my-distgit-key"

        metadata.runtime.command = "images:rebase"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["content"] = "..."
        metadata.config.distgit = mock.Mock()

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.clone("my-root-dir", "my-branch")

        expected_cmd = ["mkdir", "-p", "my-root-dir/my-namespace/my-distgit-key"]
        cmd_assert_mock.assert_called_once_with(expected_cmd)

        expected_log = "Creating local build dir: my-root-dir/my-namespace/my-distgit-key"
        metadata.logger.info.assert_called_once_with(expected_log)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.yellow_print", return_value=None)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_images_build_command(self, cmd_assert_mock, yellow_print_mock, *_):
        metadata = mock.Mock()
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
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=mock.ANY)
        yellow_print_mock.assert_called_once()

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.yellow_print", return_value=None)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_clone_cmd_with_user(self, cmd_assert_mock, *_):
        metadata = mock.Mock()
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
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=mock.ANY)

    @mock.patch("distgit.os.path.isfile", return_value=False)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch(self, cmd_assert_mock, _):
        metadata = mock.Mock()
        metadata.logger.info.return_value = None
        metadata.config.distgit.branch = "my-branch"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.merge_branch("my-target")

        expected_cmd_calls = [
            mock.call(["rhpkg", "switch-branch", "my-target"], retries=mock.ANY),
            mock.call(cmd=["git", "merge", "--allow-unrelated-histories", "-m", "Merge branch my-branch into my-target", "my-branch"],
                      on_retry=["git", "reset", "--hard", "my-target"],
                      retries=mock.ANY)
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

        expected_log_calls = [
            mock.call("Switching to branch: my-target"),
            mock.call("Merging source branch history over current branch"),
        ]
        self.assertEqual(expected_log_calls, metadata.logger.info.mock_calls)

    @mock.patch("distgit.os.path.isfile", return_value=False)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch_allow_overwrite(self, cmd_assert_mock, _):
        metadata = mock.Mock()
        metadata.logger.info.return_value = None
        metadata.config.distgit.branch = "my-branch"

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.merge_branch("my-target", allow_overwrite=True)

        expected_cmd_calls = [
            mock.call(["rhpkg", "switch-branch", "my-target"], retries=mock.ANY),
            mock.call(cmd=["git", "merge", "--allow-unrelated-histories", "-m", "Merge branch my-branch into my-target", "my-branch"],
                      on_retry=["git", "reset", "--hard", "my-target"],
                      retries=mock.ANY)
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

        expected_log_calls = [
            mock.call("Switching to branch: my-target"),
            mock.call("Merging source branch history over current branch"),
        ]
        self.assertEqual(expected_log_calls, metadata.logger.info.mock_calls)

    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_merge_branch_dockerfile_or_oit_dir_already_present(self, *_):
        metadata = mock.Mock()
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

    @mock.patch("distgit.assertion.isdir", return_value=True)
    def test_source_path(self, _):
        metadata = mock.Mock()
        metadata.runtime.resolve_source.return_value = "source-root"
        metadata.config.content.source.path = "sub-path"
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root/sub-path", repo.source_path())

    @mock.patch("distgit.assertion.isdir", return_value=True)
    def test_source_path_without_sub_path(self, _):
        metadata = mock.Mock()
        metadata.runtime.resolve_source.return_value = "source-root"
        metadata.config.content.source.path = distgit.Missing
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root", repo.source_path())

    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_commit_local(self, cmd_assert_mock):
        metadata = mock.Mock()
        metadata.runtime.local = True
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("", repo.commit("commit msg"))
        self.assertFalse(cmd_assert_mock.called)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=(1, "", ""))
    def test_commit_log_diff_failed(self, cmd_gather_mock, _):
        metadata = mock.Mock()
        metadata.runtime.local = False

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        try:
            repo.commit("commit msg", log_diff=True)
            self.fail()
        except IOError as e:
            expected_msg = ("Command returned non-zero exit status: "
                            "Failed fetching distgit diff")
            self.assertEqual(expected_msg, e.message)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "stdout", ""))
    def test_commit_log_diff_succeeded(self, cmd_gather_mock, _):
        metadata = mock.Mock()
        metadata.distgit_key = "distgit_key"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.commit("commit msg", log_diff=True)
        metadata.runtime.add_distgits_diff.assert_called_once_with("distgit_key", "stdout")

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "stdout-sha", ""))
    @mock.patch("distgit.exectools.cmd_assert")
    def test_commit_with_source_sha(self, cmd_assert_mock, *_):
        metadata = mock.Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        # @TODO: find out how/when source_sha gets assigned
        repo.source_sha = "my-source-sha"

        self.assertEqual("stdout-sha", repo.commit("commit msg"))

        expected_cmd_calls = [
            mock.call(["git", "add", "-A", "."]),
            mock.call(["git", "commit", "--allow-empty", "-m", "commit msg - my-source-sha\n- MaxFileSize: 50000000"]),
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "stdout-sha", ""))
    @mock.patch("distgit.exectools.cmd_assert")
    def test_commit_without_source_sha(self, cmd_assert_mock, *_):
        metadata = mock.Mock()
        metadata.name = "name"
        metadata.runtime.local = False
        metadata.runtime.add_distgits_diff.return_value = None

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("stdout-sha", repo.commit("commit msg"))

        expected_cmd_calls = [
            mock.call(["git", "add", "-A", "."]),
            mock.call(["git", "commit", "--allow-empty", "-m", "commit msg\n- MaxFileSize: 50000000"]),
        ]
        self.assertEqual(expected_cmd_calls, cmd_assert_mock.mock_calls)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.exectools.cmd_gather", return_value=(1, "stdout-sha", ""))
    @mock.patch("distgit.exectools.cmd_assert")
    def test_commit_failed_fetching_sha(self, cmd_assert_mock, *_):
        metadata = mock.Mock()
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
        self.assertIn("distgit_key image is missing package haproxy", d.runtime.missing_pkgs)

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


if __name__ == "__main__":
    unittest.main()
