
import errno
import os
import unittest

from flexmock import flexmock
from unittest import mock

from doozerlib import distgit, model
from doozerlib.assembly import AssemblyTypes

from .support import MockConfig, MockMetadata, MockRuntime, TestDistgit


class TestGenericDistGit(TestDistgit):
    def setUp(self):
        super(TestGenericDistGit, self).setUp()
        self.dg = distgit.DistGitRepo(self.md, autoclone=False)
        self.dg.runtime.group_config = model.Model()

    @staticmethod
    def mock_runtime(**kwargs):
        def flexmock_defaults(**inner_kwargs):
            params = dict(**kwargs)
            for k, v in inner_kwargs.items():
                if k not in params:
                    params[k] = v
            return flexmock(**params)

        # Pass in a set of defaults to flexmock, but allow caller to override
        # anything they want.
        return flexmock_defaults(
            group_config=flexmock(
                urls=flexmock(brew_image_host="brew-img-host", brew_image_namespace="brew-img-ns"),
                insecure_source=False,
            ),
            resolve_brew_image_url=lambda *_, **__: '',
            working_dir="my-working-dir",
            branch="some-branch",
            command="some-command",
            add_record=lambda *_, **__: None,
            assembly_type=AssemblyTypes.STANDARD,
        )

    def test_init(self):
        """
        Ensure that init creates the object expected
        """
        self.assertIsInstance(self.dg, distgit.DistGitRepo)

    def test_init_with_branch_override(self):
        metadata = flexmock(runtime=self.mock_runtime(branch="original-branch"),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing)),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("original-branch", repo.branch)

        metadata.config.distgit.branch = "new-branch"
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("new-branch", repo.branch)

    def test_init_with_autoclone(self):
        flexmock(distgit.DistGitRepo).should_receive("clone").once()
        distgit.DistGitRepo(self.md)

    def test_clone_already_cloned(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # pretenting the directory exists (already cloned)
        flexmock(distgit.os.path).should_receive("isdir").and_return(True)

        expected_log_msg = ("Distgit directory already exists; "
                            "skipping clone: my-root-dir/my-namespace/my-distgit-key")
        logger = flexmock()
        logger.should_receive("info").with_args(expected_log_msg).once()

        metadata = flexmock(namespace="my-namespace",
                            distgit_key="my-distgit-key",
                            runtime=self.mock_runtime(local=False, branch="_irrelevant_", upcycle=False),
                            config=MockConfig(),
                            logger=logger,
                            prevent_cloning=False,
                            name="_irrelevant_")

        expected_cmd = ['git', '-C', 'my-root-dir/my-namespace/my-distgit-key', 'rev-parse', 'HEAD']
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, strip=True)
         .and_return("abcdefg", "")
         .once())

        distgit.DistGitRepo(metadata, autoclone=False).clone("my-root-dir", "my-branch")

    def test_clone_fails_to_create_namespace_dir(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # pretenting the directory doesn't exist (not yet cloned)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        metadata = flexmock(config=MockConfig(),
                            runtime=self.mock_runtime(local=True,
                                                      branch="_irrelevant_",
                                                      command="_irrelevant_",
                                                      rhpkg_config_lst=[]),
                            name="_irrelevant_",
                            logger="_irrelevant_",
                            namespace="_irrelevant_",
                            prevent_cloning=False,
                            distgit_key="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        # simulating a "File exists" error
        (flexmock(distgit.os)
         .should_receive("mkdir")
         .and_raise(OSError(errno.EEXIST, os.strerror(errno.EEXIST))))

        try:
            repo.clone("my-root-dir", "my-branch")
        except OSError:
            self.fail("Should not have raised a \"dir already exists\" exception")
        except:
            pass  # doesn't matter if something fails at a later point

        # simulating any other OSError
        (flexmock(distgit.os)
         .should_receive("mkdir")
         .and_raise(OSError))

        self.assertRaises(OSError, repo.clone, "my-root-dir", "my-branch")

    def test_clone_with_fake_distgit(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))
        flexmock(distgit.os).should_receive("mkdir").replace_with(lambda _: None)

        # pretenting the directory doesn't exist (not yet cloned)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        expected_log_msg = ("Creating local build dir: "
                            "my-root-dir/my-namespace/my-distgit-key")
        logger = flexmock()
        logger.should_receive("info").with_args(expected_log_msg).once()

        expected_cmd = ["mkdir", "-p", "my-root-dir/my-namespace/my-distgit-key"]
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd)
         .once())

        expected_cmd = ['git', '-C', 'my-root-dir/my-namespace/my-distgit-key', 'rev-parse', 'HEAD']
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, strip=True)
         .and_return("abcdefg", "")
         .once())

        metadata = flexmock(config=MockConfig(content="_irrelevant_"),
                            runtime=self.mock_runtime(local=True,
                                                      command="images:rebase",
                                                      branch="_irrelevant_",
                                                      rhpkg_config_lst=[]),
                            namespace="my-namespace",
                            distgit_key="my-distgit-key",
                            prevent_cloning=False,
                            logger=logger,
                            name="_irrelevant_")

        distgit.DistGitRepo(metadata, autoclone=False).clone("my-root-dir", "my-branch")

    def test_clone_images_build_command(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))
        flexmock(distgit.os).should_receive("mkdir").replace_with(lambda _: None)

        # pretenting the directory doesn't exist (not yet cloned)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        expected_log_msg = ("Cloning distgit repository [branch:my-branch] "
                            "into: my-root-dir/my-namespace/my-distgit-key")
        logger = flexmock()
        logger.should_receive("info").with_args(expected_log_msg).once()

        expected_cmd = [
            "timeout", "999", "rhpkg", "clone", "my-qualified-name",
            "my-root-dir/my-namespace/my-distgit-key", "--branch", "my-branch"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, retries=3, set_env=object)
         .once()
         .and_return(None))

        expected_cmd = ['git', '-C', 'my-root-dir/my-namespace/my-distgit-key', 'rev-parse', 'HEAD']
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, strip=True)
         .and_return("abcdefg", "")
         .once())

        expected_warning = ("Warning: images:rebase was skipped and "
                            "therefore your local build will be sourced "
                            "from the current dist-git contents and not "
                            "the typical GitHub source. ")

        (flexmock(distgit)
         .should_receive("yellow_print")
         .with_args(expected_warning)
         .once())

        metadata = flexmock(config=MockConfig(content="_irrelevant_"),
                            runtime=self.mock_runtime(local=False,
                                                      command="images:build",
                                                      global_opts={"rhpkg_clone_timeout": 999},
                                                      user=None,
                                                      branch="_irrelevant_",
                                                      rhpkg_config_lst=[],
                                                      downstream_commitish_overrides={}),
                            namespace="my-namespace",
                            distgit_key="my-distgit-key",
                            qualified_name="my-qualified-name",
                            prevent_cloning=False,
                            logger=logger,
                            name="_irrelevant_")

        distgit.DistGitRepo(metadata, autoclone=False).clone("my-root-dir", "my-branch")

    def test_clone_cmd_with_user(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))
        flexmock(distgit.os).should_receive("mkdir").replace_with(lambda _: None)

        # pretenting the directory doesn't exist (not yet cloned)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        # avoid warning print in the middle of the test progress report
        flexmock(distgit).should_receive("yellow_print").replace_with(lambda _: None)

        expected_cmd = [
            "timeout", "999", "rhpkg", "--user=my-user", "clone", "my-qualified-name",
            "my-root-dir/my-namespace/my-distgit-key", "--branch", "my-branch"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, retries=3, set_env=object)
         .once()
         .and_return(None))

        expected_cmd = ['git', '-C', 'my-root-dir/my-namespace/my-distgit-key', 'rev-parse', 'HEAD']
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_cmd, strip=True)
         .and_return("abcdefg", "")
         .once())

        metadata = flexmock(config=MockConfig(content="_irrelevant_"),
                            runtime=self.mock_runtime(local=False,
                                                      command="images:build",
                                                      global_opts={"rhpkg_clone_timeout": 999},
                                                      user="my-user",
                                                      branch="_irrelevant_",
                                                      rhpkg_config_lst=[],
                                                      downstream_commitish_overrides={}),
                            namespace="my-namespace",
                            distgit_key="my-distgit-key",
                            qualified_name="my-qualified-name",
                            logger=flexmock(info=lambda _: None),
                            prevent_cloning=False,
                            name="_irrelevant_", )

        distgit.DistGitRepo(metadata, autoclone=False).clone("my-root-dir", "my-branch")

    def test_merge_branch(self):
        # pretenting there is no Dockerfile nor .oit directory
        flexmock(distgit.os.path).should_receive("isfile").and_return(False)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        expected_1st_log_msg = "Switching to branch: my-target"

        logger = flexmock()
        logger.should_receive("info").with_args(expected_1st_log_msg).once().ordered()

        expected_1st_cmd = ["rhpkg", "switch-branch", "my-target"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_1st_cmd, retries=3)
         .once()
         .ordered())

        expected_2nd_log_msg = "Merging source branch history over current branch"
        logger.should_receive("info").with_args(expected_2nd_log_msg).once().ordered()

        expected_2nd_cmd = [
            "git", "merge", "--allow-unrelated-histories", "-m",
            "Merge branch my-branch into my-target", "my-branch"]

        expected_on_retry = ["git", "reset", "--hard", "my-target"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_2nd_cmd, retries=3, on_retry=expected_on_retry)
         .once()
         .ordered())

        metadata = flexmock(config=flexmock(distgit=flexmock(branch="my-branch")),
                            logger=logger,
                            runtime=self.mock_runtime(branch="_irrelevant_",
                                                      rhpkg_config_lst=[]),
                            name="_irrelevant_")

        distgit.DistGitRepo(metadata, autoclone=False).merge_branch("my-target")

    def test_merge_branch_allow_overwrite(self):
        # pretenting there is no Dockerfile nor .oit directory
        flexmock(distgit.os.path).should_receive("isfile").and_return(False)
        flexmock(distgit.os.path).should_receive("isdir").and_return(False)

        expected_1st_log_msg = "Switching to branch: my-target"

        logger = flexmock()
        logger.should_receive("info").with_args(expected_1st_log_msg).once().ordered()

        expected_1st_cmd = ["rhpkg", "switch-branch", "my-target"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_1st_cmd, retries=3)
         .once()
         .ordered())

        expected_2nd_log_msg = "Merging source branch history over current branch"
        logger.should_receive("info").with_args(expected_2nd_log_msg).once().ordered()

        expected_2nd_cmd = [
            "git", "merge", "--allow-unrelated-histories", "-m",
            "Merge branch my-branch into my-target", "my-branch"]

        expected_on_retry = ["git", "reset", "--hard", "my-target"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_2nd_cmd, retries=3, on_retry=expected_on_retry)
         .once()
         .ordered())

        metadata = flexmock(config=flexmock(distgit=flexmock(branch="my-branch")),
                            logger=logger,
                            runtime=self.mock_runtime(branch="_irrelevant_",
                                                      rhpkg_config_lst=[]),
                            name="_irrelevant_")

        (distgit.DistGitRepo(metadata, autoclone=False)
         .merge_branch("my-target", allow_overwrite=True))

    def test_merge_branch_dockerfile_or_oit_dir_already_present(self):
        # pretenting there is a Dockerfile present
        flexmock(distgit.os.path).should_receive("isfile").and_return(True)

        # avoid actually executing any command
        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .replace_with(lambda *_, **__: None))

        metadata = flexmock(config=flexmock(distgit=flexmock(branch="my-branch")),
                            runtime=self.mock_runtime(branch="_irrelevant_",
                                                      rhpkg_config_lst=[]),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        try:
            repo.merge_branch("my-target")
            self.fail()
        except IOError as e:
            expected_msg = ("Unable to continue merge. "
                            "Dockerfile found in target branch. "
                            "Use --allow-overwrite to force.")
            self.assertEqual(expected_msg, str(e))

    def test_source_path(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))
        flexmock(distgit.os.path).should_receive("isdir").and_return(True)

        metadata = flexmock(runtime=self.mock_runtime(resolve_source=lambda *_: "source-root",
                                                      branch="_irrelevant_"),
                            config=flexmock(content=flexmock(source=flexmock(path="sub-path")),
                                            distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            config_filename="_irrelevant_",
                            name="_irrelevant_")
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root/sub-path", repo.source_path())

    def test_source_path_without_sub_path(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))
        flexmock(distgit.os.path).should_receive("isdir").and_return(True)

        metadata = flexmock(runtime=self.mock_runtime(resolve_source=lambda *_: "source-root",
                                                      branch="_irrelevant_"),
                            config=flexmock(content=flexmock(source=flexmock(path=distgit.Missing)),
                                            distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            config_filename="_irrelevant_",
                            name="_irrelevant_")
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("source-root", repo.source_path())

    def test_commit_local(self):
        flexmock(distgit.exectools).should_receive("cmd_assert").times(0)

        metadata = flexmock(runtime=self.mock_runtime(local=True, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("", repo.commit("commit msg"))

    def test_commit_log_diff_failed(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # simulating a failure when running "git diff Dockerfile"
        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .with_args(["git", "diff", "Dockerfile"])
         .and_return((1, "stdout", "stderr")))

        metadata = flexmock(runtime=self.mock_runtime(local=False, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")
        (flexmock(distgit.DistGitRepo).should_receive("_get_diff").once().and_raise(ChildProcessError, "Command returned non-zero exit status: Failed fetching distgit diff"))
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        try:
            repo.commit("commit msg", log_diff=True)
            self.fail()
        except ChildProcessError as e:
            expected_msg = ("Command returned non-zero exit status: "
                            "Failed fetching distgit diff")
            self.assertEqual(expected_msg, str(e))

    def test_commit_log_diff_succeeded(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # avoid actually executing any command
        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .and_return((0, "stdout", "stderr")))

        metadata = flexmock(distgit_key="my-distgit-key",
                            runtime=self.mock_runtime(local=False, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")

        (flexmock(metadata.runtime)
         .should_receive("add_distgits_diff")
         .with_args("my-distgit-key", "stdout")
         .once()
         .and_return(None))

        (flexmock(distgit.DistGitRepo).should_receive("_get_diff").once().and_return("stdout"))

        distgit.DistGitRepo(metadata, autoclone=False).commit("commit msg", log_diff=True)

    def test_commit_with_source_sha(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        expected_1st_cmd = ["git", "add", "-A", "."]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_1st_cmd)
         .once()
         .ordered())

        expected_2nd_cmd = [
            "git", "commit", "--allow-empty", "-m",
            "# commit msg\nMaxFileSize: 104857600\njenkins.url: null\n"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_2nd_cmd)
         .once()
         .ordered())

        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .with_args(["git", "rev-parse", "HEAD"])
         .and_return((0, "sha-from-stdout", "")))

        metadata = flexmock(distgit_key="my-distgit-key",
                            runtime=self.mock_runtime(local=False,
                                                      branch="_irrelevant_",
                                                      add_distgits_diff=lambda: None),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)

        # @TODO: find out how/when source_sha gets assigned
        repo.source_sha = "my-source-sha"

        self.assertEqual("sha-from-stdout", repo.commit("commit msg"))

    def test_commit_without_source_sha(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        expected_1st_cmd = ["git", "add", "-A", "."]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_1st_cmd)
         .once()
         .ordered())

        expected_2nd_cmd = [
            "git", "commit", "--allow-empty", "-m",
            "# commit msg\nMaxFileSize: 104857600\njenkins.url: null\n"]

        (flexmock(distgit.exectools)
         .should_receive("cmd_assert")
         .with_args(expected_2nd_cmd)
         .once()
         .ordered())

        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .with_args(["git", "rev-parse", "HEAD"])
         .and_return((0, "sha-from-stdout", "")))

        metadata = flexmock(distgit_key="my-distgit-key",
                            runtime=self.mock_runtime(local=False,
                                                      branch="_irrelevant_",
                                                      add_distgits_diff=lambda: None),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("sha-from-stdout", repo.commit("commit msg"))

    def test_commit_failed_fetching_sha(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # simulating a failure when fetching the commit sha
        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .and_return((0, "", ""))  # git add
         .and_return((0, "", ""))  # git commit
         .and_return((1, "", "")))  # git rev-parse

        metadata = flexmock(distgit_key="my-distgit-key",
                            runtime=self.mock_runtime(local=False,
                                                      branch="_irrelevant_",
                                                      add_distgits_diff=lambda: None),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")

        repo = distgit.DistGitRepo(metadata, autoclone=False)
        repo.distgit_dir = "my-distgit-dir"

        try:
            repo.commit("commit msg")
            self.fail()
        except IOError as e:
            expected_msg = ("Command returned non-zero exit status: "
                            "Failure fetching commit SHA for my-distgit-dir")
            self.assertEqual(expected_msg, str(e))

    def test_tag_local(self):
        flexmock(distgit.exectools).should_receive("cmd_gather").times(0)

        metadata = flexmock(runtime=self.mock_runtime(local=True, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertEqual("", repo.tag("my-version", "my-release"))

    def test_tag_no_version(self):
        flexmock(distgit.exectools).should_receive("cmd_gather").times(0)

        metadata = flexmock(runtime=self.mock_runtime(local=False, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=flexmock(info=lambda _: None),
                            name="_irrelevant_")
        repo = distgit.DistGitRepo(metadata, autoclone=False)

        self.assertIsNone(repo.tag(None, "my-release"))

    def test_tag_no_release(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .with_args(["git", "tag", "-f", "my-version", "-m", "my-version"])
         .once()
         .and_return(None))

        expected_log_msg = "Adding tag to local repo: my-version"

        logger = flexmock()
        logger.should_receive("info").with_args(expected_log_msg).once()

        metadata = flexmock(runtime=self.mock_runtime(local=False, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=logger,
                            name="_irrelevant_")
        distgit.DistGitRepo(metadata, autoclone=False).tag("my-version", None)

    def test_tag_with_release(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        (flexmock(distgit.exectools)
         .should_receive("cmd_gather")
         .with_args(["git", "tag", "-f", "my-version-my-release", "-m", "my-version-my-release"])
         .once()
         .and_return(None))

        expected_log_msg = "Adding tag to local repo: my-version-my-release"

        logger = flexmock()
        logger.should_receive("info").with_args(expected_log_msg).once()

        metadata = flexmock(runtime=self.mock_runtime(local=False, branch="_irrelevant_"),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            logger=logger,
                            name="_irrelevant_")
        distgit.DistGitRepo(metadata, autoclone=False).tag("my-version", "my-release")

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

    @mock.patch("requests.head")
    def test_cgit_file_available(self, mocked_head):
        meta = MockMetadata(MockRuntime(self.logger))
        cgit_url = "http://distgit.example.com/cgit/containers/foo/plain/some_path/some_file.txt?h=some-branch&id=abcdefg"
        meta.cgit_file_url = lambda *args, **kwargs: cgit_url
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.sha = "abcdefg"

        mocked_head.return_value.status_code = 404
        existence, url = dg.cgit_file_available("some_path/some_file.txt")
        self.assertEqual(url, cgit_url)
        self.assertFalse(existence)

        mocked_head.return_value.status_code = 200
        existence, url = dg.cgit_file_available("some_path/some_file.txt")
        self.assertEqual(url, cgit_url)
        self.assertTrue(existence)

        mocked_head.return_value.status_code = 500
        mocked_head.return_value.raise_for_status.side_effect = IOError
        with self.assertRaises(IOError):
            dg.cgit_file_available("some_path/some_file.txt")


if __name__ == "__main__":
    unittest.main()
