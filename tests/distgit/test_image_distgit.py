import tempfile
import re

import flexmock
from mock import Mock, patch
from .test_distgit import TestDistgit
from .mocks import *

import distgit
from model import Model


class TestImageDistGit(TestDistgit):
    def setUp(self):
        super(TestImageDistGit, self).setUp()
        self.img_dg = distgit.ImageDistGitRepo(self.md, autoclone=False)
        self.img_dg.runtime.group_config = Model()

    def test_image_build_method_default(self):
        metadata = Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config.distgit = Mock()
        metadata.config.image_build_method = distgit.Missing

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("default-method", repo.image_build_method)

    def test_image_build_method_imagebuilder(self):
        metadata = Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config["from"] = {"builder": "..."}
        metadata.config.distgit = Mock()
        metadata.config.image_build_method = distgit.Missing

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("imagebuilder", repo.image_build_method)

    def test_image_build_method_from_config(self):
        metadata = Mock()
        metadata.runtime.group_config.default_image_build_method = "default-method"
        metadata.config = type("MyConfig", (dict,), {})()
        metadata.config.distgit = Mock()
        metadata.config.image_build_method = "config-method"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("config-method", repo.image_build_method)

    def test_push_image_is_late_push(self):
        metadata = Mock()
        metadata.config.push.late = True
        metadata.distgit_key = "distgit_key"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)

        expected = ("distgit_key", True)
        actual = repo.push_image([], "push_to_defaults")
        self.assertEqual(expected, actual)

    def test_push_image_nothing_to_push(self):
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.distgit_key = "distgit_key"
        metadata.get_default_push_names.return_value = []
        metadata.get_additional_push_names.return_value = []

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        push_to_defaults = False

        expected = ("distgit_key", True)
        actual = repo.push_image([], push_to_defaults)
        self.assertEqual(expected, actual)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.os.path.isdir", return_value=True)
    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.os.remove", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("__builtin__.open")
    def test_push_image_to_defaults(self, open_mock, _, __, ___, ____, _____, ______):
        open_mock.write.return_value = None
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.os.path.isdir", return_value=True)
    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.os.remove", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("__builtin__.open")
    def test_push_image_without_version_release_tuple(self, open_mock, _, __, ___, ____, _____, ______):
        open_mock.write.return_value = None
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.get_latest_build_info.return_value = ("_", "my-version", "my-release")
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults)
        self.assertEqual(expected, actual)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.os.path.isdir", return_value=True)
    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.os.remove", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("__builtin__.open")
    def test_push_image_dry_run(self, open_mock, _, __, ___, ____, _____, ______):
        open_mock.write.return_value = None
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"),
                                 dry_run=True)
        self.assertEqual(expected, actual)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.os.path.isdir", return_value=True)
    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.os.remove", return_value=None)
    @patch("distgit.time.sleep", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(1, "", "stderr"))
    @patch("__builtin__.open")
    def test_push_image_to_defaults_fail_mirroring(self, open_mock, _, __, ___, ____, _____, ______, _______):
        open_mock.write.return_value = None
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True
        version_release_tuple = ("version", "release")

        try:
            repo.push_image(tag_list,
                            push_to_defaults,
                            version_release_tuple=version_release_tuple)
            self.fail("Should have raised an exception, but didn't")
        except IOError as e:
            expected_msg = "Error pushing image: stderr"
            self.assertEqual(expected_msg, e.message)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir", return_value=None)
    @patch("distgit.os.path.isdir", return_value=True)
    @patch("distgit.os.path.isfile", return_value=True)
    @patch("distgit.os.remove", return_value=None)
    @patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("__builtin__.open")
    def test_push_image_insecure_source(self, open_mock, cmd_gather_mock, __, ___, ____, _____, ______):
        open_mock.write.return_value = None
        metadata = Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = True
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

        expected_cmd = "oc image mirror  --insecure=true --filename=my-working-dir/push/my-distgit-key"
        cmd_gather_mock.assert_called_once_with(expected_cmd)

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
