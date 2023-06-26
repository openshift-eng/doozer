import io
import logging
import re
import tempfile
import unittest
from pathlib import Path
from threading import Lock
from unittest.mock import Mock, patch

from flexmock import flexmock
from unittest.mock import MagicMock

from doozerlib import distgit, model
from doozerlib.assembly import AssemblyTypes
from doozerlib.image import ImageMetadata

from ..support import MockScanner, TestDistgit


class TestImageDistGit(TestDistgit):

    def setUp(self):
        super(TestImageDistGit, self).setUp()
        self.img_dg = distgit.ImageDistGitRepo(self.md, autoclone=False)
        self.img_dg.runtime.group_config = model.Model()

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
            get_major_minor_fields=lambda *_, **__: (4, 14)
        )

    def test_clone_invokes_read_master_data(self):
        """
        Mocking `clone` method of parent class, since we are only interested
        in validating that `_read_master_data` is called in the child class.
        """
        (flexmock(distgit.DistGitRepo)
            .should_receive("clone")
            .replace_with(lambda *_: None))

        (flexmock(distgit.ImageDistGitRepo)
            .should_receive("_read_master_data")
            .once())

        metadata = flexmock(runtime=self.mock_runtime(),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing)),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        (distgit.ImageDistGitRepo(metadata, autoclone=False)
            .clone("distgits_root_dir", "distgit_branch"))

    def test_image_build_method_default(self):
        metadata = flexmock(runtime=self.mock_runtime(group_config=flexmock(default_image_build_method="default-method")),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing),
                                            image_build_method=distgit.Missing,
                                            get=lambda *_: {}),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("default-method", repo.image_build_method)

    def test_image_build_method_imagebuilder(self):
        get = lambda key, default: dict({"builder": "..."}) if key == "from" else default

        metadata = flexmock(runtime=self.mock_runtime(group_config=flexmock(default_image_build_method=distgit.Missing)),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing),
                                            image_build_method=distgit.Missing,
                                            get=get),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("osbs2", repo.image_build_method)

        metadata = flexmock(runtime=self.mock_runtime(group_config=flexmock(default_image_build_method="osbs2")),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing),
                                            image_build_method=distgit.Missing,
                                            get=get),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("osbs2", repo.image_build_method)

        metadata = flexmock(runtime=self.mock_runtime(group_config=flexmock(default_image_build_method="osbs2")),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing),
                                            image_build_method="imagebuilder",
                                            get=get),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("imagebuilder", repo.image_build_method)

    def test_image_build_method_from_config(self):
        metadata = flexmock(runtime=self.mock_runtime(group_config=flexmock(default_image_build_method="default-method")),
                            config=flexmock(distgit=flexmock(branch=distgit.Missing),
                                            image_build_method="config-method",
                                            get=lambda _, d: d),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        self.assertEqual("config-method", repo.image_build_method)

    def test_wait_for_build_with_build_status_true(self):
        logger = flexmock()

        (logger
            .should_receive("info")
            .with_args("Member waiting for me to build: i-am-waiting")
            .once()
            .ordered())

        (logger
            .should_receive("info")
            .with_args("Member successfully waited for me to build: i-am-waiting")
            .once()
            .ordered())

        metadata = flexmock(logger=logger,
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            runtime=self.mock_runtime(),
                            name="_irrelevant_")

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        repo.build_lock = flexmock()  # we don't want tests to be blocked
        repo.build_status = True

        repo.wait_for_build("i-am-waiting")

    def test_wait_for_build_with_build_status_false(self):
        metadata = flexmock(qualified_name="my-qualified-name",
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            runtime=self.mock_runtime(),
                            name="_irrelevant_",
                            logger=flexmock(info=lambda *_: None))

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        repo.build_lock = flexmock()  # we don't want tests to be blocked
        repo.build_status = False

        try:
            repo.wait_for_build("i-am-waiting")
            self.fail("Should have raised IOError")
        except IOError as e:
            expected = "Error building image: my-qualified-name (i-am-waiting was waiting)"
            actual = str(e)
            self.assertEqual(expected, actual)

    def test_push(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        flexmock(distgit.exectools)\
            .should_receive("cmd_assert")\
            .with_args("timeout 999 rhpkg push", retries=3)\
            .ordered()

        flexmock(distgit.exectools)\
            .should_receive("cmd_assert")\
            .with_args("timeout 999 git push --tags", retries=3)\
            .ordered()

        metadata = flexmock(runtime=self.mock_runtime(global_opts={"rhpkg_push_timeout": 999},
                                                      get_named_semaphore=lambda *_, **__: Lock()),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            name="_irrelevant_",
                            logger=flexmock(info=lambda *_: None))

        expected = (metadata, True)
        actual = distgit.ImageDistGitRepo(metadata, autoclone=False).push()

        self.assertEqual(expected, actual)

    def test_push_with_io_error(self):
        # preventing tests from interacting with the real filesystem
        flexmock(distgit).should_receive("Dir").and_return(flexmock(__exit__=None))

        # preventing tests from executing real commands
        flexmock(distgit.exectools).should_receive("cmd_gather").and_return(None)

        # pretending cmd_assert raised an IOError
        flexmock(distgit.exectools).should_receive("cmd_assert").and_raise(IOError("io-error"))

        metadata = flexmock(runtime=self.mock_runtime(global_opts={"rhpkg_push_timeout": "_irrelevant_"},
                                                      get_named_semaphore=lambda *_, **__: Lock()),
                            config=flexmock(distgit=flexmock(branch="_irrelevant_")),
                            name="_irrelevant_",
                            logger=flexmock(info=lambda *_: None))

        expected = (metadata, repr(IOError("io-error")))
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

        with tempfile.NamedTemporaryFile() as temp:
            self.img_dg._logs_dir = lambda: temp.name  # search a dir that won't be there
            self.assertIsNone(self.img_dg._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("Exception while trying to analyze build logs", output)

    def test_mangle_pkgmgr_cmds_unchanged(self):
        unchanging = [
            "yum install foo",
            "dnf install foo",
            "microdnf install foo",
            "ignore yum-config-manager here",
            "foo && yum install && bar",
            "ignore dnf config-manager here",
            "ignore microdnf config-manager here",
        ]
        for cmd in unchanging:
            self.assertFalse(distgit.ImageDistGitRepo._mangle_pkgmgr(cmd)[0])

    def test_mangle_pkgmgr_cmds_changed(self):
        # note: adjacent spaces are not removed, so removals may result in redundant spaces
        changes = {
            "yum-config-manager foo bar baz": ": 'removed yum-config-manager'",
            "dnf config-manager foo bar baz": ": 'removed dnf config-manager'",
            "yum --enablerepo=bar install foo --disablerepo baz": "yum  install foo  ",
            "microdnf --enablerepo=bar install foo --disablerepo baz": "microdnf  install foo  ",

            "yum-config-manager foo bar baz && yum --enablerepo=bar install foo && build stuff":
                ": 'removed yum-config-manager' \\\n && yum  install foo \\\n && build stuff",

            "dnf config-manager foo bar baz && microdnf --enablerepo=bar install foo && build stuff":
                ": 'removed dnf config-manager' \\\n && microdnf  install foo \\\n && build stuff",

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
            changed, result = distgit.ImageDistGitRepo._mangle_pkgmgr(cmd)
            self.assertEqual(result, expect)
            self.assertTrue(changed)

    def test_mangle_pkgmgr_parse_err(self):
        with self.assertRaises(IOError) as e:
            distgit.ImageDistGitRepo._mangle_pkgmgr("! ( && || $ ) totally broken")
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
        self.img_dg.config.content = model.Model()

        # no source, dist-git only; should go on to check if built/stale
        flexmock(self.img_dg).should_receive("_built_or_recent").once().and_return(None)
        self.assertIsNone(self.img_dg.matches_source_commit({}))

        # source specified and matches Dockerfile in dist-git
        self.img_dg.config.content.source = model.Model()
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

    def test_inject_yum_update_commands_without_final_stage_user(self):
        runtime = MagicMock()
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "enabled_repos": ["repo-a", "repo-b"]
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dockerfile = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
COPY --from=builder /some/path/a /some/path/b
        """.strip()
        dg.logger = logging.getLogger()
        dg.dg_path = MagicMock()
        actual = dg._update_yum_update_commands(True, io.StringIO(dockerfile)).getvalue().strip().splitlines()
        expected = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
# __doozer=yum-update
RUN yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
COPY --from=builder /some/path/a /some/path/b
        """.strip().splitlines()
        self.assertListEqual(actual, expected)

    def test_inject_yum_update_commands_with_final_stage_user(self):
        runtime = MagicMock()
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-7'},
                "enabled_repos": ["repo-a", "repo-b"],
                "final_stage_user": 1002,
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dockerfile = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
COPY --from=builder /some/path/a /some/path/b
        """.strip()
        dg.logger = logging.getLogger()
        dg.dg_path = MagicMock()
        actual = dg._update_yum_update_commands(True, io.StringIO(dockerfile)).getvalue().strip().splitlines()
        expected = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
# __doozer=yum-update
USER 0
# __doozer=yum-update
RUN yum install -y yum-plugin-priorities && yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
# __doozer=yum-update
USER 1002
COPY --from=builder /some/path/a /some/path/b
        """.strip().splitlines()
        self.assertListEqual(actual, expected)

    def test_remove_yum_update_commands(self):
        runtime = MagicMock()
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "enabled_repos": ["repo-a", "repo-b"]
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()
        dg.dg_path = MagicMock()
        dockerfile = """
FROM some-base-image:some-tag AS builder
# __doozer=yum-update
USER 0
# __doozer=yum-update
RUN yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
# __doozer=yum-update
USER 0
# __doozer=yum-update
RUN yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
# __doozer=yum-update
USER 1001
COPY --from=builder /some/path/a /some/path/b
        """.strip()
        actual = dg._update_yum_update_commands(False, io.StringIO(dockerfile)).getvalue().strip().splitlines()
        expected = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
COPY --from=builder /some/path/a /some/path/b
        """.strip().splitlines()
        self.assertListEqual(actual, expected)

    def test_inject_yum_update_commands_without_repos(self):
        runtime = MagicMock()
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "enabled_repos": []
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()
        dg.dg_path = MagicMock()
        dockerfile = """
FROM some-base-image:some-tag AS builder
# __doozer=yum-update
USER 0
# __doozer=yum-update
RUN yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
# __doozer=yum-update
USER 0
# __doozer=yum-update
RUN yum update -y && yum clean all  # set final_stage_user in ART metadata if this fails
# __doozer=yum-update
USER 1001
COPY --from=builder /some/path/a /some/path/b
        """.strip()
        actual = dg._update_yum_update_commands(True, io.StringIO(dockerfile)).getvalue().strip().splitlines()
        expected = """
FROM some-base-image:some-tag AS builder
LABEL name=value
RUN some-command
FROM another-base-image:some-tag
COPY --from=builder /some/path/a /some/path/b
        """.strip().splitlines()
        self.assertListEqual(actual, expected)

    def test_generate_osbs_image_config_with_addtional_tags(self):
        runtime = MagicMock(uuid="123456")
        runtime.group_config.cachito.enabled = False
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "additional_tags": ["tag_a", "tag_b"]
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()

        # assembly is not enabled
        runtime.assembly = None
        container_yaml = dg._generate_osbs_image_config("v4.10.0")
        self.assertEqual(sorted(container_yaml["tags"]), sorted(['v4.10.0.123456', 'v4.10', 'v4.10.0', 'tag_a', 'tag_b']))

        # assembly is enabled
        runtime.assembly = "art3109"
        container_yaml = dg._generate_osbs_image_config("v4.10.0")
        self.assertEqual(sorted(container_yaml["tags"]), sorted(['assembly.art3109', 'v4.10.0.123456', 'v4.10', 'v4.10.0', 'tag_a', 'tag_b']))

    def test_detect_package_manangers_without_git_clone(self):
        runtime = MagicMock(uuid="123456")
        runtime.group_config.cachito.enabled = True
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "additional_tags": ["tag_a", "tag_b"]
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()

        with self.assertRaises(FileNotFoundError):
            dg._detect_package_managers()

    @patch("pathlib.Path.is_dir", autospec=True)
    @patch("pathlib.Path.is_file", autospec=True)
    def test_detect_package_manangers(self, is_file: Mock, is_dir: Mock):
        runtime = MagicMock(uuid="123456")
        runtime.group_config.cachito.enabled = True
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "additional_tags": ["tag_a", "tag_b"]
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()
        dg.distgit_dir = "/path/to/distgit/containers/foo"
        dg.dg_path = Path(dg.distgit_dir)
        is_dir.return_value = True
        is_file.side_effect = lambda f: f.name in ["go.mod", "package-lock.json"]

        actual = dg._detect_package_managers()
        self.assertEqual(set(actual), {"gomod", "npm"})

    @patch("pathlib.Path.is_dir", autospec=True, return_value=True)
    def test_generate_osbs_image_config_with_cachito_enabled(self, is_dir: Mock):
        runtime = MagicMock(uuid="123456", assembly="test", assembly_basis_event=None, profile=None, odcs_mode=False)
        runtime.group_config = model.Model()
        meta = ImageMetadata(runtime, model.Model({
            "key": "foo",
            'data': {
                'name': 'openshift/foo',
                'distgit': {'branch': 'fake-branch-rhel-8'},
                "additional_tags": ["tag_a", "tag_b"],
                "content": {
                    "source": {"git": {"url": "git@example.com:openshift-priv/foo.git", "branch": {"target": "release-4.10"}}}
                },
                "cachito": {"enabled": True, "flags": ["gomod-vendor-check"]}
            },
        }))
        dg = distgit.ImageDistGitRepo(meta, autoclone=False)
        dg.logger = logging.getLogger()
        dg.distgit_dir = "/path/to/distgit/containers/foo"
        dg.dg_path = Path(dg.distgit_dir)
        dg.actual_source_url = "git@example.com:openshift-priv/foo.git"
        dg.source_full_sha = "deadbeef"
        dg._detect_package_managers = MagicMock(return_value=["gomod"])

        actual = dg._generate_osbs_image_config("v4.10.0")
        self.assertEqual(actual["remote_sources"][0]["remote_source"]["repo"], "https://example.com/openshift-priv/foo")
        self.assertEqual(actual["remote_sources"][0]["remote_source"]["ref"], "deadbeef")
        self.assertEqual(actual["remote_sources"][0]["remote_source"]["pkg_managers"], ["gomod"])
        self.assertEqual(actual["remote_sources"][0]["remote_source"]["flags"], ["gomod-vendor-check"])


if __name__ == "__main__":
    unittest.main()
