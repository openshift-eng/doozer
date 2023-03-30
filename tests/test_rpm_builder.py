import logging
import io
from pathlib import Path

from unittest import IsolatedAsyncioTestCase, mock

from doozerlib import distgit, gitdata, rpmcfg
from doozerlib.exectools import RetryException
from doozerlib.rpm_builder import RPMBuilder


class TestRPMBuilder(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        pass

    def _make_runtime(self, assembly=None):
        runtime = mock.MagicMock()
        runtime.group_config.public_upstreams = [{"private": "https://github.com/openshift-priv", "public": "https://github.com/openshift"}]
        runtime.brew_logs_dir = "/path/to/brew-logs"
        runtime.assembly = assembly
        stream = io.StringIO()
        logging.basicConfig(level=logging.INFO, stream=stream)
        runtime.logger = logging.getLogger()
        return runtime

    def _make_rpm_meta(self, runtime, source_sha, distgit_sha):
        data_obj = gitdata.DataObj("foo", "/path/to/ocp-build-data/rpms/foo.yml", {
            "name": "foo",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/foo.git", "branch": {"target": "release-4.8"}},
                    "specfile": "foo.spec",
                    "modifications": [
                        {"action": "add", "command": ["my-command", "--my-arg"]}
                    ],
                }
            },
            "targets": ["rhaos-4.4-rhel-8-candidate", "rhaos-4.4-rhel-7-candidate"],
        })

        rpm = rpmcfg.RPMMetadata(runtime, data_obj, clone_source=False)
        rpm.clone_source = mock.MagicMock(return_value=source_sha)
        rpm.get_package_name_from_spec = mock.Mock(return_value='foo')
        rpm.logger = mock.MagicMock(spec=logging.Logger)
        rpm.private_fix = False
        rpm.pre_init_sha = source_sha
        rpm.source_path = "/path/to/sources/foo"
        rpm.specfile = rpm.source_path + "/foo.spec"
        rpm.targets = ["rhaos-4.4-rhel-8-candidate", "rhaos-4.4-rhel-7-candidate"]
        dg = distgit.RPMDistGitRepo(rpm, autoclone=False)
        dg.distgit_dir = "/path/to/distgits/rpms/foo"
        dg.dg_path = Path(dg.distgit_dir)
        dg.commit = mock.MagicMock(return_value=distgit_sha)
        dg.push_async = mock.AsyncMock()
        rpm.distgit_repo = mock.MagicMock(return_value=dg)
        rpm._run_modifications = mock.MagicMock()
        return rpm

    @mock.patch("doozerlib.rpm_builder.Path.mkdir")
    @mock.patch("shutil.copy")
    @mock.patch("aiofiles.os.remove")
    @mock.patch("aiofiles.open")
    @mock.patch("doozerlib.rpm_builder.exectools.cmd_assert_async")
    async def test_rebase(self, mocked_cmd_assert_async: mock.Mock, mocked_open: mock.Mock, mocked_os_remove: mock.Mock,
                          mocked_copy: mock.Mock, mocked_mkdir: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime()
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        dg = rpm.distgit_repo()
        mocked_cmd_assert_async.side_effect = \
            lambda cmd, **kwargs: {"spectool": ("Source0: 1.tar.gz\nSource1: a.diff\nPatch0: b/c.diff\n", "")} \
            .get(cmd[0], ("fake_stdout", "fake_stderr"))

        builder = RPMBuilder(runtime, scratch=False, dry_run=False)
        builder._populate_specfile_async = mock.AsyncMock(return_value=["fake spec content"])

        actual = await builder.rebase(rpm, "1.2.3", "202104070000.test.p?")

        self.assertEqual(actual, distgit_sha)
        self.assertEqual(rpm.release, "202104070000.test.p0.g" + source_sha[:7])
        mocked_open.assert_called_once_with(dg.dg_path / "foo.spec", "w")
        mocked_open.return_value.__aenter__.return_value.writelines.assert_called_once_with(["fake spec content"])
        mocked_cmd_assert_async.assert_any_call(["tar", "-czf", dg.dg_path / f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz", "--exclude=.git", fr"--transform=s,^\./,{rpm.config.name}-{rpm.version}/,", "."], cwd=rpm.source_path)
        mocked_cmd_assert_async.assert_any_call(["rhpkg", "new-sources", f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz"], cwd=dg.dg_path, retries=3)
        mocked_cmd_assert_async.assert_called_with(["spectool", "--", dg.dg_path / "foo.spec"], cwd=dg.dg_path)
        mocked_copy.assert_any_call(Path(rpm.source_path) / "a.diff", dg.dg_path / "a.diff", follow_symlinks=False)
        mocked_copy.assert_any_call(Path(rpm.source_path) / "b/c.diff", dg.dg_path / "b/c.diff", follow_symlinks=False)
        rpm._run_modifications.assert_called_once_with(f"{rpm.source_path}/foo.spec", rpm.source_path)
        dg.commit.assert_called_once_with(f"Automatic commit of package [{rpm.config.name}] release [{rpm.version}-{rpm.release}].",
                                          commit_attributes={
                                              'version': '1.2.3',
                                              'release': '202104070000.test.p0.g3f17b42',
                                              'io.openshift.build.commit.id': '3f17b42b8aa7d294c0d2b6f946af5fe488f3a722',
                                              'io.openshift.build.source-location': None}
                                          )
        dg.push_async.assert_called_once()

    @mock.patch("doozerlib.rpm_builder.Path.mkdir")
    @mock.patch("shutil.copy")
    @mock.patch("aiofiles.os.remove")
    @mock.patch("aiofiles.open")
    @mock.patch("doozerlib.rpm_builder.exectools.cmd_assert_async")
    async def test_rebase_with_assembly(self, mocked_cmd_assert_async: mock.Mock, mocked_open: mock.Mock, mocked_os_remove: mock.Mock,
                                        mocked_copy: mock.Mock, mocked_mkdir: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime(assembly='tester')
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        dg = rpm.distgit_repo()
        mocked_cmd_assert_async.side_effect = \
            lambda cmd, **kwargs: {"spectool": ("Source0: 1.tar.gz\nSource1: a.diff\nPatch0: b/c.diff\n", "")} \
            .get(cmd[0], ("fake_stdout", "fake_stderr"))

        builder = RPMBuilder(runtime, scratch=False, dry_run=False)
        builder._populate_specfile_async = mock.AsyncMock(return_value=["fake spec content"])

        actual = await builder.rebase(rpm, "1.2.3", "202104070000.test.p?")

        self.assertEqual(actual, distgit_sha)
        self.assertEqual(rpm.release, "202104070000.test.p0.g" + source_sha[:7] + '.assembly.tester')
        mocked_open.assert_called_once_with(dg.dg_path / "foo.spec", "w")
        mocked_open.return_value.__aenter__.return_value.writelines.assert_called_once_with(["fake spec content"])
        mocked_cmd_assert_async.assert_any_call(["tar", "-czf", dg.dg_path / f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz", "--exclude=.git", fr"--transform=s,^\./,{rpm.config.name}-{rpm.version}/,", "."], cwd=rpm.source_path)
        mocked_cmd_assert_async.assert_any_call(["rhpkg", "new-sources", f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz"], cwd=dg.dg_path, retries=3)
        mocked_cmd_assert_async.assert_called_with(["spectool", "--", dg.dg_path / "foo.spec"], cwd=dg.dg_path)
        mocked_copy.assert_any_call(Path(rpm.source_path) / "a.diff", dg.dg_path / "a.diff", follow_symlinks=False)
        mocked_copy.assert_any_call(Path(rpm.source_path) / "b/c.diff", dg.dg_path / "b/c.diff", follow_symlinks=False)
        rpm._run_modifications.assert_called_once_with(f'{rpm.source_path}/foo.spec', rpm.source_path)
        dg.commit.assert_called_once_with(f"Automatic commit of package [{rpm.config.name}] release [{rpm.version}-{rpm.release}].",
                                          commit_attributes={
                                              'version': '1.2.3',
                                              'release': '202104070000.test.p0.g3f17b42.assembly.tester',
                                              'io.openshift.build.commit.id': '3f17b42b8aa7d294c0d2b6f946af5fe488f3a722',
                                              'io.openshift.build.source-location': None})
        dg.push_async.assert_called_once()

    @mock.patch("doozerlib.rpm_builder.exectools.cmd_gather_async")
    async def test_build_success(self, mocked_cmd_gather_async: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime()
        runtime.hotfix = True
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        rpm.assert_golang_versions = mock.MagicMock()
        dg = rpm.distgit_repo()
        builder = RPMBuilder(runtime, scratch=False, dry_run=False)
        builder._golang_required = mock.AsyncMock(return_value=True)
        builder._build_target_async = mock.AsyncMock(side_effect=lambda _, target: {
            "rhaos-4.4-rhel-8-candidate": (10001, "https://brewweb.example.com/brew/taskinfo?taskID=10001"),
            "rhaos-4.4-rhel-7-candidate": (10002, "https://brewweb.example.com/brew/taskinfo?taskID=10002"),
        }[target])
        builder._watch_tasks_async = mock.AsyncMock(side_effect=lambda task_ids, _: {
            task_id: None for task_id in task_ids})
        mocked_cmd_gather_async.return_value = (0, "some stdout", "some stderr")
        dg.resolve_specfile_async = mock.AsyncMock(return_value=(dg.dg_path / "foo.spec", ("foo", "1.2.3", "1"), source_sha))
        koji_api = runtime.shared_koji_client_session.return_value.__enter__.return_value
        koji_api.multicall.return_value.__enter__.return_value.listBuilds.side_effect = lambda taskID, completeBefore: {
            10001: mock.MagicMock(result=[{"nvr": "foo-1.2.3-1.el8"}]),
            10002: mock.MagicMock(result=[{"nvr": "foo-1.2.3-1.el7"}]),
        }[taskID]

        actual = await builder.build(rpm, retries=3)
        expected = ([10001, 10002], ["https://brewweb.example.com/brew/taskinfo?taskID=10001", "https://brewweb.example.com/brew/taskinfo?taskID=10002"], ["foo-1.2.3-1.el8", "foo-1.2.3-1.el7"])
        self.assertEqual(actual, expected)
        self.assertTrue(rpm.build_status)
        builder._golang_required.assert_called_once_with(rpm.specfile)
        rpm.assert_golang_versions.assert_called_once()
        builder._build_target_async.assert_any_call(rpm, "rhaos-4.4-rhel-8-candidate")
        builder._build_target_async.assert_any_call(rpm, "rhaos-4.4-rhel-7-candidate")
        builder._watch_tasks_async.assert_called_once_with([10001, 10002], mock.ANY)
        mocked_cmd_gather_async.assert_any_call(["brew", "download-logs", "--recurse", "-d", mock.ANY, 10001])
        mocked_cmd_gather_async.assert_any_call(["brew", "download-logs", "--recurse", "-d", mock.ANY, 10002])

    @mock.patch("asyncio.sleep")
    @mock.patch("doozerlib.rpm_builder.exectools.cmd_gather_async")
    async def test_build_failure(self, mocked_cmd_gather_async: mock.Mock, mocked_sleep: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime()
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        rpm.assert_golang_versions = mock.MagicMock()
        dg = rpm.distgit_repo()
        builder = RPMBuilder(runtime, scratch=False, dry_run=False)
        builder._golang_required = mock.AsyncMock(return_value=True)
        builder._build_target_async = mock.AsyncMock(side_effect=lambda _, target: {
            "rhaos-4.4-rhel-8-candidate": (10001, "https://brewweb.example.com/brew/taskinfo?taskID=10001"),
            "rhaos-4.4-rhel-7-candidate": (10002, "https://brewweb.example.com/brew/taskinfo?taskID=10002"),
        }[target])
        builder._watch_tasks_async = mock.AsyncMock(side_effect=lambda task_ids, _: {
            task_id: "Some error" for task_id in task_ids})
        mocked_cmd_gather_async.return_value = (0, "some stdout", "some stderr")
        dg.resolve_specfile_async = mock.AsyncMock(return_value=(dg.dg_path / "foo.spec", ("foo", "1.2.3", "1"), source_sha))

        with self.assertRaises(RetryException) as cm:
            await builder.build(rpm, retries=3)

        expected = ([10001, 10002], ["https://brewweb.example.com/brew/taskinfo?taskID=10001", "https://brewweb.example.com/brew/taskinfo?taskID=10002"])
        self.assertEqual(cm.exception.args[1], expected)
        self.assertIn("Giving up after 3 failed attempt(s):", cm.exception.args[0])
        self.assertFalse(rpm.build_status)
        builder._golang_required.assert_called_once_with(rpm.specfile)
        rpm.assert_golang_versions.assert_called_once()
        builder._build_target_async.assert_any_call(rpm, "rhaos-4.4-rhel-8-candidate")
        builder._build_target_async.assert_any_call(rpm, "rhaos-4.4-rhel-7-candidate")
        builder._watch_tasks_async.assert_any_call([10001, 10002], mock.ANY)
        mocked_cmd_gather_async.assert_any_call(["brew", "download-logs", "--recurse", "-d", mock.ANY, 10001])
        mocked_cmd_gather_async.assert_any_call(["brew", "download-logs", "--recurse", "-d", mock.ANY, 10002])
        mocked_sleep.assert_called()

    @mock.patch("doozerlib.rpm_builder.exectools.cmd_assert_async")
    async def test_golang_required(self, mocked_cmd_assert_async: mock.Mock):
        mocked_cmd_assert_async.return_value = ("""
git
python3-devel
        """, "")
        builder = RPMBuilder(mock.Mock(), scratch=False, dry_run=False)
        actual = await builder._golang_required("./foo.spec")
        self.assertFalse(actual)

        mocked_cmd_assert_async.return_value = ("""
git
python3-devel
golang-1.2.3
systemd-units
        """, "")
        builder = RPMBuilder(mock.Mock(), scratch=False, dry_run=False)
        actual = await builder._golang_required("./foo.spec")
        self.assertTrue(actual)

    @mock.patch("aiofiles.open")
    async def test_populate_specfile_async(self, mocked_open: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime()
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        version = "1.2.3"
        release = "202104070000.yuxzhu_test.p0"
        rpm.set_nvr(version, release)
        rpm.get_jira_info = mock.MagicMock(return_value=("My Product", "My Component"))
        mocked_file = mocked_open.return_value.__aenter__.return_value
        mocked_file.readlines.return_value = """
%global os_git_vars OS_GIT_VERSION='' OS_GIT_COMMIT='' OS_GIT_MAJOR='' OS_GIT_MINOR='' OS_GIT_TREE_STATE=''
%global commit 0000000
Name:           foo
Version:        %{version}
Release:        %{release}%{dist}
Summary:        FOO binaries
License:        ASL 2.0

Source0:        https://example.com/foo/archive/%{commit}/%{name}-%{version}.tar.gz
%description
Some description

%prep
%setup -q
#...
%autosetup -S git
%changelog
        """.splitlines()

        specfile_content = await RPMBuilder._populate_specfile_async(rpm, "foo-1.2.3.tar.gz", "https://example.com/foo/archive/commit/shasum")

        self.assertIn("Version:        1.2.3\n", specfile_content)
        self.assertIn("Release:        202104070000.yuxzhu_test.p0%{?dist}\n", specfile_content)
        self.assertIn("Source0:        foo-1.2.3.tar.gz\n", specfile_content)
        self.assertIn("%description\n[Maintainer] project: My Product, component: My Component\n", specfile_content)
        self.assertIn("%setup -q -n foo-1.2.3\n", specfile_content)
        self.assertIn("%autosetup -S git -n foo-1.2.3 -p1\n", specfile_content)
        self.assertIn("Version:        1.2.3\n", specfile_content)
        self.assertIn("AOS Automation Release Team <noreply@redhat.com>", specfile_content[17])

    @mock.patch("doozerlib.rpm_builder.exectools.cmd_assert_async")
    async def test_build_target_async(self, mocked_cmd_assert_async: mock.Mock):
        source_sha = "3f17b42b8aa7d294c0d2b6f946af5fe488f3a722"
        distgit_sha = "4cd7f576ad005aadd3c25ea56c7986bc6a7e7340"
        runtime = self._make_runtime()
        rpm = self._make_rpm_meta(runtime, source_sha, distgit_sha)
        dg = rpm.distgit_repo()
        mocked_cmd_assert_async.return_value = ("""
Created task: 123456
Task info: https://brewweb.example.com/brew/taskinfo?taskID=123456
        """, "")

        # call with scratch=False
        builder = RPMBuilder(mock.Mock(), scratch=False, dry_run=False)
        expected = (123456, "https://brewweb.example.com/brew/taskinfo?taskID=123456")
        actual = await builder._build_target_async(rpm, "my-target")

        self.assertEqual(actual, expected)
        mocked_cmd_assert_async.assert_called_once_with(["rhpkg", "build", "--nowait", "--target", "my-target"], cwd=dg.dg_path)

        # call with scratch=True
        builder = RPMBuilder(mock.Mock(), scratch=True, dry_run=False)
        mocked_cmd_assert_async.reset_mock()
        actual = await builder._build_target_async(rpm, "my-target2")

        self.assertEqual(actual, expected)
        mocked_cmd_assert_async.assert_called_once_with(["rhpkg", "build", "--nowait", "--target", "my-target2", "--skip-tag"], cwd=dg.dg_path)

    @mock.patch("doozerlib.rpm_builder.brew.watch_tasks")
    async def test_watch_tasks_async(self, mocked_watch_tasks: mock.Mock):
        task_ids = [10001, 10002]
        mocked_watch_tasks.return_value = {task: None for task in task_ids}

        builder = RPMBuilder(mock.Mock(), scratch=True, dry_run=False)
        actual = await builder._watch_tasks_async(task_ids, mock.Mock())

        self.assertEqual(actual, {task: None for task in task_ids})
        mocked_watch_tasks.assert_called_once_with(mock.ANY, mock.ANY, task_ids, mock.ANY)
