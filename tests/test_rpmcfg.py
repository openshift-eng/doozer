import unittest
from unittest import mock
from unittest.mock import MagicMock
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.exceptions import DoozerFatalError
import yaml


class TestRPMMetadata(unittest.TestCase):
    FOO_RPM_CONFIG = """
content:
  source:
    git:
      branch:
        target: release-4.7
      url: git@github.com:openshift/foo.git
    specfile: foo.spec
name: foo
owners:
- aos-master@redhat.com
distgit:
  branch: rhaos-4.7-rhel-7
targets:
- rhaos-4.7-rhel-7-candidate
- rhaos-4.7-rhel-8-candidate
    """

    @mock.patch("doozerlib.logutil.EntityLoggingAdapter")
    @mock.patch("doozerlib.rpmcfg.Dir")
    def test__build_rpm(self, MockDir, MockEntityLoggingAdapter):
        runtime = mock.MagicMock(brew_logs_dir="/path/to/brew/logs")
        koji_session = runtime.build_retrying_koji_client.return_value
        data_obj = mock.MagicMock(
            key="foo",
            filename="foo.yml",
            path="/path/to/ocp-build-data/rpms/foo.yml",
            data=yaml.safe_load(TestRPMMetadata.FOO_RPM_CONFIG)
        )
        metadata = RPMMetadata(runtime, data_obj, clone_source=False)
        metadata.source_path = "/path/to/sources/foo"
        record = {}
        terminate_event = mock.MagicMock()
        with mock.patch("doozerlib.rpmcfg.exectools.cmd_gather") as mock_cmd_gather, \
             mock.patch("doozerlib.rpmcfg.watch_tasks") as mock_watch_tasks:
            def fake_cmd_gather(cmd, **kwargs):
                if cmd == ['tito', 'release', '--debug', '--yes', '--test', 'aos']:
                    return 0, "Created task: 1\nTask info: https://brewweb.example.com/brew/taskinfo?taskID=1", ""
                if len(cmd) >= 2 and cmd[0] == "brew" and cmd[1] == "download-logs" and "--recurse" in cmd:
                    return 0, "", ""
                raise ValueError(f"Unexpected command: {cmd}")
            mock_cmd_gather.side_effect = fake_cmd_gather
            koji_session.getTaskRequest.return_value = ("https://distgit.example.com/rpms/foo.git#abcdefg", "rhaos-4.7-rhel-7-candidate", {})
            koji_session.build.return_value = 2
            mock_watch_tasks.return_value = {1: None, 2: None}
            result = metadata._build_rpm(False, record, terminate_event)
            self.assertTrue(result)
            self.assertEqual(record["task_id"], 1)
            self.assertListEqual(record["task_ids"], [1, 2])
            mock_cmd_gather.assert_called()
            koji_session.getTaskRequest.assert_called()
            koji_session.build.assert_called()
            mock_watch_tasks.assert_called()

    @mock.patch("doozerlib.logutil.EntityLoggingAdapter")
    @mock.patch("doozerlib.rpmcfg.Dir")
    def test_assert_golang_versions(self, MockDir, MockEntityLoggingAdapter):
        runtime = mock.MagicMock(brew_logs_dir="/path/to/brew/logs")
        koji_session = runtime.build_retrying_koji_client.return_value
        data_obj = mock.MagicMock(
            key="foo",
            filename="foo.yml",
            path="/path/to/ocp-build-data/rpms/foo.yml",
            data=yaml.safe_load(TestRPMMetadata.FOO_RPM_CONFIG)
        )
        koji_session.multicall.return_value.__enter__.return_value.getBuildTarget.side_effect = lambda target: MagicMock(result={"build_tag_name": target.replace("-candidate", "-build")})
        metadata = RPMMetadata(runtime, data_obj, clone_source=False)

        runtime.group_config.check_golang_versions = "exact"
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:
            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [{"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [{"name": "golang-scl-shim", "version": "1.4.0", "release": "2.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [{"name": "golang", "version": "1.4.5", "release": "3.el8", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]
            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [{"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}]
            metadata.assert_golang_versions()
            koji_session.getLatestBuilds.assert_called_with('rhaos-4.7-rhel-7-build', package="go-toolset-1.4", type="rpm")

        runtime.group_config.check_golang_versions = "exact"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:
            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [{"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [{"name": "golang-scl-shim", "version": "1.4.6", "release": "2.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [{"name": "golang", "version": "1.4.6", "release": "3.el8", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]
            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [{"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}]
            with self.assertRaises(DoozerFatalError):
                metadata.assert_golang_versions()

        runtime.group_config.check_golang_versions = "x.y"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:
            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [{"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [{"name": "golang-scl-shim", "version": "1.4.5", "release": "2.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [{"name": "golang", "version": "1.4.6", "release": "3.el8", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]
            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [{"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}]
            metadata.assert_golang_versions()

        runtime.group_config.check_golang_versions = "x.y"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:
            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [{"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [{"name": "golang-scl-shim", "version": "1.4.5", "release": "2.el7", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [{"name": "golang", "version": "1.5.6", "release": "3.el8", "epoch": None}],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]
            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [{"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}]
            with self.assertRaises(DoozerFatalError):
                metadata.assert_golang_versions()
