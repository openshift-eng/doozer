import unittest
from unittest.mock import ANY, MagicMock, patch

from doozerlib import constants
from doozerlib.distgit import ImageDistGitRepo
from doozerlib.gitdata import DataObj
from doozerlib.image import ImageMetadata
from doozerlib.osbs2_builder import OSBS2Builder


class TestOSBS2Builder(unittest.TestCase):

    def _make_image_meta(self, runtime):
        data_obj = DataObj("foo", "/path/to/ocp-build-data/images/foo.yml", {
            "name": "foo",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/foo.git", "branch": {"target": "release-4.8"}},
                }
            },
            "targets": ["rhaos-4.12-rhel-8-containers-candidate"],
        })
        meta = ImageMetadata(runtime, data_obj, clone_source=False, prevent_cloning=True)
        meta.branch = MagicMock(return_value="rhaos-4.12-rhel-8")
        return meta

    def test_construct_build_source_url(self):
        runtime = MagicMock()
        osbs2 = OSBS2Builder(runtime)
        meta = self._make_image_meta(runtime)
        dg = ImageDistGitRepo(meta, autoclone=False)

        with self.assertRaises(ValueError):
            dg.sha = None
            actual = osbs2._construct_build_source_url(dg)

        dg.sha = "deadbeef"
        actual = osbs2._construct_build_source_url(dg)
        self.assertEqual(actual, f"{constants.DISTGIT_GIT_URL}/containers/foo#deadbeef")

    def test_start_build(self):
        runtime = MagicMock()
        osbs2 = OSBS2Builder(runtime)
        meta = self._make_image_meta(runtime)
        dg = ImageDistGitRepo(meta, autoclone=False)
        dg.sha = "deadbeef"
        dg.branch = "rhaos-4.12-rhel-8"
        dg.cgit_file_available = MagicMock(return_value=(True, "http://cgit.example.com/foo.repo"))
        profile = {
            "signing_intent": "release",
            "repo_type": "signed",
            "repo_list": [],
        }
        koji_api = MagicMock(logged_in=False)
        koji_api.buildContainer.return_value = 12345

        osbs2._start_build(dg, "rhaos-4.12-rhel-8-containers-candidate", profile, koji_api)
        dg.cgit_file_available.assert_called_once_with(".oit/signed.repo")
        koji_api.gssapi_login.assert_called_once_with()
        koji_api.buildContainer.assert_called_once_with(
            f"{constants.DISTGIT_GIT_URL}/containers/foo#deadbeef",
            "rhaos-4.12-rhel-8-containers-candidate",
            opts={
                'scratch': False,
                'signing_intent': "release",
                'yum_repourls': ["http://cgit.example.com/foo.repo"],
                'git_branch': "rhaos-4.12-rhel-8",
            },
            channel="container-binary")
        self.assertEqual(osbs2.task_id, 12345)
        self.assertEqual(osbs2.task_url, f"{constants.BREWWEB_URL}/taskinfo?taskID=12345")

    @patch("doozerlib.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("doozerlib.brew.watch_task", return_value=None)
    def test_build(self, watch_task: MagicMock, cmd_gather: MagicMock):
        class MockedOSBS2Builder(OSBS2Builder):
            def _start_build(self, dg, target, profile, koji_api):
                self.task_id = 12345
                self.task_url = f'https://brewweb.engineering.redhat.com/brew/taskinfo?taskID={self.task_id}'

        koji_api = MagicMock(logged_in=False)
        koji_api.getTaskResult = MagicMock(return_value={"koji_builds": [42]})
        koji_api.getBuild = MagicMock(return_value={"id": 42, "nvr": "foo-v4.12.0-12345.p0.assembly.test"})
        runtime = MagicMock()
        runtime.build_retrying_koji_client = MagicMock(return_value=koji_api)
        osbs2 = MockedOSBS2Builder(runtime)
        meta = self._make_image_meta(runtime)
        dg = ImageDistGitRepo(meta, autoclone=False)
        dg.cgit_file_available = MagicMock(return_value=(True, "http://cgit.example.com/foo.repo"))
        koji_api.buildContainer.return_value = 12345
        meta.distgit_repo = MagicMock(return_value=dg)
        dg.sha = "deadbeef"
        dg.branch = "rhaos-4.12-rhel-8"
        dg.org_version = "v4.12.0"
        dg.org_release = "12345.p0.assembly.test"
        profile = {
            "signing_intent": "release",
            "repo_type": "signed",
            "repo_list": [],
        }

        osbs2.build(meta, profile, retries=1)

        koji_api.gssapi_login.assert_called()
        koji_api.getTaskResult.assert_called_once_with(12345)
        koji_api.getBuild.assert_called_once_with(42)
        koji_api.tagBuild.assert_called_once_with('rhaos-4.12-rhel-8-hotfix', "foo-v4.12.0-12345.p0.assembly.test")
        runtime.build_retrying_koji_client.assert_called_once_with()
        watch_task.assert_called_once_with(koji_api, ANY, 12345, terminate_event=ANY)
        cmd_gather.assert_called_once_with(['brew', 'download-logs', '--recurse', '-d', ANY, 12345])
        self.assertEqual(osbs2.task_id, 12345)
        self.assertEqual(osbs2.task_url, f"{constants.BREWWEB_URL}/taskinfo?taskID=12345")


if __name__ == '__main__':
    unittest.main()
