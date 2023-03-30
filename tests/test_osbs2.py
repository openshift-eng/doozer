import unittest
from unittest.mock import ANY, MagicMock, patch

from doozerlib import constants
from doozerlib.distgit import ImageDistGitRepo
from doozerlib.gitdata import DataObj
from doozerlib.image import ImageMetadata
from doozerlib.osbs2_builder import OSBS2Builder


class TestOSBS2Builder(unittest.IsolatedAsyncioTestCase):

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

    async def test_start_build(self):
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

        task_id, task_url = osbs2._start_build(dg, "rhaos-4.12-rhel-8-containers-candidate", profile, koji_api)
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
        self.assertEqual(task_id, 12345)
        self.assertEqual(task_url, f"{constants.BREWWEB_URL}/taskinfo?taskID=12345")

    @patch("doozerlib.exectools.cmd_gather", return_value=(0, "", ""))
    @patch("doozerlib.brew.watch_task", return_value=None)
    @patch("doozerlib.osbs2_builder.OSBS2Builder._start_build", return_value=(12345, f"{constants.BREWWEB_URL}/taskinfo?taskID=12345"))
    async def test_build(self, _start_build: MagicMock, watch_task: MagicMock, cmd_gather: MagicMock):
        koji_api = MagicMock(logged_in=False)
        koji_api.getTaskResult = MagicMock(return_value={"koji_builds": [42]})
        koji_api.getBuild = MagicMock(return_value={"id": 42, "nvr": "foo-v4.12.0-12345.p0.assembly.test"})
        runtime = MagicMock()
        runtime.build_retrying_koji_client = MagicMock(return_value=koji_api)
        osbs2 = OSBS2Builder(runtime)
        meta = self._make_image_meta(runtime)
        dg = ImageDistGitRepo(meta, autoclone=False)
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

        task_id, task_url, nvr = await osbs2.build(meta, profile, retries=1)
        self.assertEqual((task_id, task_url, nvr), (12345, f"{constants.BREWWEB_URL}/taskinfo?taskID=12345", {'id': 42, 'nvr': 'foo-v4.12.0-12345.p0.assembly.test'}))
        koji_api.gssapi_login.assert_called_once_with()
        koji_api.getTaskResult.assert_called_once_with(12345)
        koji_api.getBuild.assert_called_once_with(42)
        koji_api.tagBuild.assert_called_once_with('rhaos-4.12-rhel-8-hotfix', "foo-v4.12.0-12345.p0.assembly.test")
        runtime.build_retrying_koji_client.assert_called_once_with()
        _start_build.assert_called_once_with(dg, 'rhaos-4.12-rhel-8-containers-candidate', {'signing_intent': 'release', 'repo_type': 'signed', 'repo_list': []}, koji_api)
        watch_task.assert_called_once_with(koji_api, ANY, 12345, ANY)
        cmd_gather.assert_called_once_with(['brew', 'download-logs', '--recurse', '-d', ANY, 12345])


if __name__ == '__main__':
    unittest.main()
