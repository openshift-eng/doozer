#!/usr/bin/env python3


import logging
import json
import unittest
import os
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock, Mock
from urllib.error import URLError

from doozerlib import rhcos
from doozerlib.model import ListModel, Model
from doozerlib.repos import Repos


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger
        self.group_config = Model({})
        self.pooled_koji_client_session = MagicMock()


def _urlopen_json_cm(mock_urlopen, content, rc=200):
    # serializes content as json and has the urlopen context manager return it
    cm = MagicMock()
    cm.getcode.return_value = rc
    cm.read.return_value = bytes(json.dumps(content), 'utf-8')
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm


class TestRhcos(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.logger = MagicMock(spec=logging.Logger)

        runtime = MockRuntime(self.logger)
        self.runtime = runtime
        self.koji_mock = runtime.pooled_koji_client_session.return_value.__enter__.return_value
        self.respath = Path(os.path.dirname(__file__), 'resources')

    def tearDown(self):
        pass

    def test_get_primary_container_conf(self):
        # default is same as it's always been
        self.assertEqual("machine-os-content", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "x86_64").get_primary_container_conf()["name"])

        # but we can configure a different primary
        self.runtime.group_config.rhcos = Model(dict(payload_tags=[dict(name="spam"), dict(name="eggs", primary=True)]))
        self.assertEqual("eggs", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "x86_64").get_primary_container_conf()["name"])

    def test_release_url(self):
        self.assertIn("4.6-s390x", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "s390x").rhcos_release_url())
        self.assertNotIn("x86_64", rhcos.RHCOSBuildFinder(self.runtime, "4.6", "x86_64").rhcos_release_url())
        self.assertIn("4.9-aarch64", rhcos.RHCOSBuildFinder(self.runtime, "4.9", "aarch64").rhcos_release_url())

        self.runtime.group_config.urls = Model(dict(rhcos_release_base=dict(aarch64="https//example.com/storage/releases/rhcos-4.x-aarch64")))
        self.assertIn("4.x-aarch64", rhcos.RHCOSBuildFinder(self.runtime, "4.9", "aarch64").rhcos_release_url())
        self.assertIn("4.9-s390x", rhcos.RHCOSBuildFinder(self.runtime, "4.9", "s390x").rhcos_release_url())

    @patch('urllib.request.urlopen')
    def test_build_id(self, mock_urlopen):
        builds = [{'id': 'id-1'}, {'id': 'id-2'}]
        _urlopen_json_cm(mock_urlopen, dict(builds=builds))
        self.assertEqual('id-1', rhcos.RHCOSBuildFinder(self.runtime, "4.4")._latest_rhcos_build_id())
        self.assertIn('/rhcos-4.4/', mock_urlopen.call_args_list[0][0][0])

    @patch('urllib.request.urlopen')
    def test_build_id_no_builds(self, mock_urlopen):
        _urlopen_json_cm(mock_urlopen, dict(builds=[]))
        self.assertIsNone(rhcos.RHCOSBuildFinder(self.runtime, "4.2", "ppc64le")._latest_rhcos_build_id())
        self.assertIn('/rhcos-4.2-ppc64le/', mock_urlopen.call_args_list[0][0][0])

    @patch('urllib.request.urlopen')
    def test_build_id_multi(self, mock_urlopen):
        builds = [{'id': 'id-1', 'arches': ['arch1', 'arch2']}, {'id': 'id-2', 'arches': ['arch1', 'arch2', 'arch3']}]
        _urlopen_json_cm(mock_urlopen, dict(builds=builds))
        self.runtime.group_config.urls = Model(dict(rhcos_release_base=dict(multi='some_url')))
        self.runtime.group_config.arches = ['arch1', 'arch2', 'arch3']
        self.assertEqual('id-2', rhcos.RHCOSBuildFinder(self.runtime, "4.4")._latest_rhcos_build_id())

    @patch('urllib.request.urlopen')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_build_id_build_release_job_completes(self, rhcos_build_meta, mock_urlopen):  # XXX: Change name
        # If not all required attributes exist, which can happen if the rhcos release job did not successfully complete, take the previous
        self.runtime.group_config.rhcos = Model(dict(payload_tags=[dict(name="spam", build_metadata_key="spam"),
                                                                   dict(name="eggs", primary=True, build_metadata_key="eggs")]))

        def mock_rhcos_build_meta(build_id, arch=None):
            # arch1 of id-1 is complete, arch2 is incomplete
            # both arches of id-2 are complete
            # id-2 should be picked up
            if build_id == 'id-1':
                if arch == 'arch1':
                    return {'spam': 'sha:123', 'eggs': 'sha:kkk'}
                if arch == 'arch2':
                    return {'spam': 'sha:123'}
            if build_id == 'id-2':
                return {'spam': 'sha:345', 'eggs': 'sha:789'}

        rhcos_build_meta.side_effect = mock_rhcos_build_meta
        builds = [{'id': 'id-1', 'arches': ['arch1', 'arch2']}, {'id': 'id-2', 'arches': ['arch1', 'arch2']}]
        _urlopen_json_cm(mock_urlopen, dict(builds=builds))
        self.runtime.group_config.urls = Model(dict(rhcos_release_base=dict(multi='some_url')))
        self.runtime.group_config.arches = ['arch1', 'arch2']
        self.assertEqual('id-2', rhcos.RHCOSBuildFinder(self.runtime, "4.14")._latest_rhcos_build_id())

    @patch('urllib.request.urlopen')
    def test_build_find_failure(self, mock_urlopen):
        mock_urlopen.side_effect = URLError("test")
        with self.assertRaises(rhcos.RHCOSNotFound):
            rhcos.RHCOSBuildFinder(self.runtime, "4.9")._latest_rhcos_build_id()

    @patch('doozerlib.rhcos.RHCOSBuildFinder.latest_rhcos_build_id')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_latest_container(self, meta_mock, id_mock):
        # "normal" lookup
        id_mock.return_value = "dummy"
        meta_mock.return_value = dict(oscontainer=dict(image="test", digest="sha256:1234abcd"))
        self.assertEqual(("dummy", "test@sha256:1234abcd"), rhcos.RHCOSBuildFinder(self.runtime, "4.4").latest_container())

        # lookup when there is no build to look up
        id_mock.return_value = None
        self.assertEqual((None, None), rhcos.RHCOSBuildFinder(self.runtime, "4.4").latest_container())

        # lookup when we have configured a different primary container
        self.runtime.group_config.rhcos = Model(dict(payload_tags=[dict(name="spam"), dict(name="eggs", primary=True)]))
        id_mock.return_value = "dummy"
        meta_mock.return_value = dict(
            oscontainer=dict(image="test", digest="sha256:1234abcdstandard"),
            altcontainer=dict(image="test", digest="sha256:abcd1234alt"),
        )
        alt_container = dict(name="rhel-coreos-8", build_metadata_key="altcontainer", primary=True)
        self.runtime.group_config.rhcos = Model(dict(payload_tags=[alt_container]))
        self.assertEqual(("dummy", "test@sha256:abcd1234alt"), rhcos.RHCOSBuildFinder(self.runtime, "4.4").latest_container())

    @patch('doozerlib.exectools.cmd_assert')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_rhcos_build_inspector(self, rhcos_build_meta_mock, cmd_assert_mock):
        """
        Tests the RHCOS build inspector abstraction to ensure it correctly parses and utilizes
        pre-canned data.
        """
        # Data source: https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/?stream=releases/rhcos-4.7-s390x&release=47.83.202107261211-0#47.83.202107261211-0
        rhcos_meta = json.loads(self.respath.joinpath('rhcos1', '47.83.202107261211-0.meta.json').read_text())
        rhcos_commitmeta = json.loads(self.respath.joinpath('rhcos1', '47.83.202107261211-0.commitmeta.json').read_text())
        # NOTE: loading and parsing these fixtures can take a few seconds, no cause for concern
        rpm_defs = yaml.safe_load(self.respath.joinpath('rhcos1', '47.83.202107261211-0.rpm_defs.yaml').read_text())
        pkg_build_dicts = yaml.safe_load(self.respath.joinpath('rhcos1', '47.83.202107261211-0.pkg_builds.yaml').read_text())

        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]
        cmd_assert_mock.return_value = ('{"config": {"config": {"Labels": {"version": "47.83.202107261211-0"}}}}', None)
        test_digest = 'sha256:spamneggs'
        test_pullspec = f'somereg/somerepo@{test_digest}'
        pullspecs = {'machine-os-content': test_pullspec}

        rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, pullspecs, 's390x')
        self.assertEqual(rhcos_build.brew_arch, 's390x')
        self.assertEqual(rhcos_build.get_container_pullspec(), test_pullspec)

        self.assertEqual(rhcos_build.stream_version, '4.7')
        self.assertEqual(rhcos_build.get_rhel_base_version(), 8)

        def canned_getRPM(nvra, *_, **__):
            return rpm_defs[nvra]

        def canned_getBuild(build_id, *_, **__):
            return pkg_build_dicts[build_id]

        koji_multicall = self.koji_mock.multicall.return_value.__enter__.return_value
        koji_multicall.getRPM.side_effect = lambda n, *_, **__: Mock(result=canned_getRPM(n))
        koji_multicall.getBuild.side_effect = lambda b, *_, **__: Mock(result=canned_getBuild(b))

        self.assertIn("util-linux-2.32.1-24.el8.s390x", rhcos_build.get_rpm_nvras())
        self.assertIn("util-linux-2.32.1-24.el8", rhcos_build.get_rpm_nvrs())
        self.assertEqual(rhcos_build.get_package_build_objects()['dbus']['nvr'], 'dbus-1.12.8-12.el8_3')
        self.assertEqual(rhcos_build.get_container_digest(), test_digest)

    @patch('doozerlib.exectools.cmd_assert')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_rhcos_build_inspector_extension(self, rhcos_build_meta_mock, cmd_assert_mock):
        """
        Tests the RHCOS build inspector to ensure it additionally includes RPMs from extensions.
        """
        # Data source: https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/storage/prod/streams/4.13/builds/413.86.202212021619-0/x86_64/commitmeta.json
        rhcos_meta = json.loads(self.respath.joinpath('rhcos2', '4.13-meta.json').read_text())
        rhcos_commitmeta = json.loads(self.respath.joinpath('rhcos2', '4.13-commitmeta.json').read_text())
        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]

        pullspecs = {'machine-os-content': 'somereg/somerepo@sha256:spamneggs'}
        cmd_assert_mock.return_value = ('{"config": {"config": {"Labels": {"version": "412.86.bogus"}}}}', None)

        rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, pullspecs, 'x86_64')

        self.assertIn("kernel-rt-core-4.18.0-372.32.1.rt7.189.el8_6.x86_64", rhcos_build.get_rpm_nvras())
        self.assertIn("kernel-rt-core-4.18.0-372.32.1.rt7.189.el8_6", rhcos_build.get_rpm_nvrs())
        self.assertIn("qemu-img-6.2.0-11.module+el8.6.0+16538+01ea313d.6",
                      rhcos_build.get_rpm_nvrs())  # epoch stripped

    @patch('doozerlib.exectools.cmd_assert')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    def test_inspector_get_container_pullspec(self, rhcos_build_meta_mock, cmd_assert_mock):
        # mock out the things RHCOSBuildInspector calls in __init__
        rhcos_meta = {"buildid": "412.86.bogus"}
        rhcos_commitmeta = {}
        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]
        cmd_assert_mock.return_value = ('{"config": {"config": {"Labels": {"version": "412.86.bogus"}}}}', None)
        pullspecs = {'machine-os-content': 'spam@eggs'}
        rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, pullspecs, 's390x')

        # test its behavior on misconfiguration / edge case
        container_conf = dict(name='spam', build_metadata_key='eggs')
        with self.assertRaises(rhcos.RhcosMissingContainerException):
            rhcos_build.get_container_pullspec(Model(container_conf))

    @patch('doozerlib.exectools.cmd_assert')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    async def test_find_non_latest_rpms_with_missing_enabled_repos(self, rhcos_build_meta_mock, cmd_assert_mock):
        # mock out the things RHCOSBuildInspector calls in __init__
        rhcos_meta = {"buildid": "412.86.bogus"}
        rhcos_commitmeta = {}
        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]
        cmd_assert_mock.return_value = ('{"config": {"config": {"Labels": {"version": "412.86.bogus"}}}}', None)
        pullspecs = {'machine-os-content': 'spam@eggs'}
        self.runtime.group_config.rhcos = Model({})
        rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, pullspecs, 's390x')
        with self.assertRaises(ValueError):
            await rhcos_build.find_non_latest_rpms()

    @patch('doozerlib.rhcos.RHCOSBuildInspector.get_os_metadata_rpm_list')
    @patch("doozerlib.repos.Repo.list_rpms")
    @patch('doozerlib.exectools.cmd_assert')
    @patch('doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta')
    async def test_find_non_latest_rpms(self, rhcos_build_meta_mock: Mock, cmd_assert_mock: Mock,
                                        list_rpms: AsyncMock, get_os_metadata_rpm_list: Mock):
        # mock out the things RHCOSBuildInspector calls in __init__
        rhcos_meta = {"buildid": "412.86.bogus"}
        rhcos_commitmeta = {}
        rhcos_build_meta_mock.side_effect = [rhcos_meta, rhcos_commitmeta]
        cmd_assert_mock.return_value = ('{"config": {"config": {"Labels": {"version": "412.86.bogus"}}}}', None)
        pullspecs = {'machine-os-content': 'spam@eggs'}
        self.runtime.group_config.rhcos = Model({
            "enabled_repos": ["rhel-8-baseos-rpms", "rhel-8-appstream-rpms"]
        })
        repos = Repos(
            {
                "rhel-8-baseos-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
                "rhel-8-appstream-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
                "rhel-8-rt-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
            },
            ["x86_64", "s390x", "ppc64le", "aarch64"]
        )
        runtime = MagicMock(
            repos=repos,
            group_config=Model({
                "rhcos": {"enabled_repos": ["rhel-8-baseos-rpms", "rhel-8-appstream-rpms"]}
            })
        )
        list_rpms.return_value = [
            {'name': 'foo', 'version': '1.0.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'foo-1.0.0-1.el9'},
            {'name': 'bar', 'version': '1.1.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'bar-1.1.0-1.el9'},
        ]
        get_os_metadata_rpm_list.return_value = [
            ['foo', '0', '1.0.0', '1.el9', 'x86_64'],
            ['bar', '0', '1.0.0', '1.el9', 'x86_64'],
        ]
        rhcos_build = rhcos.RHCOSBuildInspector(runtime, pullspecs, 'x86_64')
        actual = await rhcos_build.find_non_latest_rpms()
        list_rpms.assert_awaited()
        get_os_metadata_rpm_list.assert_called_once_with()
        self.assertEqual(actual, [('bar-1.0.0-1.el9', 'bar-1.1.0-1.el9', 'rhel-8-baseos-rpms')])


if __name__ == "__main__":
    unittest.main()
