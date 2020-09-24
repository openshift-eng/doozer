import asyncio
import json
import unittest
from unittest.mock import MagicMock, patch

import mock
from flexmock import flexmock

from doozerlib import rhcos


def _urlopen_json_cm(mock_urlopen, content, rc=200):
    # serializes content as json and has the urlopen context manager return it
    cm = MagicMock()
    cm.getcode.return_value = rc
    cm.read.return_value = bytes(json.dumps(content), 'utf-8')
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm


class TestRhcos(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_release_url(self):
        self.assertIn("4.6-s390x", rhcos.rhcos_release_url("4.6", "s390x"))
        self.assertNotIn("x86_64", rhcos.rhcos_release_url("4.6", "x86_64"))

    @patch('urllib.request.urlopen')
    def test_build_id(self, mock_urlopen):
        _urlopen_json_cm(mock_urlopen, dict(builds=['id-1', 'id-2']))
        self.assertEqual('id-1', rhcos._latest_rhcos_build_id("4.4"))
        self.assertIn('/rhcos-4.4/', mock_urlopen.call_args_list[0][0][0])

        _urlopen_json_cm(mock_urlopen, dict(builds=[]))
        self.assertIsNone(rhcos._latest_rhcos_build_id("4.2", "ppc64le"))
        self.assertIn('/rhcos-4.2-ppc64le/', mock_urlopen.call_args_list[1][0][0])

    @patch('doozerlib.rhcos.latest_rhcos_build_id')
    @patch('doozerlib.rhcos.rhcos_build_meta')
    def test_build_meta(self, meta_mock, id_mock):
        id_mock.return_value = "dummy"
        meta_mock.return_value = dict(oscontainer=dict(image="test", digest="sha256:1234abcd"))
        self.assertEqual(("dummy", "test@sha256:1234abcd"), rhcos.latest_machine_os_content("4.4"))

        id_mock.return_value = None
        self.assertEqual((None, None), rhcos.latest_machine_os_content("4.4"))

    def test_get_rhcos_pullspec_from_image_stream(self):
        is_namespace = "test-ns"
        is_name = "test-is-name"
        rhcos_tag = "tag-2"
        expect = "quay.io/foo/bar:tag-2"
        loop = asyncio.get_event_loop()
        with mock.patch("doozerlib.rhcos.cmd_assert_async") as cmd_assert_async:
            cmd_assert_async.return_value = ("""{
                "apiVersion": "image.openshift.io/v1",
                "kind": "ImageStream",
                "status": {
                    "tags": [
                        {"tag": "tag-1", "items": [{"dockerImageReference": "quay.io/foo/bar:tag-1"}]},
                        {"tag": "tag-2", "items": [{"dockerImageReference": "quay.io/foo/bar:tag-2"}]},
                        {"tag": "tag-3", "items": [{"dockerImageReference": "quay.io/foo/bar:tag-3"}]}
                    ]
                }
            }""", "")
            actual = loop.run_until_complete(rhcos.get_rhcos_pullspec_from_image_stream(is_namespace, is_name, rhcos_tag))
            self.assertEqual(actual, expect)

    def test_get_rhcos_version_arch(self):
        pullspec = "quay.io/foo/bar:tag-2"
        loop = asyncio.get_event_loop()
        with mock.patch("doozerlib.rhcos.cmd_assert_async") as cmd_assert_async:
            cmd_assert_async.return_value = ("""{
                "config": {
                    "config": {
                        "Labels": {"version": "46.1.2.3"}
                    },
                    "architecture": "amd64"
                }
            }""", "")
            version, arch = loop.run_until_complete(rhcos.get_rhcos_version_arch(pullspec))
            self.assertEqual(version, "46.1.2.3")
            self.assertEqual(arch, "x86_64")

    def test_get_rhcos_build_metadata(self):
        version = "46.1.2.3"
        arch = "x86_64"
        metadata = {"foo": "bar"}
        loop = asyncio.get_event_loop()
        with mock.patch("aiohttp.ClientSession") as ClientSession:
            fake_response = ClientSession.return_value.__aenter__.return_value.get.return_value

            fake_response.status = 200
            fake_response.json.return_value = metadata
            meta = loop.run_until_complete(rhcos.get_rhcos_build_metadata(version, arch))
            self.assertEqual(meta, metadata)

            fake_response.status = 403
            meta = loop.run_until_complete(rhcos.get_rhcos_build_metadata(version, arch))
            self.assertEqual(meta, None)

    def test_get_rhcos_pullspec_from_release(self):
        rhcos_tag = "tag-2"
        release = "registry.svc.ci.openshift.org/foo/bar:tag-1"
        loop = asyncio.get_event_loop()
        with mock.patch("doozerlib.rhcos.cmd_assert_async") as cmd_assert_async:
            cmd_assert_async.return_value = ("""{
                "references": {
                    "spec": {
                        "tags": [
                            {"name": "tag-1", "from": {"name": "quay.io/foo/bar:tag-1"}},
                            {"name": "tag-2", "from": {"name": "quay.io/foo/bar:tag-2"}},
                            {"name": "tag-3", "from": {"name": "quay.io/foo/bar:tag-3"}}
                        ]
                    }
                }
            }""", "")
            actual = loop.run_until_complete(rhcos.get_rhcos_pullspec_from_release(release, rhcos_tag))
            self.assertEqual(actual, "quay.io/foo/bar:tag-2")


if __name__ == "__main__":
    unittest.main()
