from io import StringIO
from typing import Optional
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from doozerlib.repodata import OutdatedRPMFinder, Repodata, RepodataLoader, Rpm, RpmModule
import xml.etree.ElementTree as ET
from ruamel.yaml import YAML

from doozerlib.rpm_utils import parse_nvr


class TestRpm(TestCase):
    def test_nevra(self):
        rpm = Rpm(name="foo", epoch=1, version="1.2.3", release="1.el9", arch="x86_64")
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_nvr(self):
        rpm = Rpm(name="foo", epoch=1, version="1.2.3", release="1.el9", arch="x86_64")
        self.assertEqual(rpm.nvr, "foo-1.2.3-1.el9")

    def test_compare(self):
        a = Rpm(name="foo", epoch=1, version="1.2.3", release="1.el9", arch="x86_64")
        b = Rpm(name="foo", epoch=1, version="1.10.3", release="1.el9", arch="x86_64")
        self.assertTrue(a.compare(b) < 0)
        a = Rpm(name="foo", epoch=2, version="1.2.3", release="1.el9", arch="x86_64")
        b = Rpm(name="foo", epoch=1, version="1.10.3", release="1.el9", arch="x86_64")
        self.assertTrue(a.compare(b) > 0)
        a = Rpm(name="foo", epoch=0, version="1.2.3", release="1.el9", arch="x86_64")
        b = Rpm(name="foo", epoch=0, version="1.2.3", release="1.el9", arch="aarch64")
        self.assertTrue(a.compare(b) == 0)

    def test_to_dict(self):
        rpm = Rpm(name="foo", epoch=1, version="1.2.3", release="1.el9", arch="x86_64")
        expected = {
            "name": "foo",
            "epoch": "1",
            "version": "1.2.3",
            "release": "1.el9",
            "arch": "x86_64",
            "nvr": "foo-1.2.3-1.el9",
            "nevra": "foo-1:1.2.3-1.el9.x86_64",
        }
        self.assertEqual(rpm.to_dict(), expected)

    def test_from_nevra(self):
        rpm = Rpm.from_nevra("foo-1:1.2.3-1.el9.x86_64")
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_from_dict(self):
        rpm = Rpm.from_dict({
            "name": "foo",
            "epoch": "1",
            "version": "1.2.3",
            "release": "1.el9",
            "arch": "x86_64",
        })
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_from_metadata(self):
        xml = """
        <package type="rpm" xmlns="http://linux.duke.edu/metadata/common">
            <name>foo</name>
            <arch>x86_64</arch>
            <version epoch="1" rel="1.el9" ver="1.2.3" />
        </package>
        """
        metadata = ET.fromstring(xml)
        rpm = Rpm.from_metadata(metadata)
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")


class TestRpmModule(TestCase):
    def test_from_metadata(self):
        yaml_str = """
        document: modulemd
        version: 2
        data:
            name: container-tools
            stream: rhel8
            version: 8030020201124131330
            context: 830d479e
            arch: x86_64
        """
        metadata = YAML(typ="safe").load(StringIO(yaml_str))
        module = RpmModule.from_metadata(metadata)
        self.assertEqual(module.nsvca, "container-tools:rhel8:8030020201124131330:830d479e:x86_64")


class TestRepodata(TestCase):
    def test_from_metadatas(self):
        repo_name = "test-x86_64"
        primary_xml = """<?xml version="1.0" encoding="UTF-8"?>
<metadata packages="2" xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
    <package type="rpm">
        <name>foo</name>
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="1.2.3" />
    </package>
    <package type="rpm">
        <name>bar</name>
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="2.2.3" />
    </package>
</metadata>
"""
        modules_yaml = """
---
document: modulemd
version: 2
data:
    name: aaa
    stream: rhel8
    version: 1
    context: deadbeef
    arch: x86_64
---
document: modulemd
version: 2
data:
    name: bbb
    stream: rhel9
    version: 2
    context: beefdead
    arch: x86_64
"""
        repodata = Repodata.from_metadatas(
            repo_name,
            ET.fromstring(primary_xml),
            YAML(typ="safe").load_all(StringIO(modules_yaml)))
        self.assertEqual(repodata.name, repo_name)
        self.assertEqual(
            [rpm.nevra for rpm in repodata.primary_rpms],
            ["foo-1:1.2.3-1.el9.x86_64", "bar-1:2.2.3-1.el9.x86_64"])
        self.assertEqual(
            [m.nsvca for m in repodata.modules],
            ['aaa:rhel8:1:deadbeef:x86_64', 'bbb:rhel9:2:beefdead:x86_64'])


class TestRepodataLoader(IsolatedAsyncioTestCase):
    @patch("doozerlib.repodata.RepodataLoader._fetch_remote_gzip", autospec=True)
    @patch("aiohttp.ClientSession", autospec=True)
    async def test_load(self, ClientSession: Mock, _fetch_remote_gzip: AsyncMock):
        repo_name = "test-x86_64"
        repo_url = "https://example.com/repos/test/x86_64/os"
        loader = RepodataLoader()
        session = ClientSession.return_value.__aenter__.return_value = Mock(name="session")
        resp = session.get.return_value = AsyncMock(name="get")
        resp.__aenter__.return_value.text.return_value = """<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
  <revision>1689150070</revision>
  <data type="primary">
    <checksum type="sha256">06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef</checksum>
    <open-checksum type="sha256">eae43283481de984a39dd093044889efd72d9ff0501f22f9e0a269e0483b596f</open-checksum>
    <location href="repodata/06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef-primary.xml.gz"/>
    <timestamp>1689149943</timestamp>
    <size>11771165</size>
    <open-size>89221590</open-size>
  </data>
  <data type="modules">
    <checksum type="sha256">454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34</checksum>
    <open-checksum type="sha256">3fec7e44b17d04eb9cedaeb2ed5292dbdc12e3baf631eb43c1e24ed3a25b1b54</open-checksum>
    <location href="repodata/454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34-modules.yaml.gz"/>
    <timestamp>1689150030</timestamp>
    <size>471210</size>
    <open-size>3707575</open-size>
  </data>
</repomd>
"""

        def _fake_fetch_remote_gzip(_, url: Optional[str]):
            primary_xml = """<?xml version="1.0" encoding="UTF-8"?>
<metadata packages="2" xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
    <package type="rpm">
        <name>foo</name>
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="1.2.3" />
    </package>
    <package type="rpm">
        <name>bar</name>
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="2.2.3" />
    </package>
</metadata>
"""
            modules_yaml = """
---
document: modulemd
version: 2
data:
    name: aaa
    stream: rhel8
    version: 1
    context: deadbeef
    arch: x86_64
---
document: modulemd
version: 2
data:
    name: bbb
    stream: rhel9
    version: 2
    context: beefdead
    arch: x86_64
"""
            if not url:
                return b''
            if url.endswith("primary.xml.gz"):
                return primary_xml.encode()
            if url.endswith("modules.yaml.gz"):
                return modules_yaml.encode()
            raise ValueError("url")
        _fetch_remote_gzip.side_effect = _fake_fetch_remote_gzip
        repodata = await loader.load(repo_name, repo_url)
        _fetch_remote_gzip.assert_any_await(ANY, "https://example.com/repos/test/x86_64/os/repodata/06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef-primary.xml.gz")
        _fetch_remote_gzip.assert_any_await(ANY, "https://example.com/repos/test/x86_64/os/repodata/454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34-modules.yaml.gz")
        self.assertEqual(repodata.name, repo_name)
        self.assertEqual(
            [rpm.nevra for rpm in repodata.primary_rpms],
            ["foo-1:1.2.3-1.el9.x86_64", "bar-1:2.2.3-1.el9.x86_64"])
        self.assertEqual(
            [m.nsvca for m in repodata.modules],
            ['aaa:rhel8:1:deadbeef:x86_64', 'bbb:rhel9:2:beefdead:x86_64'])


class TestOutdatedRPMFinder(IsolatedAsyncioTestCase):
    async def test_find_non_latest_rpms_with_no_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = []
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger
        )
        self.assertEqual(actual, [])

    async def test_find_non_latest_rpms_with_non_modular_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = [
            Repodata(
                name="alfa-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("a-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("b-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="bravo-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("b-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:3.0.0-el8.x86_64"),
                    Rpm.from_nevra("d-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
        ]
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger
        )
        expected = [
            ('b-0:1.0.0-el8.x86_64', 'b-0:2.0.0-el8.x86_64', 'alfa-x86_64'),
            ('c-0:1.0.0-el8.x86_64', 'c-0:3.0.0-el8.x86_64', 'bravo-x86_64'),
            ('d-0:1.0.0-el8.x86_64', 'd-0:2.0.0-el8.x86_64', 'bravo-x86_64')
        ]
        self.assertEqual(actual, expected)

    async def test_find_non_latest_rpms_with_modular_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = [
            Repodata(
                name="alfa-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("a-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("b-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="bravo-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("b-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:3.0.0-el8.x86_64"),
                    Rpm.from_nevra("d-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:999.0.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:999.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="charlie-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("e-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:2.0.0-el8.x86_64"),
                ],
                modules=[
                    RpmModule(
                        name="e",
                        stream="1",
                        version=1000,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:1.0.0-el8.x86_64",
                        }
                    ),
                    RpmModule(
                        name="e",
                        stream="1",
                        version=1001,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:1.1.0-el8.x86_64",
                        }
                    ),
                    RpmModule(
                        name="e",
                        stream="1",
                        version=2000,
                        context="whatever2",
                        arch="x86_64",
                        rpms={
                            "e-0:3.0.0-el8.x86_64",
                        }
                    ),
                    RpmModule(
                        name="e",
                        stream="2",
                        version=2000,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:2.0.0-el8.x86_64",
                            "f-0:2.0.0-el8.x86_64",
                        }
                    ),
                ],
            ),
        ]
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger
        )
        expected = [
            ('b-0:1.0.0-el8.x86_64', 'b-0:2.0.0-el8.x86_64', 'alfa-x86_64'),
            ('c-0:1.0.0-el8.x86_64', 'c-0:3.0.0-el8.x86_64', 'bravo-x86_64'),
            ('d-0:1.0.0-el8.x86_64', 'd-0:2.0.0-el8.x86_64', 'bravo-x86_64'),
            ('e-0:1.0.0-el8.x86_64', 'e-0:1.1.0-el8.x86_64', 'charlie-x86_64'),
            ('f-0:1.0.0-el8.x86_64', 'f-0:999.0.0-el8.x86_64', 'bravo-x86_64'),
        ]
        self.assertEqual(actual, expected)
