import logging
import os
import shutil
import tempfile
import unittest
from unittest import IsolatedAsyncioTestCase, mock

from flexmock import flexmock

from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repos

try:
    from importlib import reload
except ImportError:
    pass
from doozerlib import exectools, image, model

TEST_YAML = """---
name: 'openshift/test'
distgit:
  namespace: 'hello'"""

# base only images have have an additional flag
TEST_BASE_YAML = """---
name: 'openshift/test_base'
base_only: true
distgit:
  namespace: 'hello'"""


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger


class TestImageMetadata(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ocp-cd-test-logs")

        self.test_file = os.path.join(self.test_dir, "test_file")
        logging.basicConfig(filename=self.test_file, level=logging.DEBUG)
        self.logger = logging.getLogger()

        self.cwd = os.getcwd()
        os.chdir(self.test_dir)

        test_yml = open('test.yml', 'w')
        test_yml.write(TEST_YAML)
        test_yml.close()

    def tearDown(self):
        os.chdir(self.cwd)

        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.test_dir)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_init(self):
        """
        The metadata object appears to need to be created while CWD is
        in the root of a git repo containing a file called '<name>.yml'
        This file must contain a structure:
           {'distgit': {'namespace': '<value>'}}

        The metadata object requires:
          a type string <image|rpm>
          a Runtime object placeholder

        """

        #
        # Check the logs
        #
        logs = [log.rstrip() for log in open(self.test_file).readlines()]

        expected = 1
        actual = len(logs)
        self.assertEqual(
            expected, actual,
            "logging lines - expected: {}, actual: {}".
            format(expected, actual))

    @unittest.skip("raising AttributeError: 'str' object has no attribute 'base_dir'")
    def test_base_only(self):
        """
        Some images are used only as a base for other images.  These base images
        are not included in a formal release.
        """

        test_base_yml = open('test_base.yml', 'w')
        test_base_yml.write(TEST_BASE_YAML)
        test_base_yml.close()

        rt = MockRuntime(self.logger)
        name = 'test.yml'
        name_base = 'test_base.yml'

        md = image.ImageMetadata(rt, name)
        md_base = image.ImageMetadata(rt, name_base)

        # Test the internal config value (will fail if implementation changes)
        # If the flag is absent, default to false
        self.assertFalse(md.config.base_only)
        self.assertTrue(md_base.config.base_only)

        # Test the base_only property of the ImageMetadata object
        self.assertFalse(md.base_only)
        self.assertTrue(md_base.base_only)

    @unittest.skip("AttributeError: 'str' object has no attribute 'filename'")
    def test_pull_url(self):
        fake_runtime = flexmock(
            get_latest_build_info=lambda: ('openshift-cli', '1.1.1', '8'),
            group_config=flexmock(urls=flexmock(brew_image_namespace='rh-osbs', brew_image_host='brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888')))

        fake_image = flexmock(
            pull_url=image.ImageMetadata.pull_url(),
            runtime=fake_runtime, config=flexmock(name='test'))

        self.assertEqual(fake_image.pull_url(), "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/rh-osbs/openshift-test")

    @unittest.skip("AttributeError: 'str' object has no attribute 'filename'")
    def test_get_latest_build_info(self):
        expected_cmd = ["brew", "latest-build", "rhaos-4.2-rhel-7-build" "go-toolset-1.10"]

        latest_build_output = """
        Build                                     Tag                   Built by
        ----------------------------------------  --------------------  ----------------
        go-toolset-1.10-1.10.3-7.el7              devtools-2018.4-rhel-7  deparker
        """

        (flexmock(exectools)
            .should_receive("cmd_gather")
            .with_args(expected_cmd)
            .once()
            .and_return((0, latest_build_output)))

        test_base_yml = open('test_pull.yml', 'w')
        test_base_yml.write(TEST_BASE_YAML)
        test_base_yml.close()
        rt = MockRuntime(self.logger)
        name = 'test_pull.yml'
        md = image.ImageMetadata(rt, name)
        n, v, r = md.get_latest_build_info()
        self.assertEqual(n, "go-toolset-1.10")
        self.assertEqual(v, "1.10.3")
        self.assertEqual(r, "7")

    def test_get_brew_image_name_short(self):
        image_model = model.Model({
            'name': 'openshift/test',
        })
        data_obj = model.Model({
            'key': 'my-distgit',
            'data': image_model,
            'filename': 'my-distgit.yaml',
        })
        rt = mock.MagicMock()
        imeta = image.ImageMetadata(rt, data_obj)
        self.assertEqual(imeta.get_brew_image_name_short(), 'openshift-test')


class TestArchiveImageInspector(IsolatedAsyncioTestCase):
    @mock.patch("doozerlib.repos.Repo.get_repodata_threadsafe")
    @mock.patch("doozerlib.image.ArchiveImageInspector.get_installed_rpm_dicts")
    @mock.patch("doozerlib.image.ArchiveImageInspector.image_arch")
    @mock.patch("doozerlib.image.ArchiveImageInspector.get_image_meta")
    @mock.patch("doozerlib.image.ArchiveImageInspector.get_brew_build_id")
    async def test_find_non_latest_rpms(self, get_brew_build_id: mock.Mock, get_image_meta: mock.Mock,
                                        image_arch: mock.Mock, get_installed_rpm_dicts: mock.Mock,
                                        get_repodata_threadsafe: mock.AsyncMock):
        runtime = mock.MagicMock(repos=Repos({
            "rhel-8-baseos-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
            "rhel-8-appstream-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
            "rhel-8-rt-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
        }, ["x86_64", "s390x", "ppc64le", "aarch64"]))
        archive = mock.MagicMock()
        brew_build_inspector = mock.MagicMock(autospec=image.BrewBuildImageInspector)
        get_brew_build_id.return_value = 12345
        brew_build_inspector.get_brew_build_id.return_value = 12345
        get_image_meta.return_value = mock.MagicMock(autospec=image.ImageMetadata, config={
            "enabled_repos": ["rhel-8-baseos-rpms", "rhel-8-appstream-rpms"]
        })
        image_arch.return_value = "x86_64"
        get_repodata_threadsafe.return_value = Repodata(
            name='rhel-8-appstream-rpms',
            primary_rpms=[
                Rpm.from_dict({'name': 'foo', 'version': '1.0.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'foo-1.0.0-1.el9'}),
                Rpm.from_dict({'name': 'bar', 'version': '1.1.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'bar-1.1.0-1.el9'}),
            ]
        )
        get_installed_rpm_dicts.return_value = [
            {'name': 'foo', 'version': '1.0.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'foo-1.0.0-1.el9'},
            {'name': 'bar', 'version': '1.0.0', 'release': '1.el9', 'epoch': '0', 'arch': 'x86_64', 'nvr': 'bar-1.0.0-1.el9'},
        ]
        inspector = image.ArchiveImageInspector(runtime, archive, brew_build_inspector)
        actual = await inspector.find_non_latest_rpms()
        get_image_meta.assert_called_once_with()
        get_installed_rpm_dicts.assert_called_once_with()
        get_repodata_threadsafe.assert_awaited()
        self.assertEqual(actual, [('bar-0:1.0.0-1.el9.x86_64', 'bar-0:1.1.0-1.el9.x86_64', 'rhel-8-appstream-rpms')])
