#!/usr/bin/env python3

import unittest
from dockerfile_parse import DockerfileParser
from . import DoozerRunnerTestCase

from doozerlib import image, exectools, model


class TestGoLangRebase(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_golang_builder(self):
        """
        Asserts standard labels are updated
        - version
        - release
        - commit labels
        Asserts that the default doozer injected environment variables are not present
        """
        target_ocp_build_data_commitish = 'f904b67600bfddb67e9013c5efb66beee4848509'
        target_version = 'v1.15.99'
        target_release = '777'
        _, _ = self.run_doozer(
            '--group', f'rhel-8-golang-1.15@{target_ocp_build_data_commitish}',
            '--lock-upstream', 'openshift-golang-builder', target_ocp_build_data_commitish,  # this build checks out ocp-build-data as its upstream
            'images:rebase',
            '--version', target_version,
            '--release', target_release,
            '-m', 'test message'
        )

        dfp = DockerfileParser(str(self.distgit_image_path('openshift-golang-builder').joinpath('Dockerfile')))

        # Check that version /release are being populated
        self.assertEqual(dfp.labels['version'], target_version)
        self.assertEqual(dfp.labels['release'], target_release)

        # Check that bz component information is being populated
        self.assertEqual(dfp.labels['io.openshift.maintainer.product'], 'OpenShift Container Platform')
        self.assertEqual(dfp.labels['io.openshift.maintainer.component'], 'Release')

        # Assert commit information
        self.assertEqual(dfp.labels['io.openshift.build.commit.id'], target_ocp_build_data_commitish)
        self.assertEqual(dfp.labels['io.openshift.build.source-location'], "https://github.com/openshift/ocp-build-data")
        self.assertEqual(dfp.labels['io.openshift.build.commit.url'], f"https://github.com/openshift/ocp-build-data/commit/{target_ocp_build_data_commitish}")

        # Ensure that meta.content.set_build_variables == false  worked. Each of the following variables is
        # injected by doozer unless the set_build_variables is false. This is required for the golang builder
        # since if these variables are set, upstream builds tend to use them as their own version/commit information.
        for env_var_name in 'SOURCE_GIT_COMMIT OS_GIT_COMMIT SOURCE_GIT_URL SOURCE_GIT_TAG OS_GIT_VERSION SOURCE_DATE_EPOCH BUILD_VERSION BUILD_RELEASE SOURCE_GIT_TAG SOURCE_GIT_URL OS_GIT_MAJOR OS_GIT_MINOR OS_GIT_PATCH OS_GIT_TREE_STATE SOURCE_GIT_TREE_STATE'.split():
            self.assertIsNone(dfp.envs.get(env_var_name, None))

        self.assertEqual(dfp.envs.get('VERSION'), '1.15')


if __name__ == "__main__":
    unittest.main()
