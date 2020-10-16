#!/usr/bin/env python3

import unittest
from dockerfile_parse import DockerfileParser
from . import DoozerRunnerTestCase

from doozerlib import image, exectools, model


class TestBasicRebase(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_standard_labels_envs_and_parents(self):
        """
        Asserts standard labels are updated
        - version
        - release
        - commit labels
        Asserts that the default doozer injected environment variables are not present
        """
        target_ocp_build_data_commitish = '4c7701c8ad3f045f1fc1be826d55c5205a3e5b76'
        target_version = 'v5.6.777'
        target_release = '999.p0'
        uuid = '0000'
        upstream_commit_oeb = '5397b55f38a11c61749398f0a3759d4eab5b3960'
        upstream_commit_oeb_short = upstream_commit_oeb[:7]
        upstream_commit_ced = '0f7594616f7ea72e28f065ef2c172fa3d852abcf'
        upstream_commit_ced_short = upstream_commit_ced[:7]
        _, _ = self.run_doozer(
            '--group', f'openshift-4.6@{target_ocp_build_data_commitish}',
            '-i', 'openshift-enterprise-base',
            '-i', 'cluster-etcd-operator',
            '--lock-upstream', 'openshift-enterprise-base', upstream_commit_oeb,
            '--lock-upstream', 'cluster-etcd-operator', upstream_commit_ced,
            '--lock-runtime-uuid', uuid,
            'images:rebase',
            '--version', target_version,
            '--release', '999.p?',
            '-m', 'test message'
        )

        oeb_dfp = DockerfileParser(str(self.distgit_image_path('openshift-enterprise-base').joinpath('Dockerfile')))
        ced_dfp = DockerfileParser(str(self.distgit_image_path('cluster-etcd-operator').joinpath('Dockerfile')))

        for dfp in (oeb_dfp, ced_dfp):
            # Check that version /release are being populated
            self.assertEqual(dfp.labels['version'], target_version)
            self.assertEqual(dfp.labels['release'], target_release)
            self.assertEqual(dfp.envs['BUILD_VERSION'], target_version)
            self.assertEqual(dfp.envs['BUILD_RELEASE'], target_release)
            self.assertEqual(dfp.envs['OS_GIT_MAJOR'], '5')
            self.assertEqual(dfp.envs['OS_GIT_MINOR'], '6')
            self.assertEqual(dfp.envs['OS_GIT_PATCH'], '777')

        self.assertEqual(oeb_dfp.envs['OS_GIT_COMMIT'], upstream_commit_oeb_short)
        self.assertEqual(oeb_dfp.envs['OS_GIT_VERSION'], f'{target_version}-{target_release}-{upstream_commit_oeb_short}'.lstrip('v'))
        self.assertEqual(oeb_dfp.envs['SOURCE_GIT_COMMIT'], upstream_commit_oeb)
        self.assertEqual(oeb_dfp.labels['io.openshift.build.commit.id'], upstream_commit_oeb)
        self.assertEqual(oeb_dfp.labels['io.openshift.build.source-location'], 'https://github.com/openshift/images')
        self.assertEqual(oeb_dfp.labels['io.openshift.build.commit.url'], f'https://github.com/openshift/images/commit/{upstream_commit_oeb}')
        self.assertEqual(len(oeb_dfp.parent_images), 1)
        self.assertEqual(oeb_dfp.parent_images[0], 'openshift/ose-base:ubi8')
        self.assertTrue(f'{target_version}.{uuid}', self.distgit_image_path('openshift-enterprise-base').joinpath('additional-tags').read_text())

        self.assertEqual(ced_dfp.envs['OS_GIT_COMMIT'], upstream_commit_ced_short)
        self.assertEqual(ced_dfp.envs['OS_GIT_VERSION'], f'{target_version}-{target_release}-{upstream_commit_ced_short}'.lstrip('v'))
        self.assertEqual(ced_dfp.envs['SOURCE_GIT_COMMIT'], upstream_commit_ced)
        self.assertEqual(ced_dfp.envs['SOURCE_DATE_EPOCH'], '1603368883')
        self.assertEqual(ced_dfp.labels['io.openshift.build.commit.id'], upstream_commit_ced)
        self.assertEqual(ced_dfp.labels['io.openshift.build.source-location'], 'https://github.com/openshift/cluster-etcd-operator')
        self.assertEqual(ced_dfp.labels['io.openshift.build.commit.url'], f'https://github.com/openshift/cluster-etcd-operator/commit/{upstream_commit_ced}')
        self.assertEqual(ced_dfp.labels['io.openshift.maintainer.product'], 'OpenShift Container Platform')
        self.assertEqual(ced_dfp.labels['io.openshift.maintainer.component'], 'Etcd')
        self.assertEqual(ced_dfp.labels['com.redhat.component'], 'cluster-etcd-operator-container')
        self.assertEqual(ced_dfp.labels['name'], 'openshift/ose-cluster-etcd-operator')
        self.assertEqual(ced_dfp.labels['io.openshift.release.operator'], 'true')
        self.assertEqual(len(ced_dfp.parent_images), 2)
        self.assertEqual(ced_dfp.parent_images[0], 'openshift/golang-builder:rhel_8_golang_1.15')
        self.assertEqual(ced_dfp.parent_images[1], f'openshift/ose-base:{target_version}.{uuid}')
        self.assertTrue(f'{target_version}.{uuid}', self.distgit_image_path('cluster-etcd-operator').joinpath('additional-tags').read_text())


if __name__ == "__main__":
    unittest.main()
