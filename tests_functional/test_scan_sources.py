#!/usr/bin/env python3

import unittest
import yaml
from . import DoozerRunnerTestCase

from doozerlib import image, exectools, model


class TestScanSources(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def _assert_result(self, r, is_changed: bool, reason_contains=[]):
        name = r['name']
        self.assertEqual(r['changed'], is_changed, msg=f'Expected {name} result to have changed={is_changed}')
        for s in reason_contains:
            self.assertTrue(s in r['reason'], msg=f'Change reason for {name} does not contain {s}')

    @staticmethod
    def _get_result_for(results, type, distgit_name):
        for r in results[type]:
            if r['name'] == distgit_name:
                return r
        raise IOError('Unable to find distgit name: ' + distgit_name)

    def test_detect_upstream_changes(self):

        # How do you find all these crazy numbers?
        # Grab the debug log for scan-sources run you are interested in replicating.
        # For every relevant value, cat debug.log | grep "scan-sources coordinate"
        # cat debug.log | grep "scan-sources coordinate" | grep <distgitkey>

        results_yaml, _ = self.run_doozer(
            '--group', 'openshift-4.7@f1ab35e29f32739e76022ecd071830da234e19b1',
            '--brew-event', str(35224480 + 1),  # coordinate "brew_event:"
            '-r', 'openshift',
            '--lock-downstream', 'openshift', '7938260919bf8bd798b3898e656ffcff843b056b',  # coordinate "dg_commit:"
            '--lock-upstream', 'openshift', 'e67f5dcb92ff67ca4f0cade72fd0ef7de757fdd6',  # coordinate "upstream_commit_hash:"
            '-i', 'ose-cluster-config-operator',
            '--lock-downstream', 'ose-cluster-config-operator', '74a8fc7bf72ab86ee249378f1978ab9de6fd2770',
            '--lock-upstream', 'ose-cluster-config-operator', '02d75c708012184dc6bdb54eb2f680a7f2662582',
            '-i', 'ose-machine-api-operator',
            '--lock-downstream', 'ose-machine-api-operator', '2e8d8a721126a876461b21fe0b0defdd2e3a560e',
            '--lock-upstream', 'ose-machine-api-operator', 'c1f0af99a394ea73daa3d4f25d1c325b2173251b',
            'config:scan-sources',
            '--yaml'
        )

        print(self.id() + 'results:\n' + results_yaml)
        results = yaml.safe_load(results_yaml)

        def get_result_for(type, distgit_name):
            return TestScanSources._get_result_for(results, type, distgit_name)

        result_rpm_openshift = get_result_for('rpms', 'openshift')
        # expecting.. Distgit spec file does not contain upstream hash e67f5dcb92ff67ca4f0cade72fd0ef7de757fdd6
        self._assert_result(result_rpm_openshift, True, ['e67f5dcb92ff67ca4f0cade72fd0ef7de757fdd6'])

        result_image_ose_machine_api_operator = get_result_for('images', 'ose-machine-api-operator')
        # expecting.. Distgit contains SOURCE_GIT_COMMIT hash c0534cccc5e282e68fda7a99c58e7678bbe32849
        #         different from upstream HEAD c1f0af99a394ea73daa3d4f25d1c325b2173251b
        self._assert_result(result_image_ose_machine_api_operator, True, ['c0534cccc5e282e68fda7a99c58e7678bbe32849', 'c1f0af99a394ea73daa3d4f25d1c325b2173251b'])

        result_image_ose_cluster_config_operator = get_result_for('images', 'ose-cluster-config-operator')
        self._assert_result(result_image_ose_cluster_config_operator, False)  # Control group; expect no change

    def test_detect_image_rpm_change(self):

        # How do you find all these crazy numbers?
        # Grab the debug log for scan-sources run you are interested in replicating.
        # For every relevant value, cat debug.log | grep "scan-sources coordinate"
        # cat debug.log | grep "scan-sources coordinate" | grep <distgitkey>

        results_yaml, _ = self.run_doozer(
            '--group', 'openshift-4.5@b21620c9d6c721f4030a69b39f60724b024c765d',
            '--brew-event', str(35224766 + 1),  # coordinate "brew_event:"
            '-r', 'openshift',
            '--lock-downstream', 'openshift', 'e87df3907735155a62d600fa18335eab5fad8a80',  # coordinate "dg_commit:"
            '--lock-upstream', 'openshift', '70b8a93e9cdae20828d630d3d2357c79edb8e334',  # coordinate "upstream_commit_hash:"
            '-i', 'jenkins-agent-maven-35-rhel7',
            '--lock-downstream', 'jenkins-agent-maven-35-rhel7', '950040a89294503f603576016e9d1598fc11c1e7',
            '--lock-upstream', 'jenkins-agent-maven-35-rhel7', 'bff850168142a6de2716bf14aa09bcfb40e5eb78',
            'config:scan-sources',
            '--yaml'
        )

        print(self.id() + 'results:\n' + results_yaml)
        results = yaml.safe_load(results_yaml)

        def get_result_for(type, distgit_name):
            return TestScanSources._get_result_for(results, type, distgit_name)

        r = get_result_for('rpms', 'openshift')
        self._assert_result(r, False)  # Just a control group

        r = get_result_for('images', 'jenkins-agent-maven-35-rhel7')
        # Expecting something like: 'Package rh-maven35-jackson-databind has been retagged by potentially
        #         relevant tags since image build: {''rhscl-3.5-rhel-7''}'
        self._assert_result(r, True, ['rh-maven35-jackson-databind', 'rhscl-3.5-rhel-7'])

    def test_detect_parent_image_ripple(self):

        # At this exact moment, openshift-enterprise-cli had an upstream code change.
        # That should trigger images which depend on it to rebuild (cluster-logging-operator).
        results_yaml, _ = self.run_doozer(
            '--group', 'openshift-4.7@f1ab35e29f32739e76022ecd071830da234e19b1',
            '--brew-event', str(35246115 + 1),  # coordinate "brew_event:"
            '-r', 'openshift-clients',
            '--lock-downstream', 'openshift-clients', 'a08a6c176a135ee2dd2d3fffb154579382315ce8',  # coordinate "dg_commit:"
            '--lock-upstream', 'openshift-clients', '9aef6d2c56fb26083112368e877dca34bee161d4',  # coordinate "upstream_commit_hash:"
            '-i', 'openshift-enterprise-cli',
            '--lock-downstream', 'openshift-enterprise-cli', '07df2ef7d844fea74fbe8b1535742884268d33d9',
            '--lock-upstream', 'openshift-enterprise-cli', '9aef6d2c56fb26083112368e877dca34bee161d4',
            '-i', 'cluster-logging-operator',
            '--lock-downstream', 'cluster-logging-operator', 'db34acea03c319dcd9bd731041dcd6333bbb025a',
            '--lock-upstream', 'cluster-logging-operator', '192886641823c745e9a7530d9f7ade18d60919b2',
            'config:scan-sources',
            '--yaml'
        )

        print(self.id() + 'results:\n' + results_yaml)
        results = yaml.safe_load(results_yaml)

        def get_result_for(type, distgit_name):
            return TestScanSources._get_result_for(results, type, distgit_name)

        r = get_result_for('rpms', 'openshift-clients')
        # Distgit spec file does not contain upstream hash 9aef6d2c56fb26083112368e877dca34bee161d4
        self._assert_result(r, True, ['9aef6d2c56fb26083112368e877dca34bee161d4'])

        r = get_result_for('images', 'openshift-enterprise-cli')
        # Distgit contains SOURCE_GIT_COMMIT hash 657671383e03d9ef22c01fb7202fa44ce0a71e18
        #         different from upstream HEAD 9aef6d2c56fb26083112368e877dca34bee161d4
        self._assert_result(r, True, ['657671383e03d9ef22c01fb7202fa44ce0a71e18', '9aef6d2c56fb26083112368e877dca34bee161d4'])

        r = get_result_for('images', 'cluster-logging-operator')
        self._assert_result(r, True, ['openshift-enterprise-cli'])

    def test_detect_has_not_been_built(self):

        #
        results_yaml, _ = self.run_doozer(
            '--group', 'openshift-4.7@f1ab35e29f32739e76022ecd071830da234e19b1',
            '--brew-event', str(35246115 + 1),  # coordinate "brew_event:"
            '-r', 'openshift-clients',
            '--lock-downstream', 'openshift-clients', 'a08a6c176a135ee2dd2d3fffb154579382315ce8',  # coordinate "dg_commit:"
            '--lock-upstream', 'openshift-clients', '9aef6d2c56fb26083112368e877dca34bee161d4',  # coordinate "upstream_commit_hash:"
            '-i', 'vertical-pod-autoscaler-operator',  # up and downstream don't matter; at brew event, this image does not exist for 4.7
            'config:scan-sources',
            '--yaml'
        )

        print(self.id() + 'results:\n' + results_yaml)
        results = yaml.safe_load(results_yaml)

        def get_result_for(type, distgit_name):
            return TestScanSources._get_result_for(results, type, distgit_name)

        r = get_result_for('rpms', 'openshift-clients')
        # Distgit spec file does not contain upstream hash 9aef6d2c56fb26083112368e877dca34bee161d4
        self._assert_result(r, True, ['9aef6d2c56fb26083112368e877dca34bee161d4'])

        r = get_result_for('images', 'vertical-pod-autoscaler-operator')
        self._assert_result(r, True, ['never been built'])


if __name__ == "__main__":
    unittest.main()
