import flexmock
import io
import os
import shutil
import string
import sys
import tempfile
import unittest

from doozerlib import operator_metadata, pushd


class TestOperatorMetadata(unittest.TestCase):

    def setUp(self):
        operator_metadata.working_dir = '/tmp'

    def test_split_nvr(self):
        nvr = 'foo-bar-operator-container-v4.2.0-201908070219'
        name, tag = operator_metadata.split_nvr(nvr)

        self.assertEqual(name, 'foo-bar-operator-container')
        self.assertEqual(tag, 'v4.2.0-201908070219')

    def test_get_channel_from_tag(self):
        actual = operator_metadata.get_channel_from_tag('v4.2.0-201908070219')
        expected = '4.2'
        self.assertEqual(actual, expected)

    def test_clone_repo(self):
        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('rhpkg clone containers/foo --branch dev')
            .and_return(None))

        operator_metadata.clone_repo('foo', 'dev')

    def test_retrieve_commit_hash_from_brew(self):
        nvr = 'foo-bar-operator-container-v4.2.0-201908070219'

        brew_stdout = """
        BUILD: foo-bar-operator-container-v4.2.0-201908070219 [945757]
        State: COMPLETE
        Built by: ocp-build/buildvm.openshift.eng.bos.redhat.com
        Source: git://pkgs.devel.redhat.com/containers/foo-bar-operator#d58608914f52c87705b0e63c8f4f86c970ad9b6a
        Volume: DEFAULT
        Task: none
        Finished: Wed, 07 Aug 2019 07:15:16 CEST
        Tags: rhaos-4.2-rhel-7-candidate
        Extra: ......
        Image archives:
        /mnt/redhat/brewroot/packages/foo-bar-operator-container/v4.2.0/...
        Operator-manifests Archives:
        /mnt/redhat/brewroot/packages/foo-bar-operator-container/v4.2.0/...
        """
        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args('brew buildinfo {}'.format(nvr))
            .once()
            .and_return((0, brew_stdout, '')))

        actual_hash = operator_metadata.retrieve_commit_hash_from_brew(nvr)
        expected_hash = 'd58608914f52c87705b0e63c8f4f86c970ad9b6a'

        self.assertEqual(actual_hash, expected_hash)

    def test_update_metadata_manifests_dir(self):
        # @TODO: test with empty & already populated metadata branch
        pass

    def test_merge_streams_on_top_level_package_yaml(self):
        package_file = 'foo-bar-operator-metadata/manifests/foo-bar.package.yaml'
        package_contents = io.BytesIO(b"""
        channels:
          - name: 4.2
            currentCSV: old-value
          - name: 4.1
            currentCSV: should-remain-unchanged
        """)

        csv_file = 'foo-bar-operator-metadata/manifests/4.2/foo-bar.v4.2.0.clusterserviceversion.yaml'
        csv_contents = io.BytesIO(b"""
        metadata:
          name: new-value
        """)
        expected_new_package_contents = {
            'channels': [
                {
                    'name': 4.2,
                    'currentCSV': 'new-value'
                },
                {
                    'name': 4.1,
                    'currentCSV': 'should-remain-unchanged'
                }
            ]
        }

        # mocking to avoid touching the real filesystem
        mock = flexmock(get_builtin_module())
        mock.should_call('open')

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args(os.path.join(operator_metadata.working_dir, 'foo-bar-operator-metadata/manifests/*.package.yaml'))
            .and_return([package_file]))

        (mock.should_receive('open')
            .with_args(package_file)
            .and_return(package_contents))

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args(os.path.join(operator_metadata.working_dir, 'foo-bar-operator-metadata/manifests/4.2/*.clusterserviceversion.yaml'))
            .and_return([csv_file]))

        (mock.should_receive('open')
            .with_args(csv_file)
            .and_return(csv_contents))

        (flexmock(operator_metadata.yaml)
            .should_receive('dump')
            .with_args(expected_new_package_contents, object)  # <-- That's the assertion we are interested
            .replace_with(lambda *_: None))

        (mock.should_receive('open')
            .with_args(package_file, 'w')
            .and_return(flexmock(__exit__=None)))

        # triggering the merge
        metadata = 'foo-bar-operator-metadata'
        channel = '4.2'
        operator_metadata.merge_streams_on_top_level_package_yaml(metadata, channel)

    def test_create_symlink_to_latest(self):
        # @TODO: test that it should always point to 4.2, even with 4.1.xxx NVRs
        pass

    def test_create_metadata_dockerfile(self):
        # @TODO: DockerfileParser touches the real filesystem everytime
        pass


def get_builtin_module():
    return sys.modules.get('__builtin__', sys.modules.get('builtins'))
