import flexmock
import io
import shutil
import sys
import unittest

from doozerlib import operator_metadata, exectools

SAMPLE_BREW_BUILDINFO_STDOUT = """
BUILD: my-operator-container-v0.1.2-201901010000 [123456]
State: COMPLETE
Built by: ocp-build/buildvm.openshift.eng.bos.redhat.com
Source: git://pkgs.devel.redhat.com/containers/my-operator#a1b2c3d4e5f6g7h8
Volume: DEFAULT
Task: none
Finished: Wed, 07 Aug 2019 07:15:16 CEST
Tags: rhaos-4.2-rhel-7-candidate
"""


class TestOperatorMetadataBuilder(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_update_metadata_repo(self):
        # @TODO: test this method
        pass

    def test_build_metadata_container(self):
        # @TODO: test this method
        pass

    def test_clone_repo(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'working_dir': '/my/working/dir'
        })

        sample_dir_obj = operator_metadata.pushd.Dir('/tmp')
        (flexmock(operator_metadata.pushd)
            .should_receive('Dir')
            .with_args('/my/working/dir/distgits/containers')
            .replace_with(lambda *_: sample_dir_obj))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg clone containers/my-repo --branch my-branch')
            .replace_with(lambda *_: '...irrelevant...'))

        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).clone_repo('my-repo', 'my-branch')

    def test_clone_repo_with_rhpkg_user(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'working_dir': '/my/working/dir',
            'user': 'my-user'
        })

        sample_dir_obj = operator_metadata.pushd.Dir('/tmp')
        (flexmock(operator_metadata.pushd)
            .should_receive('Dir')
            .with_args('/my/working/dir/distgits/containers')
            .replace_with(lambda *_: sample_dir_obj))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg --user my-user clone containers/my-repo --branch my-branch')
            .replace_with(lambda *_: '...irrelevant...'))

        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).clone_repo('my-repo', 'my-branch')

    def test_checkout_repo(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'working_dir': '/my/working/dir',
        })

        sample_dir_obj = operator_metadata.pushd.Dir('/tmp')
        (flexmock(operator_metadata.pushd)
            .should_receive('Dir')
            .with_args('/my/working/dir/distgits/containers/my-repo')
            .replace_with(lambda *_: sample_dir_obj))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('git checkout my-hash')
            .replace_with(lambda *_: '...irrelevant...'))

        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).checkout_repo('my-repo', 'my-hash')

    def test_update_metadata_manifests_dir_metadata_package_yaml_not_present(self):
        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('rm -rf /tmp/my-dev-operator-metadata/manifests/0.1')
            .once())

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('mkdir -p /tmp/my-dev-operator-metadata/manifests')
            .once())

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args((
                'cp -r '
                '/tmp/my-operator/path/to/operator/manifests/0.1 '
                '/tmp/my-dev-operator-metadata/manifests'
            ))
            .once())

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-dev-operator-metadata/manifests/*package.yaml')
            .and_return([]))

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-operator/path/to/operator/manifests/*package.yaml')
            .and_return(['/full/path/to/operator.package.yaml']))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args((
                'cp /full/path/to/operator.package.yaml '
                '/tmp/my-dev-operator-metadata/manifests'
            ))
            .once())

        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = 'dev'
        runtime = type('TestRuntime', (object,), {
            'image_map': {
                'my-operator': type('TestImageMetadata', (object,), {
                    'config': {
                        'update-csv': {
                            'manifests-dir': 'path/to/operator/manifests/'
                        }
                    }
                })
            }
        })
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator'
        }
        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).update_metadata_manifests_dir()

    def test_update_metadata_manifests_dir_metadata_package_yaml_already_present(self):
        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('rm -rf /tmp/my-dev-operator-metadata/manifests/0.1')
            .once())

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('mkdir -p /tmp/my-dev-operator-metadata/manifests')
            .once())

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args((
                'cp -r '
                '/tmp/my-operator/path/to/operator/manifests/0.1 '
                '/tmp/my-dev-operator-metadata/manifests'
            ))
            .once())

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-dev-operator-metadata/manifests/*package.yaml')
            .and_return(['one-item']))

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-operator/path/to/operator/manifests/*package.yaml')
            .times(0))

        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = 'dev'
        runtime = type('TestRuntime', (object,), {
            'image_map': {
                'my-operator': type('TestImageMetadata', (object,), {
                    'config': {
                        'update-csv': {
                            'manifests-dir': 'path/to/operator/manifests/'
                        }
                    }
                })
            }
        })
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator'
        }
        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).update_metadata_manifests_dir()

    def test_replace_version_by_sha_on_image_references(self):
        initial = """
        apiVersion: operators.coreos.com/v1alpha1
        kind: ClusterServiceVersion
        metadata:
          name: clusterlogging.4.1.16-201901010000
          namespace: placeholder
        spec:
          version: 4.1.16-201901010000
          displayName: Cluster Logging
          image: image-registry.svc:5000/openshift/ose-cluster-logging-operator:v4.1.6-201901010000
          some:
            key:
            - item: "image-registry.svc:5000/openshift/dependency-b:v1.1.1-1111"
            - item: "image-registry.svc:5000/openshift/dependency-c:v1.1.1-1111"
            - item: "image-registry.svc:5000/openshift/dependency-d:v1.1.1-1111"
        other:
        - reference: image-registry.svc:5000/openshift/dependency-b:v1.1.1-1111
        """

        expected = """
        apiVersion: operators.coreos.com/v1alpha1
        kind: ClusterServiceVersion
        metadata:
          name: clusterlogging.4.1.16-201901010000
          namespace: placeholder
        spec:
          version: 4.1.16-201901010000
          displayName: Cluster Logging
          image: image-registry.svc:5000/openshift/ose-cluster-logging-operator@sha256:aaaaaaaaaaaaaa
          some:
            key:
            - item: "image-registry.svc:5000/openshift/dependency-b@sha256:bbbbbbbbbbbbbb"
            - item: "image-registry.svc:5000/openshift/dependency-c@sha256:cccccccccccccc"
            - item: "image-registry.svc:5000/openshift/dependency-d@sha256:dddddddddddddd"
        other:
        - reference: image-registry.svc:5000/openshift/dependency-b@sha256:bbbbbbbbbbbbbb
        """

        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/ose-cluster-logging-operator:v4.1.6-201901010000').and_return('sha256:aaaaaaaaaaaaaa')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-b:v1.1.1-1111').and_return('sha256:bbbbbbbbbbbbbb')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-c:v1.1.1-1111').and_return('sha256:cccccccccccccc')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-d:v1.1.1-1111').and_return('sha256:dddddddddddddd')

        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'registry': 'image-registry.svc:5000'
                    }
                }
            })
        }
        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
        actual = op_md.replace_version_by_sha_on_image_references(initial)
        self.assertMultiLineEqual(actual, expected)

    def test_get_file_list_from_operator_art_yaml(self):
        art_yaml_file_contents = io.BytesIO(b"""
        updates:
          - file: foo/bar.yaml
            update_list:
              - search: foo
                replace: bar
          - file: chunky/bacon.yaml
            update_list:
              - search: chunky
                replace: bacon
        """)

        mock = flexmock(get_builtin_module())
        mock.should_call('open')
        (mock.should_receive('open')
            .with_args('/working/dir/my-operator/deploy/olm/manifests/art.yaml')
            .and_return(art_yaml_file_contents))

        nvr = '...irrelevant...'
        stream = 'dev'
        runtime = type('TestRuntime', (object,), {
            'group_config': type('', (object,), {'vars': {}}),
        })
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator',
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'manifests-dir': 'deploy/olm/manifests/'
                    }
                }
            })
        }
        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
        self.assertEqual(op_md.get_file_list_from_operator_art_yaml(), [
            '/working/dir/my-dev-operator-metadata/manifests/foo/bar.yaml',
            '/working/dir/my-dev-operator-metadata/manifests/chunky/bacon.yaml'
        ])

    def test_get_file_list_from_operator_art_yaml_with_variable_replacement(self):
        art_yaml_file_contents = io.BytesIO(b"""
        updates:
          - file: "{MAJOR}.{MINOR}/{OTHERVAR}.clusterserviceversion.yaml"
            update_list:
              - search: foo
                replace: bar
          - file: filename-with-{MINOR}-vars-{OTHERVAR}.yaml
            update_list:
              - search: chunky
                replace: bacon
        """)

        mock = flexmock(get_builtin_module())
        mock.should_call('open')
        (mock.should_receive('open')
            .with_args('/working/dir/my-operator/deploy/olm/manifests/art.yaml')
            .and_return(art_yaml_file_contents))

        nvr = '...irrelevant...'
        stream = 'dev'
        runtime = type('TestRuntime', (object,), {
            'group_config': type('', (object,), {'vars': {
                'MAJOR': 4,
                'MINOR': 2,
                'OTHERVAR': 'other'
            }}),
        })
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator',
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'manifests-dir': 'deploy/olm/manifests/'
                    }
                }
            })
        }
        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
        self.assertEqual(op_md.get_file_list_from_operator_art_yaml(), [
            '/working/dir/my-dev-operator-metadata/manifests/4.2/other.clusterserviceversion.yaml',
            '/working/dir/my-dev-operator-metadata/manifests/filename-with-2-vars-other.yaml'
        ])

    def test_fetch_image_sha_successfully(self):
        expected_cmd = 'skopeo inspect docker://brew-img-host/brew-img-ns/openshift-my-image'

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args(expected_cmd)
            .and_return((0, '{"Digest": "shashasha"}', '')))

        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = flexmock(
            group_config=flexmock(
                urls=flexmock(brew_image_host="brew-img-host", brew_image_namespace="brew-img-ns"),
                insecure_source=False,
            )
        )

        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime)

        self.assertEqual(op_md.fetch_image_sha('openshift/my-image'), 'shashasha')

    def test_fetch_image_sha_failed(self):
        # @TODO: test this method
        pass

    def test_merge_streams_on_top_level_package_yaml_channel_already_present(self):
        package_yaml_filename = '/tmp/my-dev-operator-metadata/manifests/my-operator.package.yaml'
        initial_package_yaml_contents = io.BytesIO(b"""
        channels:
          - name: 4.1
            currentCSV: initial-value
          - name: 4.2
            currentCSV: should-remain-unchanged
        """)

        mock = flexmock(get_builtin_module())
        mock.should_call('open')

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-dev-operator-metadata/manifests/*package.yaml')
            .and_return([package_yaml_filename]))

        (mock.should_receive('open')
            .with_args(package_yaml_filename)
            .and_return(initial_package_yaml_contents))

        expected_package_yaml_contents = {
            'channels': [
                {'name': 4.1, 'currentCSV': 'updated-value'},
                {'name': 4.2, 'currentCSV': 'should-remain-unchanged'}
            ],
            'defaultChannel': '4.2'
        }

        (flexmock(operator_metadata.yaml)
            .should_receive('dump')
            .with_args(expected_package_yaml_contents)  # <-- That's the assertion we are interested
            .replace_with(lambda *_: None))

        (mock.should_receive('open')
            .with_args(package_yaml_filename, 'w')
            .and_return(flexmock(write=lambda *_: None, __exit__=None)))

        nvr = 'my-operator-container-v4.1.2-201901010000'
        stream = 'dev'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator',
            'csv': 'updated-value',
            'operator': type('', (object,), {
                'config': {'update-csv': {}}
            })
        }
        (operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
            .merge_streams_on_top_level_package_yaml())

    def test_merge_streams_on_top_level_package_yaml_channel_not_present(self):
        package_yaml_filename = '/tmp/my-dev-operator-metadata/manifests/my-operator.package.yaml'
        initial_package_yaml_contents = io.BytesIO(b"""
        channels:
          - name: 0.2
            currentCSV: should-remain-unchanged
        """)

        mock = flexmock(get_builtin_module())
        mock.should_call('open')

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-dev-operator-metadata/manifests/*package.yaml')
            .and_return([package_yaml_filename]))

        (mock.should_receive('open')
            .with_args(package_yaml_filename)
            .and_return(initial_package_yaml_contents))

        expected_package_yaml_contents = {
            'channels': [
                {'name': 0.2, 'currentCSV': 'should-remain-unchanged'},
                {'name': '0.1', 'currentCSV': 'updated-value'}
            ],
            'defaultChannel': '0.1'
        }

        (flexmock(operator_metadata.yaml)
            .should_receive('dump')
            .with_args(expected_package_yaml_contents)  # <-- That's the assertion we are interested
            .replace_with(lambda *_: None))

        (mock.should_receive('open')
            .with_args(package_yaml_filename, 'w')
            .and_return(flexmock(write=lambda *_: None, __exit__=None)))

        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = 'dev'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator',
            'csv': 'updated-value',
            'operator': type('', (object,), {
                'config': {'update-csv': {}}
            })
        }
        (operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
            .merge_streams_on_top_level_package_yaml())

    def test_create_metadata_dockerfile(self):
        # using the real filesystem, because DockerfileParser library keeps
        # opening and closing files at every operation, really hard to mock
        exectools.cmd_assert('mkdir -p /tmp/my-operator')
        exectools.cmd_assert('mkdir -p /tmp/my-dev-operator-metadata')
        with open('/tmp/my-operator/Dockerfile', 'w') as f:
            f.write("""FROM openshift/foo-bar-operator:v0.1.2.20190826.143750
                       ENV SOURCE_GIT_COMMIT=... SOURCE_DATE_EPOCH=00000 BUILD_VERSION=vX.Y.Z

                       ADD deploy/olm-catalog/path/to/manifests /manifests

                       LABEL \
                               com.redhat.component="my-operator-container" \
                               name="openshift/ose-my-operator" \
                               com.redhat.delivery.appregistry="true" \
                               version="vX.Y.Z" \
                               release="201908261419"
            """)

        nvr = '...irrelevant...'
        stream = 'dev'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator'
        }
        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).create_metadata_dockerfile()
        with open('/tmp/my-dev-operator-metadata/Dockerfile', 'r') as f:
            self.assertItemsEqual([l.strip() for l in f.readlines()], [
                'FROM scratch',
                'COPY ./manifests /manifests',
                'LABEL version=vX.Y.Z',
                'LABEL com.redhat.delivery.appregistry=true',
                'LABEL name=openshift/ose-my-operator-metadata',
                'LABEL com.redhat.component=my-operator-metadata-container',
            ])

        # Cleaning up
        shutil.rmtree('/tmp/my-operator')
        shutil.rmtree('/tmp/my-dev-operator-metadata')

    def test_commit_and_push_metadata_repo(self):
        sample_dir_obj = operator_metadata.pushd.Dir('/tmp')
        (flexmock(operator_metadata.pushd)
            .should_receive('Dir')
            .with_args('/my/working/dir/my-stage-operator-metadata')
            .replace_with(lambda *_: sample_dir_obj))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('git add .')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('rhpkg commit -m "Update operator metadata"')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg push')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        nvr = '...irrelevant...'
        stream = 'stage'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/my/working/dir',
            'operator_name': 'my-operator'
        }
        (operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
            .commit_and_push_metadata_repo())

    def test_commit_and_push_metadata_repo_with_rhpkg_user(self):
        sample_dir_obj = operator_metadata.pushd.Dir('/tmp')
        (flexmock(operator_metadata.pushd)
            .should_receive('Dir')
            .with_args('/my/working/dir/my-prod-operator-metadata')
            .replace_with(lambda *_: sample_dir_obj))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('git add .')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('rhpkg --user my-user commit -m "Update operator metadata"')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg --user my-user push')
            .once()
            .replace_with(lambda *_: '...irrelevant...'))

        nvr = '...irrelevant...'
        stream = 'prod'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/my/working/dir',
            'operator_name': 'my-operator',
            'rhpkg_user': 'my-user'
        }
        (operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
            .commit_and_push_metadata_repo())

    def test_metadata_package_yaml_exists(self):
        nvr = '...irrelevant...'
        stream = 'stage'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator'
        }

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/working/dir/my-stage-operator-metadata/manifests/*package.yaml')
            .and_return(['one-item']))

        self.assertTrue(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_package_yaml_exists()
        )

    def test_metadata_package_yaml_does_not_exist(self):
        nvr = '...irrelevant...'
        stream = 'stage'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator'
        }

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/working/dir/my-stage-operator-metadata/manifests/*package.yaml')
            .and_return([]))

        self.assertFalse(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_package_yaml_exists()
        )

    def test_extract_brew_task_id(self):
        rhpkg_container_build_output = ("""
            Created task: 23233205
            Task info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=23233205
        """)
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).extract_brew_task_id(rhpkg_container_build_output),
            '23233205'
        )

    def test_property_working_dir(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'working_dir': '/my/working/dir'
        })
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).working_dir,
            '/my/working/dir/distgits/containers'
        )

    def test_property_rhpkg_user(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'user': 'my_user'
        })
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).rhpkg_user,
            'my_user'
        )

    def test_property_rhpkg_user_without_runtime_user(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
        })
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).rhpkg_user,
            ''
        )

    def test_property_operator_branch(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'group_config': type('TestGroupConfig', (object,), {
                'branch': 'my-branch'
            })
        })
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).operator_branch,
            'my-branch'
        )

    def test_property_target(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator_branch': 'my-operator-branch'
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).target,
            'my-operator-branch-candidate'
        )

    def test_property_operator_name(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'brew_buildinfo': (0, SAMPLE_BREW_BUILDINFO_STDOUT, '')
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator_name,
            'my-operator'
        )

    def test_property_commit_hash(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'brew_buildinfo': (0, SAMPLE_BREW_BUILDINFO_STDOUT, '')
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).commit_hash,
            'a1b2c3d4e5f6g7h8'
        )

    def test_property_operator(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'image_map': {
                'my-operator-name': 'My Operator Object'
            }
        })
        cached_attrs = {
            'operator_name': 'my-operator-name'
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator,
            'My Operator Object'
        )

    def test_property_metadata_name(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator_name': 'my-operator-name'
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_name,
            'my-operator-name-metadata'
        )

    def test_property_metadata_repo(self):
        nvr = '...irrelevant...'
        stream = 'dev'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator_name': 'my-operator'
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_repo,
            'my-dev-operator-metadata'
        )

    def test_property_channel(self):
        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).channel,
            '0.1'
        )

    def test_property_operator_manifests_dir(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'image_map': {
                'my-operator-name': type('TestImageMetadata', (object,), {
                    'config': {
                        'update-csv': {
                            'manifests-dir': 'path/to/operator/manifests/'
                        }
                    }
                })
            }
        })
        cached_attrs = {
            'operator_name': 'my-operator-name'
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator_manifests_dir,
            'path/to/operator/manifests'
        )

    def test_property_metadata_manifests_dir(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).metadata_manifests_dir,
            'manifests'
        )

    def test_property_operator_package_yaml_filename(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = type('TestRuntime', (object,), {
            'image_map': {
                'my-operator-name': type('TestImageMetadata', (object,), {
                    'config': {
                        'update-csv': {
                            'manifests-dir': 'path/to/operator/manifests/'
                        }
                    }
                })
            }
        })
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator-name'
        }

        arg = '/working/dir/my-operator-name/path/to/operator/manifests/*package.yaml'
        ret = '/working/dir/my-operator-name/path/to/operator/manifests/my-operator.package.yaml'
        flexmock(operator_metadata.glob).should_receive('glob').with_args(arg).and_return([ret])

        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator_package_yaml_filename,
            '/working/dir/my-operator-name/path/to/operator/manifests/my-operator.package.yaml'
        )

    def test_property_metadata_package_yaml_filename(self):
        nvr = '...irrelevant...'
        stream = 'prod'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator'
        }

        arg = '/working/dir/my-prod-operator-metadata/manifests/*package.yaml'
        ret = '/working/dir/my-prod-operator-metadata/manifests/my-operator.package.yaml'
        flexmock(operator_metadata.glob).should_receive('glob').with_args(arg).and_return([ret])

        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_package_yaml_filename,
            '/working/dir/my-prod-operator-metadata/manifests/my-operator.package.yaml'
        )

    def test_property_metadata_csv_yaml_filename(self):
        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = 'stage'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/working-dir',
            'operator_name': 'my-operator',
        }

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/working-dir/my-stage-operator-metadata/manifests/0.1/*.clusterserviceversion.yaml')
            .and_return(['/working-dir/my-stage-operator-metadata/manifests/0.1/my-operator.clusterserviceversion.yaml']))

        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_csv_yaml_filename,
            '/working-dir/my-stage-operator-metadata/manifests/0.1/my-operator.clusterserviceversion.yaml'
        )

    def test_property_csv(self):
        metadata_csv_yaml_filename = '/tmp/my-dev-operator-metadata/0.1/my-operator.clusterserviceversion.yaml'
        csv_yaml_file_contents = io.BytesIO(b"""
        metadata:
          name: my-csv
        """)

        (flexmock(operator_metadata.glob)
            .should_receive('glob')
            .with_args('/tmp/my-dev-operator-metadata/manifests/0.1/*.clusterserviceversion.yaml')
            .and_return([metadata_csv_yaml_filename]))

        mock = flexmock(get_builtin_module())
        mock.should_call('open')
        (mock.should_receive('open')
            .with_args(metadata_csv_yaml_filename)
            .and_return(csv_yaml_file_contents))

        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = 'dev'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator',
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).csv,
            'my-csv'
        )

    def test_property_channel_name(self):
        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {}
                }
            })
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).channel_name,
            '0.1'
        )

    def test_property_channel_name_with_override_on_4_1(self):
        nvr = 'my-operator-container-v4.1.2-201901010000'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'channel': 'my-custom-channel-name'
                    }
                }
            })
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).channel_name,
            'my-custom-channel-name'
        )

    def test_property_channel_name_with_override_on_4_2(self):
        nvr = 'my-operator-container-v4.2.2-201901010000'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'channel': 'my-custom-channel-name'
                    }
                }
            })
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).channel_name,
            '4.2'
        )

    def test_property_operator_art_yaml(self):
        art_yaml_file_contents = io.BytesIO(b"""
        updates:
          - file: foo/bar.yaml
            update_list:
              - search: foo
                replace: bar
          - file: chunky/bacon.yaml
            update_list:
              - search: chunky
                replace: bacon
        """)

        mock = flexmock(get_builtin_module())
        mock.should_call('open')
        (mock.should_receive('open')
            .with_args('/working/dir/my-operator/deploy/olm/manifests/art.yaml')
            .and_return(art_yaml_file_contents))

        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'working_dir': '/working/dir',
            'operator_name': 'my-operator',
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'manifests-dir': 'deploy/olm/manifests/'
                    }
                }
            })
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator_art_yaml,
            {
                'updates': [
                    {'file': 'foo/bar.yaml', 'update_list': [{'search': 'foo', 'replace': 'bar'}]},
                    {'file': 'chunky/bacon.yaml', 'update_list': [{'search': 'chunky', 'replace': 'bacon'}]}
                ]
            }
        )

    def test_operator_csv_registry(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        cached_attrs = {
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'registry': 'my.registry.svc:5000'
                    }
                }
            })
        }
        self.assertEqual(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).operator_csv_registry,
            'my.registry.svc:5000'
        )

    def test_get_brew_buildinfo(self):
        nvr = 'my-operator-container-v0.1.2-201901010000'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args('brew buildinfo {}'.format(nvr))
            .replace_with(lambda *_: '...irrelevant...'))

        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime).get_brew_buildinfo()


class TestOperatorMetadataLatestBuildReporter(unittest.TestCase):
    def test_get_latest_build(self):
        runtime = type('TestRuntime', (object,), {
            'group_config': type('TestGroupConfig', (object,), {
                'branch': 'my-target-branch'
            }),
            'image_map': {
                'my-operator': type('TestImageMetadata', (object,), {
                    'config': {}
                })
            }
        })

        expected_cmd = ('brew latest-build '
                        'my-target-branch-candidate '
                        'my-operator-metadata-container '
                        '--quiet')

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args(expected_cmd)
            .and_return(('..', 'irrelevant', '..')))

        operator_metadata.OperatorMetadataLatestBuildReporter('my-operator', runtime).get_latest_build()

    def test_get_latest_build_with_custom_component_name(self):
        runtime = type('TestRuntime', (object,), {
            'group_config': type('TestGroupConfig', (object,), {
                'branch': 'my-target-branch'
            }),
            'image_map': {
                'my-operator': type('TestImageMetadata', (object,), {
                    'config': {
                        'distgit': {
                            'component': 'my-custom-component-name-container'
                        }
                    }
                })
            }
        })

        expected_cmd = ('brew latest-build '
                        'my-target-branch-candidate '
                        'my-custom-component-name-metadata-container '
                        '--quiet')

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args(expected_cmd)
            .and_return(('..', 'irrelevant', '..')))

        operator_metadata.OperatorMetadataLatestBuildReporter('my-operator', runtime).get_latest_build()


def get_builtin_module():
    return sys.modules.get('__builtin__', sys.modules.get('builtins'))
