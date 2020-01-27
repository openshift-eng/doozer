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
            'working_dir': '/my/working/dir',
            'rhpkg_config': ''
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
            'user': 'my-user',
            'rhpkg_config': ''
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
            'rhpkg_config': ''
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
        image_ref_mode = 'manifest-list'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator'
        }
        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode, **cached_attrs).update_metadata_manifests_dir()

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
        image_ref_mode = 'manifest-list'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'my-operator'
        }
        operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode, **cached_attrs).update_metadata_manifests_dir()

    def test_find_and_replace_image_versions_by_sha(self):
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
  relatedImages:
    - name: dependency-b
      image: image-registry.svc:5000/openshift/dependency-b@sha256:bbbbbbbbbbbbbb
    - name: dependency-c
      image: image-registry.svc:5000/openshift/dependency-c@sha256:cccccccccccccc
    - name: dependency-d
      image: image-registry.svc:5000/openshift/dependency-d@sha256:dddddddddddddd
    - name: ose-cluster-logging-operator
      image: image-registry.svc:5000/openshift/ose-cluster-logging-operator@sha256:aaaaaaaaaaaaaa
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

        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/ose-cluster-logging-operator:v4.1.6-201901010000', 'manifest-list').and_return('sha256:aaaaaaaaaaaaaa')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-b:v1.1.1-1111', 'manifest-list').and_return('sha256:bbbbbbbbbbbbbb')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-c:v1.1.1-1111', 'manifest-list').and_return('sha256:cccccccccccccc')
        flexmock(operator_metadata.OperatorMetadataBuilder).should_receive('fetch_image_sha').with_args('openshift/dependency-d:v1.1.1-1111', 'manifest-list').and_return('sha256:dddddddddddddd')

        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        image_ref_mode = 'manifest-list'
        cached_attrs = {
            'working_dir': '/tmp',
            'operator_name': 'ose-cluster-logging-operator',
            'operator': type('', (object,), {
                'config': {
                    'update-csv': {
                        'registry': 'image-registry.svc:5000',
                        'manifests-dir': 'manifests'
                    }
                }
            })
        }
        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode, **cached_attrs)
        actual = op_md.find_and_replace_image_versions_by_sha(initial, 'manifest-list')
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
        image_ref_mode = 'by-arch'
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
        op_md = flexmock(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode, **cached_attrs),
            metadata_csv_yaml_filename='/working/dir/my-dev-operator-metadata/manifests/4.2/other.clusterserviceversion.yaml'
        )
        self.assertEqual(op_md.get_file_list_from_operator_art_yaml('x86_64'), [
            '/working/dir/my-dev-operator-metadata/manifests/foo/bar.yaml',
            '/working/dir/my-dev-operator-metadata/manifests/chunky/bacon.yaml',
            '/working/dir/my-dev-operator-metadata/manifests/4.2/other.clusterserviceversion.yaml',
        ])

    def test_get_file_list_from_no_art_yaml(self):
        nvr = '...irrelevant...'
        stream = 'dev'
        runtime = type('TestRuntime', (object,), {
            'group_config': type('', (object,), {'vars': {}}),
        })
        image_ref_mode = 'by-arch'
        op_md = flexmock(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode),
            operator_art_yaml={},
            metadata_csv_yaml_filename='some-file-name',
        )

        self.assertEqual(op_md.get_file_list_from_operator_art_yaml('x86_64'), ['some-file-name'])

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
        image_ref_mode = 'by-arch'
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
        op_md = flexmock(
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode, **cached_attrs),
            metadata_csv_yaml_filename='/working/dir/my-dev-operator-metadata/manifests/4.2/other.clusterserviceversion.yaml'
        )
        self.assertEqual(op_md.get_file_list_from_operator_art_yaml('x86_64'), [
            '/working/dir/my-dev-operator-metadata/manifests/4.2/other.clusterserviceversion.yaml',
            '/working/dir/my-dev-operator-metadata/manifests/filename-with-2-vars-other.yaml'
        ])

    def test_fetch_image_sha_successfully(self):
        expected_cmd = 'skopeo inspect --raw docker://brew-img-host/brew-img-ns/openshift-my-image'

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_gather')
            .with_args(expected_cmd)
            .and_return((0, """
            {
                "manifests": [
                    {
                        "digest": "sha256:3781665ddc79c7519d111fdc60fc881a223df8bb1d870a3b8a683d69ee7e7468",
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "platform": {
                            "architecture": "ppc64le",
                            "os": "linux"
                        },
                        "size": 1371
                    },
                    {
                        "digest": "sha256:8922dea388e2e41ea30fd54f5f80a3530c5bb746c0136321283dd981b1441015",
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "platform": {
                            "architecture": "amd64",
                            "os": "linux"
                        },
                        "size": 1371
                    },
                    {
                        "digest": "sha256:baa138d231382390d24c93300664b767b649bbfd16061fd74a4ad5a18abb9652",
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "platform": {
                            "architecture": "s390x",
                            "os": "linux"
                        },
                        "size": 1371
                    }
                ],
                "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
                "schemaVersion": 2
            }
            """, '')))

        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = flexmock(
            group_config=flexmock(
                urls=flexmock(brew_image_host="brew-img-host", brew_image_namespace="brew-img-ns"),
                insecure_source=False,
            )
        )
        image_ref_mode = 'by-arch'

        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, image_ref_mode)

        self.assertEqual(
            op_md.fetch_image_sha('openshift/my-image', 'x86_64'),
            'sha256:8922dea388e2e41ea30fd54f5f80a3530c5bb746c0136321283dd981b1441015'
        )

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
                'config': {'update-csv': {}},
                'data_obj': type('', (object,), {'data': {'arches': ['s390x']}})
            })
        }
        (operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs)
            .merge_streams_on_top_level_package_yaml())

    def test_get_default_channel(self):
        nvr = '...irrelevant...'
        stream = '...irrelevant...'
        runtime = '...irrelevant...'
        op_md = operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime)

        self.assertEqual(op_md.get_default_channel({'channels': [
            {'name': '4.0'}, {'name': '4.1'},
            {'name': '4.2'}, {'name': 'stable'},
            {'name': 'alpha'}, {'name': '3.11'},
        ]}), '4.2')

        self.assertEqual(op_md.get_default_channel({'channels': [{'name': 'stable'}]}), 'stable')
        self.assertEqual(op_md.get_default_channel({'channels': [{'name': 'alpha'}]}), 'alpha')

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
                'config': {'update-csv': {}},
                'data_obj': type('', (object,), {'data': {'arches': ['s390x']}})
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
                'LABEL version=vX.Y.Z.201908261419.dev',
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
            .with_args('rhpkg  commit -m "Update operator metadata"')
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg push')
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
            .replace_with(lambda *_: '...irrelevant...'))

        (flexmock(operator_metadata.exectools)
            .should_receive('cmd_assert')
            .with_args('timeout 600 rhpkg --user my-user push')
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
        runtime = type('TestRuntime', (object,), {
            'brew_tag': None
        })
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
            operator_metadata.OperatorMetadataBuilder(nvr, stream, runtime, **cached_attrs).metadata_csv_yaml_filename(),
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

    def test_no_operator_art_yaml(self):
        md = operator_metadata.OperatorMetadataBuilder(
            nvr='...irrelevant...',
            stream='...irrelevant...',
            runtime='...irrelevant...',
            working_dir='...irrelevant...',
            operator_name='...irrelevant...',
            operator=flexmock(config={'update-csv': {'manifests-dir': 'csv-dir'}}),
        )
        mock = flexmock(get_builtin_module())
        mock.should_call('open')
        (mock.should_receive('open')
            .and_raise(IOError()))

        self.assertEqual(md.operator_art_yaml, {})

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
            'brew_tag': None,
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
            'brew_tag': None,
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
            .once()
            .and_return(('..', 'irrelevant', '..')))

        operator_metadata.OperatorMetadataLatestBuildReporter('my-operator', runtime).get_latest_build()


class TestOperatorMetadataLatestNvrReporter(unittest.TestCase):
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
    # runtime = MagicMock(
    #     group_config=MagicMock(branch='my-target-branch'),
    #     image_map={
    #         'my-operator': MagicMock(config={})
    #     },
    # )

    def test_unpack_nvr(self):
        nvr_reporter = operator_metadata.OperatorMetadataLatestNvrReporter('package-container-v1.2.3-20191022', 'dev', self.runtime)
        self.assertEquals(nvr_reporter.unpack_nvr(nvr_reporter.operator_nvr), ('package-container', 'v1.2.3', '20191022'))
        # Add test for metadata nvr

    def test_get_latest_build(self):
        nvr_reporter = operator_metadata.OperatorMetadataLatestNvrReporter('my-operator-container-v1.2.3-20191022', 'dev', self.runtime)
        self.assertEquals(nvr_reporter.metadata_component, 'my-operator-metadata-container')

        # test ignoring builds for later operators
        flexmock(nvr_reporter, get_all_builds=[
            'my-operator-metadata-container-v1.2.3.20191022.dev-1',
            'my-operator-metadata-container-v1.2.3.20191022.dev-2',
            'my-operator-metadata-container-v1.2.4.20191029.dev-12',
        ])

        self.assertEqual(nvr_reporter.get_latest_build(), 'my-operator-metadata-container-v1.2.3.20191022.dev-2')

        # test that releases are compared numerically
        flexmock(nvr_reporter, get_all_builds=[
            'my-operator-metadata-container-v1.2.3.20191022.dev-12',
            'my-operator-metadata-container-v1.2.3.20191022.dev-2',
        ])

        self.assertEqual(nvr_reporter.get_latest_build(), 'my-operator-metadata-container-v1.2.3.20191022.dev-12')

        # technically 0 is a valid release... but who would do that?
        flexmock(nvr_reporter, get_all_builds=['my-operator-metadata-container-v1.2.3.20191022.dev-0'])

        self.assertEqual(nvr_reporter.get_latest_build(), 'my-operator-metadata-container-v1.2.3.20191022.dev-0')


class TestChannelVersion(unittest.TestCase):

    def test_version_compare(self):

        def version(raw):
            return operator_metadata.ChannelVersion(raw)

        self.assertTrue(version("5.4") > version("stable"))
        self.assertTrue(version("5.3") > version("4.4"))
        self.assertTrue(version("5.4") == version("5.4"))
        self.assertTrue(version("5.4") < version("6.0"))
        self.assertTrue(version("5.4") < version("5.5"))

        self.assertFalse(version("5.4") < version("stable"))
        self.assertFalse(version("5.3") < version("4.4"))
        self.assertFalse(version("5.4") != version("5.4"))
        self.assertFalse(version("5.4") > version("6.0"))
        self.assertFalse(version("5.4") > version("5.5"))


def get_builtin_module():
    return sys.modules.get('__builtin__', sys.modules.get('builtins'))


if __name__ == '__main__':
    unittest.main()
