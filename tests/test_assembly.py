import yaml

from unittest import TestCase

from doozerlib.assembly import merger, group_for_assembly, metadata_config_for_assembly
from doozerlib.model import Model, Missing


class TestAssembly(TestCase):

    def setUp(self) -> None:
        releases_yml = """
releases:
  ART_1:
    assembly:
      members:
        rpms:
        - distgit_key: openshift-kuryr
          metadata:  # changes to make the metadata
            content:
              source:
                git:
                  url: git@github.com:jupierce/kuryr-kubernetes.git
                  branch:
                    target: 1_hash
      group:
        arches:
        - x86_64
        - ppc64le
        - s390x
        advisories:
          image: 11
          extras: 12

  ART_2:
    assembly:
      members:
        rpms:
        - distgit_key: openshift-kuryr
          metadata:  # changes to make the metadata
            content:
              source:
                git:
                  url: git@github.com:jupierce/kuryr-kubernetes.git
                  branch:
                    target: 2_hash
      group:
        arches:
        - x86_64
        - s390x
        advisories:
          image: 21

  ART_3:
    assembly:
      basis:
        assembly: ART_2
      group:
        advisories:
          image: 31

  ART_4:
    assembly:
      basis:
        assembly: ART_3
      group:
        advisories!:
          image: 41

  ART_5:
    assembly:
      basis:
        assembly: ART_4
      group:
        arches!:
        - s390x
        advisories!:
          image: 51

  ART_6:
    assembly:
      basis:
        assembly: ART_5
      members:
        rpms:
        - distgit_key: '*'
          metadata:
            content:
              source:
                git:
                  branch:
                    target: customer_6


"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))

    def test_merger(self):
        # First value dominates on primitive
        self.assertEqual(merger(4, 5), 4)
        self.assertEqual(merger('4', '5'), '4')
        self.assertEqual(merger(None, '5'), None)
        self.assertEqual(merger(True, None), True)

        # Dicts are additive
        self.assertEqual(
            merger({'x': 5}, None),
            {'x': 5}
        )

        self.assertEqual(
            merger({'x': 5}, {'y': 6}),
            {'x': 5, 'y': 6}
        )

        # Depth does not matter
        self.assertEqual(
            merger({'r': {'x': 5}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 6}}
        )

        self.assertEqual(
            merger({'r': {'x': 5, 'y': 7}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 7}}
        )

        # ? key provides default only
        self.assertEqual(
            merger({'r': {'x': 5, 'y?': 7}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 6}}
        )

        # ! key dominates completely
        self.assertEqual(
            merger({'r!': {'x': 5}}, {'r': {'y': 6}}),
            {'r': {'x': 5}}
        )

        # Lists are combined, dupes eliminated, and results sorted
        self.assertEqual(
            merger({'r': [1, 2]}, {'r': [1, 3, 4]}),
            {'r': [1, 2, 3, 4]}
        )

        # ! key dominates completely
        self.assertEqual(
            merger({'r!': [1, 2]}, {'r': [3, 4]}),
            {'r': [1, 2]}
        )

    def test_group_for_assembly(self):

        group_config = Model(dict_to_model={
            'arches': [
                'x86_64'
            ],
            'advisories': {
                'image': 1,
                'extras': 1,
            }
        })

        config = group_for_assembly(self.releases_config, 'ART_1', group_config)
        self.assertEqual(len(config.arches), 3)

        config = group_for_assembly(self.releases_config, 'ART_2', group_config)
        self.assertEqual(len(config.arches), 2)

        # 3 inherits from 2 an only overrides advisory value
        config = group_for_assembly(self.releases_config, 'ART_3', group_config)
        self.assertEqual(len(config.arches), 2)
        self.assertEqual(config.advisories.image, 31)
        self.assertEqual(config.advisories.extras, 1)  # Extras never override, so should be from group_config

        # 4 inherits from 3, but sets "advsories!"
        config = group_for_assembly(self.releases_config, 'ART_4', group_config)
        self.assertEqual(len(config.arches), 2)
        self.assertEqual(config.advisories.image, 41)
        self.assertEqual(config.advisories.extras, Missing)

        # 5 inherits from 4, but sets "advsories!" (overriding 4's !) and "arches!"
        config = group_for_assembly(self.releases_config, 'ART_5', group_config)
        self.assertEqual(len(config.arches), 1)
        self.assertEqual(config.advisories.image, 51)

        config = group_for_assembly(self.releases_config, 'not_defined', group_config)
        self.assertEqual(len(config.arches), 1)

    def test_metadata_config_for_assembly(self):

        meta_config = Model(dict_to_model={
            'owners': ['kuryr-team@redhat.com'],
            'content': {
                'source': {
                    'git': {
                        'url': 'git@github.com:openshift-priv/kuryr-kubernetes.git',
                        'branch': {
                            'target': 'release-4.8',
                        }
                    },
                    'specfile': 'openshift-kuryr-kubernetes-rhel8.spec'
                }
            },
            'name': 'openshift-kuryr'
        })

        config = metadata_config_for_assembly(self.releases_config, 'ART_1', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, '1_hash')

        config = metadata_config_for_assembly(self.releases_config, 'ART_5', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, '2_hash')

        config = metadata_config_for_assembly(self.releases_config, 'ART_6', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden. 6 changes branches for all rpms
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, 'customer_6')
