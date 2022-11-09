import unittest

from doozerlib import util
from doozerlib.model import Model


class TestUtil(unittest.TestCase):

    def test_isolate_assembly_in_release(self):
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1'), None)
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly'), None)
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.x'), 'x')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.xyz'), 'xyz')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.xyz.el7'), 'xyz')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.4.9.99.el7'), '4.9.99')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.4.9.el700.hi'), '4.9')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.art12398.el10'), 'art12398')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.art12398.el10'), 'art12398')

    def test_isolate_el_version_in_release(self):
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.9.99.el7'), 7)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.9.el7'), 7)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.art12398.el199'), 199)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.art12398'), None)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.7.e.8'), None)

    def test_isolate_nightly_name_components(self):
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-2019-11-08-213727'), ('4.1', 'x86_64', False))
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-priv-2019-11-08-213727'), ('4.1', 'x86_64', True))
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-s390x-2019-11-08-213727'), ('4.1', 's390x', False))
        self.assertEqual(util.isolate_nightly_name_components('4.9.0-0.nightly-arm64-priv-2021-06-08-213727'), ('4.9', 'aarch64', True))

    def test_isolate_pflag(self):
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p1'), 'p1')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p1.el7'), 'p1')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p0'), 'p0')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p2'), None)
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p1.assembly.p'), 'p1')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p0.assembly.test'), 'p0')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p2.assembly.stream'), None)
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p0.assembly.test.el7'), 'p0')

    def test_convert_remote_git_to_https(self):
        # git@ to https
        self.assertEqual(util.convert_remote_git_to_https('git@github.com:openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

        # https to https (no-op)
        self.assertEqual(util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs'),
                         'https://github.com/openshift/aos-cd-jobs')

        # https to https, remove suffix
        self.assertEqual(util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

        # ssh to https
        self.assertEqual(util.convert_remote_git_to_https('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

    def test_convert_remote_git_to_ssh(self):
        # git@ to https
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # https to https (no-op)
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # https to https, remove suffix
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # ssh to https
        self.assertEqual(util.convert_remote_git_to_ssh('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
                         'git@github.com:openshift/aos-cd-jobs.git')

    def test_extract_version_fields(self):
        self.assertEqual(util.extract_version_fields('1.2.3'), [1, 2, 3])
        self.assertEqual(util.extract_version_fields('1.2'), [1, 2])
        self.assertEqual(util.extract_version_fields('v1.2.3'), [1, 2, 3])
        self.assertEqual(util.extract_version_fields('v1.2'), [1, 2])
        self.assertRaises(IOError, util.extract_version_fields, 'v1.2', 3)
        self.assertRaises(IOError, util.extract_version_fields, '1.2', 3)

    def test_go_arch_suffixes(self):
        expectations = {
            "x86_64": "",
            "amd64": "",
            "aarch64": "-arm64",
            "arm64": "-arm64"
        }
        for arch, suffix in expectations.items():
            self.assertEqual(util.go_suffix_for_arch(arch), suffix)

    def test_brew_arch_suffixes(self):
        expectations = {
            "x86_64": "",
            "amd64": "",
            "aarch64": "-aarch64",
            "arm64": "-aarch64"
        }
        for arch, suffix in expectations.items():
            self.assertEqual(util.brew_suffix_for_arch(arch), suffix)

    def test_bogus_arch_xlate(self):
        with self.assertRaises(Exception):
            util.go_arch_for_brew_arch("bogus")
        with self.assertRaises(Exception):
            util.brew_arch_for_go_arch("bogus")

    def test_find_latest_builds(self):
        builds = [
            {"id": 13, "name": "a-container", "version": "v1.2.3", "release": "3.assembly.stream", "tag_name": "tag1"},
            {"id": 12, "name": "a-container", "version": "v1.2.3", "release": "2.assembly.hotfix_a", "tag_name": "tag1"},
            {"id": 11, "name": "a-container", "version": "v1.2.3", "release": "1.assembly.hotfix_a", "tag_name": "tag1"},
            {"id": 23, "name": "b-container", "version": "v1.2.3", "release": "3.assembly.test", "tag_name": "tag1"},
            {"id": 22, "name": "b-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b", "tag_name": "tag1"},
            {"id": 21, "name": "b-container", "version": "v1.2.3", "release": "1.assembly.stream", "tag_name": "tag1"},
            {"id": 33, "name": "c-container", "version": "v1.2.3", "release": "3", "tag_name": "tag1"},
            {"id": 32, "name": "c-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b", "tag_name": "tag1"},
            {"id": 31, "name": "c-container", "version": "v1.2.3", "release": "1", "tag_name": "tag1"},
        ]
        actual = util.find_latest_builds(builds, "stream")
        self.assertEqual([13, 21, 33], [b["id"] for b in actual])

        actual = util.find_latest_builds(builds, "hotfix_a")
        self.assertEqual([12, 21, 33], [b["id"] for b in actual])

        actual = util.find_latest_builds(builds, "hotfix_b")
        self.assertEqual([13, 22, 32], [b["id"] for b in actual])

        actual = util.find_latest_builds(builds, "test")
        self.assertEqual([13, 23, 33], [b["id"] for b in actual])

        actual = util.find_latest_builds(builds, None)
        self.assertEqual([13, 23, 33], [b["id"] for b in actual])

    def test_isolate_timestamp_in_release(self):
        actual = util.isolate_timestamp_in_release("foo-4.7.0-202107021813.p0.g01c9f3f.el8")
        expected = "202107021813"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.8.0-202106152230.p0.g25122f5.assembly.stream")
        expected = "202106152230"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-1.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202199999999.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("")
        expected = None
        self.assertEqual(actual, expected)

    def test_get_release_name_for_assembly(self):
        releases_config = Model({
            "releases": {
                "4.12.99": {
                    "assembly": {
                        "type": "standard",
                        "basis": {
                            "assembly": "4.12.98",
                        }
                    }
                },
                "4.12.98": {
                    "assembly": {
                        "type": "standard",
                        "basis": {
                            "event": 12345,
                        }
                    }
                },
                "art0000": {
                    "assembly": {
                        "type": "custom",
                        "basis": {
                            "assembly": "4.12.99",
                        }
                    }
                },
                "art0001": {
                    "assembly": {
                        "type": "custom",
                        "basis": {
                            "assembly": "art0000",
                        }
                    }
                },
                "art0002": {
                    "assembly": {
                        "type": "custom",
                        "basis": {
                            "assembly": "art0001",
                            "patch_version": 23,
                        }
                    }
                },
                "art0003": {
                    "assembly": {
                        "type": "custom",
                        "basis": {
                            "assembly": "art0002",
                        }
                    }
                },
            }
        })

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "4.12.99")
        expected = "4.12.99"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "4.12.98")
        expected = "4.12.98"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0000")
        expected = "4.12.99-assembly.art0000"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0001")
        expected = "4.12.99-assembly.art0001"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0002")
        expected = "4.12.23-assembly.art0002"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0003")
        expected = "4.12.23-assembly.art0003"
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
