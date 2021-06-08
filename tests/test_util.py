from __future__ import absolute_import, print_function, unicode_literals
import unittest

from doozerlib import util


class TestUtil(unittest.TestCase):

    def test_isolate_assembly_in_release(self):
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1'), None)
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly'), None)
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.x'), 'x')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.xyz'), 'xyz')
        self.assertEqual(util.isolate_assembly_in_release('1.2.3-y.p.p1.assembly.xyz.el7'), 'xyz')

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


if __name__ == "__main__":
    unittest.main()
