from __future__ import absolute_import, print_function, unicode_literals
import unittest

from doozerlib import util


class TestUtil(unittest.TestCase):

    def test_isolate_pflag(self):
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p1'), 'p1')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p0'), 'p0')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p2'), None)
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p1.assembly.p'), 'p1')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p0.assembly.test'), 'p0')
        self.assertEqual(util.isolate_pflag_in_release('1.2.3-y.p.p2.assembly.stream'), None)

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


if __name__ == "__main__":
    unittest.main()
