from __future__ import unicode_literals
import unittest
from doozerlib import distgit


class TestDistgitBuildImageRefName(unittest.TestCase):

    def test_ref_name_without_ose_prefix(self):
        actual = distgit.build_image_ref_name('kubefed-operator')
        expected = "openshift/ose-kubefed-operator"

        self.assertEqual(actual, expected)

    def test_ref_name_with_ose_prefix(self):
        actual = distgit.build_image_ref_name('ose-kubefed-operator')
        expected = "openshift/ose-kubefed-operator"

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
