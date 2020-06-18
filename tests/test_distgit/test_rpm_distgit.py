from __future__ import absolute_import, print_function, unicode_literals
import unittest

import flexmock

from doozerlib import distgit, model

from .support import TestDistgit


class TestRPMDistGit(TestDistgit):
    def setUp(self):
        super(TestRPMDistGit, self).setUp()
        self.rpm_dg = distgit.RPMDistGitRepo(self.md, autoclone=False)
        self.rpm_dg.runtime.group_config = model.Model()

    def test_init_with_missing_source_specfile(self):
        metadata = flexmock(config=flexmock(content=flexmock(source=distgit.Missing),
                                            distgit=flexmock(branch="_irrelevant_")),
                            runtime=flexmock(branch="_irrelevant_"),
                            name="_irrelevant_",
                            logger="_irrelevant_")

        try:
            distgit.RPMDistGitRepo(metadata, autoclone=False)
            self.fail("Should have raised a ValueError")
        except ValueError as e:
            expected = "Must specify spec file name for RPMs."
            actual = str(e)
            self.assertEqual(expected, actual)

    def test_pkg_find_in_spec(self):
        """ Test RPMDistGitRepo._find_in_spec """
        self.assertEqual("", self.rpm_dg._find_in_spec("spec", "(?mx) nothing", "thing"))
        self.assertIn("No thing found", self.stream.getvalue())
        self.assertEqual("SOMEthing", self.rpm_dg._find_in_spec("no\nSOMEthing", "(?imx) ^ (something)", "thing"))


if __name__ == "__main__":
    unittest.main()
