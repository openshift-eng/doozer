import unittest

import flexmock

import distgit
from model import Model

from .support import TestDistgit


class TestRPMDistGit(TestDistgit):
    def setUp(self):
        super(TestRPMDistGit, self).setUp()
        self.rpm_dg = distgit.RPMDistGitRepo(self.md, autoclone=False)
        self.rpm_dg.runtime.group_config = Model()

    def test_pkg_find_in_spec(self):
        """ Test RPMDistGitRepo._find_in_spec """
        self.assertEqual("", self.rpm_dg._find_in_spec("spec", "(?mx) nothing", "thing"))
        self.assertIn("No thing found", self.stream.getvalue())
        self.assertEqual("SOMEthing", self.rpm_dg._find_in_spec("no\nSOMEthing", "(?imx) ^ (something)", "thing"))

    def test_pkg_distgit_matches_commit(self):
        """
        Check the logic for matching a commit from cgit
        """
        flexmock(self.md).should_receive("fetch_cgit_file").once().and_raise(Exception(""))
        self.assertFalse(self.rpm_dg._matches_commit("anything", {}))

        test_file = u"""
            Version: 42
            Release: 201901020304.git.1.12spam7%{?dist}
            %global commit 5p4mm17y5p4m
        """
        flexmock(self.md).should_receive("fetch_cgit_file").and_return(test_file)
        flexmock(self.rpm_dg).should_receive("_built_or_recent").and_return(None)
        self.assertIsNone(self.rpm_dg._matches_commit("12spam78", {}))  # matches, falls through
        self.assertIsNone(self.rpm_dg._matches_commit("5p4mm17y5p4m", {}))  # matches, falls through
        self.assertFalse(self.rpm_dg._matches_commit("11eggs11", {}))  # doesn't match, returns

        self.assertNotIn("No Release: field found", self.stream.getvalue())
        flexmock(self.md).should_receive("fetch_cgit_file").once().and_return("nothing")
        self.assertFalse(self.rpm_dg._matches_commit("nothing", {}))
        self.assertIn("No Release: field found", self.stream.getvalue())

    def test_pkg_build_or_recent(self):
        flexmock(self.rpm_dg).should_receive("release_is_recent").and_return(None)
        self.rpm_dg.name = "mypkg"

        builds = dict(mypkg=("v1", "r1"))
        self.assertTrue(self.rpm_dg._built_or_recent("v1", "r1", builds))

        builds = dict(mypkg=("v1", "r1.el7"))
        self.assertTrue(self.rpm_dg._built_or_recent("v1", "r1%{?dist}", builds))
        self.assertIsNone(self.rpm_dg._built_or_recent("v2", "r1", builds))


if __name__ == "__main__":
    unittest.main()
