import unittest
import mock
from datetime import datetime, timedelta

import distgit
from model import Model

from .support import MockMetadata, MockRuntime, TestDistgit


class TestGenericDistGit(TestDistgit):
    def setUp(self):
        super(TestGenericDistGit, self).setUp()
        self.dg = distgit.DistGitRepo(self.md, autoclone=False)
        self.dg.runtime.group_config = Model()

    def test_init(self):
        """
        Ensure that init creates the object expected
        """
        self.assertIsInstance(self.dg, distgit.DistGitRepo)

    def test_init_with_branch_override(self):
        metadata = mock.Mock()
        metadata.runtime.branch = "original-branch"

        metadata.config.distgit.branch = distgit.Missing
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("original-branch", repo.branch)

        metadata.config.distgit.branch = "new-branch"
        repo = distgit.DistGitRepo(metadata, autoclone=False)
        self.assertEqual("new-branch", repo.branch)

    @mock.patch("distgit.DistGitRepo.clone")
    def test_init_with_autoclone(self, clone_mock):
        """
        Mocking `clone` method, since only `init` is what is under test here.
        """
        distgit.DistGitRepo(self.md)
        clone_mock.assert_called_once()

    def test_logging(self):
        """
        Ensure that logs work
        """
        msg = "Hey there!"
        self.dg.logger.info(msg)

        actual = self.stream.getvalue()

        self.assertIn(msg, actual)

    def test_add_missing_pkgs_succeed(self):
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.ImageDistGitRepo(md, autoclone=False)
        d._add_missing_pkgs("haproxy")

        self.assertEqual(1, len(d.runtime.missing_pkgs))
        self.assertIn("distgit_key image is missing package haproxy", d.runtime.missing_pkgs)

    def test_distgit_is_recent(self):
        scan_freshness = self.dg.runtime.group_config.scan_freshness = Model()
        self.assertFalse(self.dg.release_is_recent("201901020304"))  # not configured

        scan_freshness.release_regex = r'^(....)(..)(..)(..)'
        scan_freshness.threshold_hours = 24
        self.assertFalse(self.dg.release_is_recent("2019"))  # no match by regex

        too_soon = datetime.now() - timedelta(hours=4)
        self.assertTrue(self.dg.release_is_recent(too_soon.strftime('%Y%m%d%H')))
        too_stale = datetime.now() - timedelta(hours=25)
        self.assertFalse(self.dg.release_is_recent(too_stale.strftime('%Y%m%d%H')))


if __name__ == "__main__":
    unittest.main()
