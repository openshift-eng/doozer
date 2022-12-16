import unittest
from pathlib import Path

from unittest.mock import Mock, patch

from doozerlib import distgit, model

from .support import TestDistgit


class TestRPMDistGit(TestDistgit):
    def setUp(self):
        super(TestRPMDistGit, self).setUp()
        self.rpm_dg = distgit.RPMDistGitRepo(self.md, autoclone=False)
        self.rpm_dg.runtime.group_config = model.Model()

    @patch("aiofiles.open")
    @patch("doozerlib.distgit.exectools.cmd_assert_async", return_value=("foo-1.2.3-1", ""))
    @patch("glob.glob", return_value=["/path/to/distgit/foo.spec"])
    async def test_resolve_specfile_async(self, mocked_glob: Mock, mocked_cmd_assert_async: Mock, mocked_open: Mock):
        self.rpm_dg.distgit_dir = "/path/to/distgit"
        mocked_file = mocked_open.return_value.__aenter__.return_value
        mocked_file.__aiter__.return_value = iter(["hello", "%global commit abcdef012345", "world!"])
        actual = await self.rpm_dg.resolve_specfile_async()
        expected = (Path("/path/to/distgit/foo.spec"), ["foo", "1.2.3", "1"], "abcdef012345")
        self.assertEqual(actual, expected)
        mocked_open.assert_called_once_with(Path("/path/to/distgit/foo.spec"), "r")
        mocked_glob.assert_called_once_with(self.rpm_dg.distgit_dir + "/*.spec")
        mocked_cmd_assert_async.assert_called_once_with(["rpmspec", "-q", "--qf", "%{name}-%{version}-%{release}", "--srpm", "--undefine", "dist", "--", Path("/path/to/distgit/foo.spec")], strip=True)


if __name__ == "__main__":
    unittest.main()
