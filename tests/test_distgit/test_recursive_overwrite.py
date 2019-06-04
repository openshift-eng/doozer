import unittest
import mock
import distgit


class TestDistgitRecursiveOverwrite(unittest.TestCase):
    """
    Mocking exectools.cmd_assert to prevent actual command executions, since
    the only purpose of these tests is to ensure that the correct command
    string is being generated.
    """

    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_without_ignore_set(self, cmd_assert_mock):
        expected_cmd = ("rsync -av "
                        " --exclude .git/ "
                        " my-source/ my-dest/")

        distgit.recursive_overwrite("my-source", "my-dest")
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=mock.ANY)

    @mock.patch("distgit.exectools.cmd_assert", return_value=None)
    def test_with_ignore_set(self, cmd_assert_mock):
        expected_cmd = ("rsync -av "
                        " --exclude .git/ "
                        " --exclude=\"ignore\" "
                        " --exclude=\"me\" "
                        " my-source/ my-dest/")

        # passing a list to ignore instead of a set, because sets are unordered,
        # making this assertion unpredictable.
        distgit.recursive_overwrite("my-source", "my-dest", ignore=["ignore", "me"])
        cmd_assert_mock.assert_called_once_with(expected_cmd, retries=mock.ANY)
