import unittest
from flexmock import flexmock
from doozerlib import distgit


class TestDistgitRecursiveOverwrite(unittest.TestCase):
    """
    Mocking exectools.cmd_assert to prevent actual command executions, since
    the only purpose of these tests is to ensure that the correct command
    string is being generated.
    """

    def test_without_ignore_set(self):
        expected_cmd = ("rsync -av "
                        " --exclude .git "
                        " my-source/ my-dest/")

        (flexmock(distgit.exectools)
            .should_receive("cmd_assert")
            .with_args(expected_cmd, retries=3)
            .once())

        distgit.recursive_overwrite("my-source", "my-dest")

    def test_with_ignore_set(self):
        expected_cmd = ("rsync -av "
                        " --exclude .git "
                        " --exclude=\"ignore\" "
                        " --exclude=\"me\" "
                        " my-source/ my-dest/")

        (flexmock(distgit.exectools)
            .should_receive("cmd_assert")
            .with_args(expected_cmd, retries=3)
            .once())

        # passing a list to ignore instead of a set, because sets are unordered,
        # making this assertion unpredictable.
        distgit.recursive_overwrite("my-source", "my-dest", ignore=["ignore", "me"])


if __name__ == '__main__':
    unittest.main()
