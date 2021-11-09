import unittest
from flexmock import flexmock
from doozerlib import distgit


class TestDistgitPullImage(unittest.TestCase):
    """
    Mocking exectools.cmd_gather to prevent actual command executions.
    Also mocking time.sleep for faster tests.
    """

    def test_generate_podman_command(self):
        expected_cmd = ["podman", "pull", "my-image"]

        (flexmock(distgit.exectools)
            .should_receive("cmd_gather")
            .with_args(expected_cmd)
            .once()
            .and_return((0, "", "")))

        distgit.pull_image("my-image")

    def test_pull_fails_more_than_3_times(self):
        flexmock(distgit.time).should_receive("sleep").replace_with(lambda _: None)

        # simulating a failed command execution
        flexmock(distgit.exectools).should_receive("cmd_gather").and_return((1, "", ""))

        expected_exception = distgit.exectools.RetryException
        self.assertRaises(expected_exception, distgit.pull_image, "my-image")

    def test_custom_logging(self):
        flexmock(distgit.time).should_receive("sleep").replace_with(lambda _: None)

        # pretending the cmd failed twice and succeeded on the third attempt.
        (flexmock(distgit.exectools)
            .should_receive("cmd_gather")
            .and_return((1, "", ""))
            .and_return((1, "", ""))
            .and_return((0, "", "")))

        logger = flexmock(distgit.logger)
        logger.should_receive("info").with_args("Pulling image: my-image").once()
        logger.should_receive("info").with_args("Error pulling image my-image -- retrying in 60 seconds").twice()

        distgit.pull_image("my-image")


if __name__ == '__main__':
    unittest.main()
