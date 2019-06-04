import unittest
import mock
import distgit


class TestDistgitPullImage(unittest.TestCase):
    """
    Mocking exectools.cmd_gather to prevent actual command executions.
    Also mocking time.sleep for faster tests.
    """

    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    def test_generate_podman_command(self, cmd_gather_mock):
        expected_cmd = ["podman", "pull", "my-image"]

        distgit.pull_image("my-image")
        cmd_gather_mock.assert_called_with(expected_cmd)

    @mock.patch("distgit.time.sleep", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(1, "", ""))
    def test_pull_fails_more_than_3_times(self, *_):
        expected_exception = distgit.exectools.RetryException
        self.assertRaises(expected_exception, distgit.pull_image, "my-image")

    @mock.patch("distgit.time.sleep", return_value=None)
    @mock.patch("distgit.logger.info", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather")
    def test_custom_logging(self, cmd_gather_mock, logger_info_mock, *_):
        # pretending the cmd failed twice and succeeded on the third attempt.
        cmd_gather_mock.side_effect = [(1, "", ""), (1, "", ""), (0, "", "")]

        expected_logging_calls = [
            mock.call("Pulling image: my-image"),
            mock.call("Error pulling image my-image -- retrying in 60 seconds"),
            mock.call("Error pulling image my-image -- retrying in 60 seconds"),
        ]

        distgit.pull_image("my-image")
        self.assertEqual(expected_logging_calls, logger_info_mock.mock_calls)
