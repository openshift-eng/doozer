#!/usr/bin/env python
"""
Test functions related to controlled command execution
"""


import asyncio
import unittest

from unittest import IsolatedAsyncioTestCase, mock

try:
    from importlib import reload
except ImportError:
    pass
import logging
import os
import shutil
import tempfile

from doozerlib import exectools, assertion


class RetryTestCase(IsolatedAsyncioTestCase):
    """
    Test the exectools.retry() method
    """
    ERROR_MSG = r"Giving up after {} failed attempt\(s\)"

    def test_success(self):
        """
        Given a function that passes, make sure it returns successfully with
        a single retry or greater.
        """
        pass_function = lambda: True
        self.assertTrue(exectools.retry(1, pass_function))
        self.assertTrue(exectools.retry(2, pass_function))

    def test_failure(self):
        """
        Given a function that fails, make sure that it raise an exception
        correctly with a single retry limit and greater.
        """
        fail_function = lambda: False
        assertRaisesRegex = self.assertRaisesRegex if hasattr(self, 'assertRaisesRegex') else self.assertRaisesRegexp
        assertRaisesRegex(
            Exception, self.ERROR_MSG.format(1), exectools.retry, 1, fail_function)
        assertRaisesRegex(
            Exception, self.ERROR_MSG.format(2), exectools.retry, 2, fail_function)

    def test_wait(self):
        """
        Verify that the retry fails and raises an exception as needed.
        Further, verify that the indicated wait loops occurred.
        """

        expected_calls = list("fw0fw1f")

        # initialize a collector for loop information
        calls = []

        # loop 3 times, writing into the collector each try and wait
        assertRaisesRegex = self.assertRaisesRegex if hasattr(self, 'assertRaisesRegex') else self.assertRaisesRegexp
        assertRaisesRegex(
            Exception, self.ERROR_MSG.format(3),
            exectools.retry, 3, lambda: calls.append("f"),
            wait_f=lambda n: calls.extend(("w", str(n))))

        # check that the test and wait loop operated as expected
        self.assertEqual(calls, expected_calls)

    def test_return(self):
        """
        Verify that the retry task return value is passed back out faithfully.
        """
        obj = {}
        func = lambda: obj
        self.assertIs(exectools.retry(1, func, check_f=lambda _: True), obj)


class TestCmdExec(unittest.TestCase):
    """
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ocp-cd-test-logs")
        self.test_file = os.path.join(self.test_dir, "test_file")
        logging.basicConfig(filename=self.test_file, level=logging.INFO)
        self.logger = logging.getLogger()

    def tearDown(self):
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.test_dir)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_cmd_assert_success(self):
        """
        """

        try:
            exectools.cmd_assert("/bin/true")
        except IOError as error:
            self.Fail("/bin/truereturned failure: {}".format(error))

        # check that the log file has all of the tests.
        log_file = open(self.test_file, 'r')
        lines = log_file.readlines()
        log_file.close()

        self.assertEqual(len(lines), 4)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_cmd_assert_fail(self):
        """
        """

        # Try a failing command 3 times, at 1 sec intervals
        with self.assertRaises(IOError):
            exectools.cmd_assert("/usr/bin/false", 3, 1)

        # check that the log file has all of the tests.
        log_file = open(self.test_file, 'r')
        lines = log_file.readlines()
        log_file.close()

        self.assertEqual(len(lines), 12)


class TestGather(IsolatedAsyncioTestCase):
    """
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ocp-cd-test-logs")
        self.test_file = os.path.join(self.test_dir, "test_file")
        logging.basicConfig(filename=self.test_file, level=logging.INFO)
        self.logger = logging.getLogger()

    def tearDown(self):
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.test_dir)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_gather_success(self):
        """
        """

        (status, stdout, stderr) = exectools.cmd_gather(
            "/usr/bin/echo hello there")
        status_expected = 0
        stdout_expected = "hello there\n"
        stderr_expected = ""

        self.assertEqual(status_expected, status)
        self.assertEqual(stdout, stdout_expected)
        self.assertEqual(stderr, stderr_expected)

        # check that the log file has all of the tests.

        log_file = open(self.test_file, 'r')
        lines = log_file.readlines()

        self.assertEqual(len(lines), 6)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_gather_fail(self):
        """
        """

        (status, stdout, stderr) = exectools.cmd_gather(
            ["/usr/bin/sed", "-e", "f"])

        status_expected = 1
        stdout_expected = ""
        stderr_expected = "/usr/bin/sed: -e expression #1, char 1: unknown command: `f'\n"

        self.assertEqual(status_expected, status)
        self.assertEqual(stdout, stdout_expected)
        self.assertEqual(stderr, stderr_expected)

        # check that the log file has all of the tests.
        log_file = open(self.test_file, 'r')
        lines = log_file.readlines()

        self.assertEqual(len(lines), 6)

    async def test_cmd_gather_async(self):
        cmd = ["uname", "-a"]
        fake_cwd = "/foo/bar"
        fake_stdout = b"fake_stdout"
        fake_stderr = b"fake_stderr"
        with mock.patch("asyncio.create_subprocess_exec") as create_subprocess_exec, \
                mock.patch("doozerlib.pushd.Dir.getcwd", return_value=fake_cwd):
            proc = create_subprocess_exec.return_value
            proc.returncode = 0
            proc.communicate.return_value = (fake_stdout, fake_stderr)

            rc, out, err = await exectools.cmd_gather_async(cmd, text_mode=True)
            create_subprocess_exec.assert_called_once_with(*cmd, cwd=fake_cwd, env=None, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.DEVNULL)
            self.assertEqual(rc, 0)
            self.assertEqual(out, fake_stdout.decode("utf-8"))
            self.assertEqual(err, fake_stderr.decode("utf-8"))

            create_subprocess_exec.reset_mock()
            rc, out, err = await exectools.cmd_gather_async(cmd, text_mode=False)
            create_subprocess_exec.assert_called_once_with(*cmd, cwd=fake_cwd, env=None, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.DEVNULL)
            self.assertEqual(rc, 0)
            self.assertEqual(out, fake_stdout)
            self.assertEqual(err, fake_stderr)

    async def test_cmd_assert_async(self):
        cmd = ["uname", "-a"]
        fake_cwd = "/foo/bar"
        with mock.patch("doozerlib.exectools.cmd_gather_async") as cmd_gather_async:
            cmd_gather_async.return_value = (0, "fake_stdout", "fake_stderr")

            out, err = await exectools.cmd_assert_async(cmd, cwd=fake_cwd, text_mode=True)
            cmd_gather_async.assert_called_once_with(cmd, text_mode=True, cwd=fake_cwd, set_env=None, strip=False, log_stdout=False, log_stderr=True)
            self.assertEqual(out, "fake_stdout")
            self.assertEqual(err, "fake_stderr")

        with mock.patch("doozerlib.exectools.cmd_gather_async") as cmd_gather_async:
            cmd_gather_async.return_value = (1, "fake_stdout", "fake_stderr")
            with self.assertRaises(ChildProcessError):
                out, err = await exectools.cmd_assert_async(cmd, cwd=fake_cwd, text_mode=True)
            cmd_gather_async.assert_called_once_with(cmd, text_mode=True, cwd=fake_cwd, set_env=None, strip=False, log_stdout=False, log_stderr=True)
            self.assertEqual(out, "fake_stdout")
            self.assertEqual(err, "fake_stderr")

    def test_parallel_exec(self):
        items = [1, 2, 3]
        results = exectools.parallel_exec(
            lambda k, v: k,
            items, n_threads=4)
        results = results.get()
        self.assertEqual(results, items)


if __name__ == "__main__":

    unittest.main()
