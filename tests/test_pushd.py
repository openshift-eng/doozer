"""
Test the Dir() class.  Verify that it works as a context_manager.
"""

import unittest

import os
from multiprocessing.dummy import Pool

from doozerlib import pushd


class DirTestCase(unittest.TestCase):
    """
    Test the features of the pushd.Dir() class.  This is a context manager.
    It is meant to be used to allow a process to work in different working
    directories and return cleanly to the original working directory.

    It also locks so that the directory can only be visited by one thread at a
    time.
    """

    def test_getcwd(self, concurrent=False):
        """
        Verify that the directory locking for concurrency is working
        """
        cwd = pushd.Dir.getcwd()
        with pushd.Dir("/"):
            self.assertEqual(pushd.Dir.getcwd(), "/")
            with pushd.Dir("/dev"):
                self.assertEqual(pushd.Dir.getcwd(), "/dev")
            self.assertEqual(pushd.Dir.getcwd(), "/")
        self.assertEqual(pushd.Dir.getcwd(), cwd)

    def test_getcwd_threads(self):
        """
        Execute the concurrency test for 10 threads
        """
        thread_count = 10
        with Pool(thread_count) as pool:
            results = [
                pool.apply_async(lambda: self.test_getcwd(concurrent=True))
                for _ in range(thread_count)
            ]
            for result in results:
                result.get()


if __name__ == "__main__":
    unittest.main()
