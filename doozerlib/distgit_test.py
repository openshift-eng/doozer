#!/usr/bin/env
"""
Test the management of distgit repositories for RPM and image builds
"""
import unittest

import StringIO
import logging
import tempfile
import shutil
import re

import distgit

class MockDistgit(object):
    def __init__(self):
        self.branch = None
        
class MockConfig(object):

    def __init__(self):
        self.distgit = MockDistgit()
        
class MockRuntime(object):
    
    def __init__(self, logger):
        self.branch = None
        self.distgits_dir = "distgits_dir"
        self.logger = logger
        
class MockMetadata(object):

    def __init__(self, runtime):
        self.config = MockConfig()
        self.runtime = runtime
        self.logger = runtime.logger
        self.name = "test"
        self.namespace = "namespace"
        self.distgit_key = "distgit_key"
        
class MockScanner(object):

    def __init__(self):
        self.matches = []
        self.files = []


class TestDistgit(unittest.TestCase):
    """
    Test the methods and functions used to manage and update distgit repos
    """

    def setUp(self):
        """
        Define and provide mock logging for test/response
        """
        self.stream = StringIO.StringIO()
        logging.basicConfig(level=logging.DEBUG, stream=self.stream)
        self.logger = logging.getLogger()
        self.logs_dir = tempfile.mkdtemp()

    def tearDown(self):
        """
        Reset logging for each test.
        """
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.logs_dir)

    def test_init(self):
        """
        Ensure that init creates the object expected
        """
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.DistGitRepo(md, autoclone=False)

        self.assertIsInstance(d, distgit.DistGitRepo)

    def test_logging(self):
        """
        Ensure that logs work
        """
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.DistGitRepo(md, autoclone=False)

        msg = "Hey there!"
        d.logger.info(msg)

        actual = self.stream.getvalue()

        self.assertIn(msg, actual)

    def test_detect_permanent_build_failures(self):
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.ImageDistGitRepo(md, autoclone=False)

        d._logs_dir = lambda: self.logs_dir
        task_dir = tempfile.mkdtemp(dir=self.logs_dir)
        with open(task_dir + "/x86_64.log", "w") as file:
            msg1 = "No package fubar available"
            file.write(msg1)
        with open(task_dir + "/task_failed.log", "w") as file:
            msg2 = "Failed component comparison for components: fubar"
            file.write(msg2)

        scanner = MockScanner()
        scanner.matches = [
            # note: we read log lines, so these can only match in a single line
            re.compile("No package .* available"),
            re.compile("Failed component comparison for components:.*"),
        ]
        scanner.files = [ "x86_64.log", "task_failed.log" ]
        fails = d._detect_permanent_build_failures(scanner)
        self.assertIn(msg1, fails)
        self.assertIn(msg2, fails)

    def test_detect_permanent_build_failures_borkage(self):
        md = MockMetadata(MockRuntime(self.logger))
        d = distgit.ImageDistGitRepo(md, autoclone=False)

        scanner = MockScanner()
        scanner.matches = []
        scanner.files = [ "x86_64.log", "task_failed.log" ]

        self.assertIsNone(d._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("Log file scanning not specified", output)

        scanner.matches.append(None)

        d._logs_dir = lambda: self.logs_dir  # search an empty log dir
        self.assertIsNone(d._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("No task logs found under", output)

        d._logs_dir = lambda: str(tempfile.TemporaryFile())  # search a dir that won't be there
        self.assertIsNone(d._detect_permanent_build_failures(scanner))
        output = self.stream.getvalue()
        self.assertIn("Exception while trying to analyze build logs", output)

        
if __name__ == "__main__":
    unittest.main()
