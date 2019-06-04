import unittest

import StringIO
import logging
import tempfile
import shutil

from model import Model


class MockDistgit(object):
    def __init__(self):
        self.branch = None


class MockContent(object):
    def __init__(self):
        self.branch = None


class MockConfig(object):

    def __init__(self):
        self.distgit = MockDistgit()
        self.content = Model()
        self.content.source = Model()
        self.content.source.specfile = "test-dummy.spec"


class SimpleMockLock(object):

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockRuntime(object):

    def __init__(self, logger):
        self.branch = None
        self.distgits_dir = "distgits_dir"
        self.logger = logger
        self.mutex = SimpleMockLock()
        self.missing_pkgs = set()

    def detect_remote_source_branch(self, _):
        pass


class MockMetadata(object):

    def __init__(self, runtime):
        self.config = MockConfig()
        self.runtime = runtime
        self.logger = runtime.logger
        self.name = "test"
        self.namespace = "namespace"
        self.distgit_key = "distgit_key"

    def fetch_cgit_file(self, file):
        pass

    def get_component_name(self):
        pass


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
        self.md = MockMetadata(MockRuntime(self.logger))

    def tearDown(self):
        """
        Reset logging for each test.
        """
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.logs_dir)
