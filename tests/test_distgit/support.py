from unittest import IsolatedAsyncioTestCase
from future import standard_library

from doozerlib.assembly import AssemblyTypes

standard_library.install_aliases()

try:
    from importlib import reload
except ImportError:
    pass
import io
import logging
import tempfile
import shutil

from doozerlib import model


class MockDistgit(object):
    def __init__(self):
        self.branch = None


class MockContent(object):
    def __init__(self):
        self.branch = None


class MockConfig(dict):

    def __init__(self, *args, **kwargs):
        super(MockConfig, self).__init__(*args, **kwargs)
        self.distgit = MockDistgit()
        self.content = model.Model()
        self.content.source = model.Model()
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
        self.cache_dir = None
        self.assembly_type = AssemblyTypes.STANDARD

    def detect_remote_source_branch(self, _):
        pass


class MockMetadata(object):

    def __init__(self, runtime):
        self.config = MockConfig()
        self.runtime = runtime
        self.logger = runtime.logger
        self.name = None
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


class TestDistgit(IsolatedAsyncioTestCase):
    """
    Test the methods and functions used to manage and update distgit repos
    """

    def setUp(self):
        """
        Define and provide mock logging for test/response
        """
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)
        self.logs_dir = tempfile.mkdtemp()
        self.md = MockMetadata(MockRuntime(self.logger))

    def tearDown(self):
        """
        Reset logging for each test.
        """
        logging.shutdown()
        reload(logging)
        shutil.rmtree(self.logs_dir)
