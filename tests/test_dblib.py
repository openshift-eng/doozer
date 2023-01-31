import unittest
from doozerlib.dblib import DB, Record
from multiprocessing import RLock, Lock, Semaphore
import logging
import datetime
import pathlib
import traceback
import sys


class FakeMetaData(object):

    def __init__(self):
        self.name = "test"
        self.namespace = "test_namespace"
        self.qualified_key = "test_qualified_key"
        self.qualified_name = "test_qualified_name"


class FakeRuntime(object):

    """This is a fake runtime class to inject into dblib running tests."""

    mutex = RLock()

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        # Create a "uuid" which will be used in FROM fields during updates
        self.uuid = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")

        self.user = ""

        self.group_config = dict()
        self.group_config["name"] = "test"

        # Cooperative threads can request exclusive access to directories.
        # This is usually only necessary if two threads want to make modifications
        # to the same global source alias. The empty string key serves as a lock for the
        # data structure.
        self.dir_locks = {'': Lock()}

        # See get_named_semaphore. The empty string key serves as a lock for the data structure.
        self.named_semaphores = {'': Lock()}

    def get_named_lock(self, absolute_path):
        with self.dir_locks['']:
            p = pathlib.Path(absolute_path).absolute()  # normalize (e.g. strip trailing /)
            if p in self.dir_locks:
                return self.dir_locks[p]
            else:
                new_lock = Lock()
                self.dir_locks[p] = new_lock
                return new_lock

    def get_named_semaphore(self, lock_name, is_dir=False, count=1):
        """
        Returns a semaphore (which can be used as a context manager). The first time a lock_name
        is received, a new semaphore will be established. Subsequent uses of that lock_name will
        receive the same semaphore.
        :param lock_name: A unique name for resource threads are contending over. If using a directory name
                            as a lock_name, provide an absolute path.
        :param is_dir: The lock_name is a directory (method will ignore things like trailing slashes)
        :param count: The number of times the lock can be claimed. Default=1, which is a full mutex.
        :return: A semaphore associated with the lock_name.
        """
        with self.named_semaphores['']:
            if is_dir:
                p = '_dir::' + str(pathlib.Path(str(lock_name)).absolute())  # normalize (e.g. strip trailing /)
            else:
                p = lock_name
            if p in self.named_semaphores:
                return self.named_semaphores[p]
            else:
                new_semaphore = Semaphore(count)
                self.named_semaphores[p] = new_semaphore
                return new_semaphore

    @staticmethod
    def timestamp():
        return datetime.datetime.utcnow().isoformat()


class DBLibTest(unittest.TestCase):

    def setUp(self):
        self.setup_failed = False
        try:
            self.fake_runtime = FakeRuntime()
            self.db = DB(runtime=self.fake_runtime, environment="test")
        except Exception:
            traceback.print_exc()
            self.setup_failed = True

    def test_select_withoutenv(self):
        if not self.setup_failed:
            self.assertRaises(RuntimeError, self.db.select, "select * from test", 10)

    def test_record(self):
        if not self.setup_failed:
            try:
                with self.db.record(operation="build", metadata=None):
                    Record.set("name", "test")
                    Record.set("position", "record")

                with self.db.record(operation="build", metadata=None):
                    Record.set("name", "test2")
                    Record.set("position", "record2")
                    Record.set("position2", "r_record2")

            except Exception:
                self.fail(msg="Failed to record.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def test_record_with_metadata(self):

        if not self.setup_failed:
            try:
                with self.db.record(operation="build", metadata=FakeMetaData()):
                    Record.set("name", "test")
                    Record.set("position", "record")
                    Record.set("country", "USA")
                    Record.set("population", 45435432523)
            except Exception:
                self.fail(msg="Failed to create record with extras.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def test_record_with_empty_value(self):
        if not self.setup_failed:
            try:
                with self.db.record(operation='build', metadata=None):
                    Record.set("name", "test")
                    Record.set("position", None)
                    Record.set("country", "")
                    Record.set("population", 0)
            except Exception:
                self.fail(msg="Failed to create record with missing attribute value.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def tearDown(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    unittest.main()
