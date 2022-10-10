import hashlib
import unittest
from doozerlib.dblib import DB, Record
from multiprocessing import RLock, Lock, Semaphore
import logging
import datetime
import pathlib
import sys
import mysql.connector


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


# Set up your local mariadb database before running this test
class DBLibWriteTest(unittest.TestCase):
    """
    Doozer had been creating new column if a new label was added in a Dockerfile of an Openshift component. But since
    the length of label names started to increase, doozer could not create a new column, as column names in SQL
    can't be longer than 64 characters. The issue was mainly with labels which start with "io.openshift". So now only
    the io.openshift labels in allow list are added as a column in the database. Other labels with length greater than
    64 is also not inserted and is logged.

    These tests will check if only the allowed labels values are inserted into the database.
    """
    def setUp(self) -> None:
        self.fake_runtime = FakeRuntime()
        self.db = DB(runtime=self.fake_runtime, environment="test")

        self.db.connection = mysql.connector.connect(host='localhost',
                                                     database='art_dash',
                                                     user='root',
                                                     password='secret')
        self.db.mysql_db_env_var_setup = True  # since we're setting the db connection above
        self.db.db = "art_dash"
        self.table_name = "log_build"

    def test_1(self):
        """
        Test to check if a label, that is in the allow list is correctly inserted
        """
        test_column = "label_io_openshift_that_should_be_there"   # add this to allow list before testing
        with self.db.record(operation="build", metadata=FakeMetaData()):
            Record.set(test_column, "test")

        cursor = self.db.connection.cursor()
        cursor.execute(f"show columns from {self.table_name}")
        result = [data[0] for data in cursor]

        assert test_column in result

        cursor.execute(f"alter table {self.table_name} drop column {test_column}")
        cursor.close()

    def test_2(self):
        """
        Test to check if a column that is not a io_openshift label is inserted properly
        """
        test_column = "normal_column"
        with self.db.record(operation="build", metadata=FakeMetaData()):
            Record.set(test_column, "test")

        cursor = self.db.connection.cursor()
        cursor.execute(f"show columns from {self.table_name}")
        result = [data[0] for data in cursor]

        assert test_column in result

        cursor.execute(f"alter table {self.table_name} drop column {test_column}")
        cursor.close()

    def test_3(self):
        """
        Test if an io_openshift label, that is not in the allow list is not inserted.
        """
        test_column = "label_io_openshift_that_should_not_be_there"
        with self.db.record(operation="build", metadata=FakeMetaData()):
            Record.set(test_column, "test")

        cursor = self.db.connection.cursor()
        cursor.execute(f"show columns from {self.table_name}")
        result = [data[0] for data in cursor]

        assert test_column not in result

    def tearDown(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    unittest.main()
