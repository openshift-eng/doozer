from __future__ import absolute_import, print_function, unicode_literals
from future.utils import as_native_str
import time
import os
import threading
import traceback
from doozerlib import constants
import functools
from .model import Missing
import datetime

try:
    import mysql.connector as mysql_connector
except:
    # Allow this module to be missing
    pass


class DBLibException(Exception):

    """
    Exception class to record exceptions raised within the dblib module.
    """

    def __init__(self, message):
        super(self.__class__, self).__init__(message)
        self.message = message

    @as_native_str
    def __str__(self):
        return self.message


def try_connecting(func):
    """
    Decorator function, which when wrapped around tries to reconnect to mysql in case the connection
    doesn't exist or is broken. Attempts 5 tries to reconnect before giving up.
    Tries reconnecting in 25, 75, 150, 250, 375 seconds before giving up.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        db = args[0]

        if db.mysql_db_env_var_setup:

            if not db.connection or not db.connection.is_connected():
                count = 1

                while count <= 5:
                    try:
                        db.connection = mysql_connector.connect(
                            host=os.getenv(constants.DB_HOST, constants.default_db_params[constants.DB_HOST]),
                            user=os.getenv(constants.DB_USER),
                            password=os.getenv(constants.DB_PWD),
                            database=os.getenv(constants.DB_NAME, constants.default_db_params[constants.DB_NAME]))
                        break
                    except Exception as e:
                        traceback.print_exc()
                        db.runtime.logger.error("Unable to connect to mysql database.  Exception {}.".format(e))
                        time.sleep(25 * count)
                        count += 1

            func(*args, **kwargs)
        else:
            db.connection = None

    return wrapper


class DB(object):

    def __init__(self, runtime, environment, dry_run=False):
        """
        :param runtime: The runtime
        :param environment: int / stage / prod  for database environment. None will not write any data.
        """

        self.runtime = runtime
        self.environment = environment
        self.dry_run = dry_run

        # db configuration parameters
        self.host = None
        self.port = None
        self.db = None
        self.pwd = None
        self.db_user = None

        # mysql config for build records
        self.mysql_db_env_var_setup = True
        self.mysql_db_env_var_setup = self.check_missing_db_env_var() and self.check_database_exists()

        self.lock = threading.Lock()

        self.connection = None

        self._table_column_cache = {}

    def check_missing_db_env_var(self):

        """
        The function checks if the expected environment variables are set. If not, returns False.
        If all required env variables are present, the object instance's configuration properties are set
        and returns True.
        """

        if not (constants.DB_USER in os.environ and constants.DB_PWD in os.environ):
            self.runtime.logger.warning("Environment variables required for db operation missing. Doozer will be running"
                                        "in no DB use mode.")
            return False

        import mysql.connector as mysql_connector

        # if required configuration parameters are found, set instance attributes to configuration values
        self.host = os.getenv(constants.DB_HOST, constants.default_db_params[constants.DB_HOST])
        self.port = os.getenv(constants.DB_PORT, constants.default_db_params[constants.DB_PORT])
        self.db = os.getenv(constants.DB_NAME, constants.default_db_params[constants.DB_NAME])
        self.pwd = os.getenv(constants.DB_PWD)
        self.db_user = os.getenv(constants.DB_USER)

        self.runtime.logger.info("Found all environment variables required for db setup.")
        return True

    def select(self, expr, limit=100):

        """
        :param expr [string] the SQL command want to query from DB
        :param limit [number] limit the length of the return value
        :return [dict] return a list of results from SQL query output, return [] if get nothing or get error

        This funtion pass the native SQL query command to DB, return the query result in dict format.
        If query failed or get nothing then return empty dict.
        By default limit the result length to 100.

        """

        exeresult = []
        if self.mysql_db_env_var_setup:
            db_connection = mysql_connector.connect(host=self.host,
                                                    user=self.db_user,
                                                    password=self.pwd,
                                                    database=self.db)
            cursor = db_connection.cursor()
            try:
                cursor.execute("{} LIMIT {}".format(expr, limit))
                exeresult = cursor.fetchall()
                self.runtime.logger.info(exeresult)
            except Exception as e:
                self.runtime.logger.error("Error executing command in database. Exception is [{}].".format(e))
            finally:
                cursor.close()
        return exeresult

    def check_database_exists(self):

        """
        This method checks if the configured database is present.
        If present returns True.

        If not, it tries to create the database. On successful creation, returns True
        otherwise returns false.

        When returns false, Doozer will run in no db mode.
        """

        db_check_connection = mysql_connector.connect(host=self.host,
                                                      user=self.db_user,
                                                      password=self.pwd)

        cursor = db_check_connection.cursor()
        cursor.execute("select count(*) from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME='{}'"
                       .format(self.db))

        exception_raised = False

        if cursor.fetchone()[0] != 0:
            self.runtime.logger.info("Configured database [{}] present on MySQL host.".
                                     format(self.db))
            return True
        else:
            create_db_cursor = db_check_connection.cursor()
            try:
                create_db_cursor.execute("create database {}".format(self.db))
                self.runtime.logger.info("Successfully created configured database [{}].".format(self.db))
            except Exception as e:
                self.runtime.logger.error("Error creating database. Exception is [{}].".format(e))
                exception_raised = True
            finally:
                create_db_cursor.close()

        return not exception_raised

    def check_table_exist(self, table_name):

        """
        :param table_name
        This method returns True, if the table to which the record is attempted to be created in is true. Otherwise
        returns false.
        """

        if table_name in self._table_column_cache:
            return True

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema ='{0}' and table_name = '{1}'
            """.format(self.db, table_name.replace('\'', '\'\'')))
        if cursor.fetchone()[0] != 0:
            cursor.close()
            self._table_column_cache[table_name] = {}
            self.runtime.logger.info("Table {} found in database {}.".format(table_name, self.db))
            return True

        cursor.close()
        return False

    @staticmethod
    def identify_column_type(value):

        """
        :param value
        This is a helper method, which returns the MYSQL type to the provided value with corresponding python
        type.
        """

        if isinstance(value, int):
            return "BIGINT", value
        elif isinstance(value, float):
            return "FLOAT", value
        elif isinstance(value, datetime.datetime):
            return "DATETIME", value
        else:
            return "VARCHAR(1000)", value

    def get_missing_columns(self, attr_payload, table_name):

        """
        This method fetches all the column in the given table, and compares it with
        the payload to check if there are any additional keys in the payload.
        If there are new columns, it returns just the new non-existing columns.
        """

        for attr_column in attr_payload:
            if not (attr_column in self._table_column_cache[table_name]):
                break
        else:
            # if the loop runs without breaking all the columns in payload are already present in
            # the table hence no new columns are required to be added
            self.runtime.logger.info("Found all the columns in cache for table [{}].".format(table_name))
            return []

        self.runtime.logger.info("Received new columns in the payload or table [{}] column cache empty."
                                 .format(table_name))

        payload_columns = set()
        existing_columns = set()
        return_columns = []

        for column in attr_payload:
            payload_columns.add(column)

        if not self._table_column_cache[table_name]:

            # if the table's column cache is not updates, load the cache and return
            # the table columns

            self.runtime.logger.info("Updating table [{}] column cache.".format(table_name))

            cursor = self.connection.cursor()
            cursor.execute(f"show columns from {table_name}")

            for data in cursor:
                existing_columns.add(data[0])
                self._table_column_cache[table_name][data[0]] = True
        else:

            # otherwise if table column cache found, return cache value

            self.runtime.logger.info("Found table [{}] column cache.".format(table_name))
            existing_columns = set(self._table_column_cache[table_name])

        # get the difference column, non-existing table columns which are present in payload
        missing_columns = list(payload_columns.difference(existing_columns))

        for index, column in enumerate(missing_columns):
            self.runtime.logger.info("Missing column {}. {}".format(index + 1, column))
            return_columns.append((column, self.identify_column_type(attr_payload[column])))
        return return_columns

    def handle_missing_table(self, table_name):

        """
        :param table_name
        This method creates table, if it doesn't exist.
        """

        table_exist_status = self.check_table_exist(table_name)

        if not table_exist_status:
            self.runtime.logger.info("Table [{}] not found in database [{}]."
                                     .format(table_name, self.db))
            cursor = self.connection.cursor()
            cursor.execute(f'create table {table_name}(log_{table_name}_id bigint auto_increment primary key)')
            cursor.close()
            self._table_column_cache[table_name] = dict()
            self._table_column_cache[table_name]["log_" + table_name + "_id"] = True
            self.runtime.logger.info("Successfully create table [{}] in database [{}]."
                                     .format(table_name, self.db))

    def handle_missing_columns(self, payload, table_name):

        """
        :param payload
        :param table_name

        This method creates missing columns, which are present in payload but not in table.
        """

        missing_columns = self.get_missing_columns(payload, table_name)

        if missing_columns:
            cursor = self.connection.cursor()

            for column in missing_columns:
                missing_column = column[0]
                column_type = column[1][0]
                cursor.execute(f"alter table {table_name} add column `{missing_column}` {column_type}")
                self._table_column_cache[table_name][missing_column] = True

                self.runtime.logger.info("Added new column [{}] of identified type [{}] to table [{}] "
                                         "of database [{}].".format(missing_column, column_type,
                                                                    table_name, self.db))

            cursor.close()

    def insert_payload(self, payload, table_name, dry_run=False):

        """
        :param payload
        :param table_name
        :param dry_run

        This  method creates table entry for the given payload. If dry_run is true, the insert string is
        just printed on output.

        The method returns 0, on successful creation of record entry in the table else returns 1.
        """

        try:
            columns, values = [], []

            for column in payload:
                columns.append(f'`{column}`')
                values.append(f'"{payload[column]}"')

            columns = ",".join(columns)
            values = ",".join(values)

            insert_string = f"insert into {table_name}({columns}) values({values})"

            if dry_run:
                print(insert_string)
                return 0

            cursor = self.connection.cursor()
            cursor.execute(insert_string)
            self.connection.commit()
            cursor.close()
            return 0

        except Exception as e:
            self.runtime.logger.error("Something went wrong creating payload entry in the database. Payload is {}. Exception is {}.".format(payload, e), exc_info=True)
            return 1

    @try_connecting
    def create_payload_entry(self, payload, table_name, record_dry_run=False):

        """
        :param payload
        :param table_name
        :param record_dry_run

        This method is a wrapper method, which calls the method to check missing table, missing column and then
        the method to insert record into table.
        """

        # this if condition only allows request to be written to db where dry_run is set
        # false both at DB initiation in runtime and creating Record in the context
        # manager. Any of the dry_run flags if set to true will not allow
        # writing record to the database.

        insert_status = None

        if self.mysql_db_env_var_setup and not self.dry_run and not record_dry_run:
            if self.connection and self.connection.is_connected():
                self.handle_missing_table(table_name)
                self.handle_missing_columns(payload, table_name)
                insert_status = self.insert_payload(payload, table_name)
            else:
                if not self.mysql_db_env_var_setup:
                    self.runtime.logger.error("Doozer running with no DB use mode. Can't create build entry.")
                insert_status = 1
        else:
            self.runtime.logger.info("Doozer either running with no DB mode or this was a dry run request.")
            insert_status = self.insert_payload(payload, table_name, True)

        if insert_status:
            self.runtime.logger.error("Something went wrong creating payload entry.")
        else:
            self.runtime.logger.info("Payload entry successfully created in database.")

    @staticmethod
    def rename_to_valid_column(column_name):

        """
        This is a helper method to transform column name having '.' and '-' and replaces these characters with '_'.
        """

        column_name = column_name.replace(".", "_")
        column_name = column_name.replace("-", "_")
        return column_name

    def record(self, operation, metadata=None, dry_run=False):
        """
        :param operation: The type of operation being tracked, table name to which the record is sent to
        :param metadata: Distgit metadata if any
        :param dry_run: If true, this record will not be written to the datastore. e.g. scratch builds.
        :return:
        """

        extras = {}
        if metadata:
            extras['dg.name'] = metadata.name
            extras['dg.namespace'] = metadata.namespace
            extras['dg.qualified_key'] = metadata.qualified_key
            extras['dg.qualified_name'] = metadata.qualified_name

        return Record(self, "log_" + str(operation), extras, dry_run)


class Record(object):
    """
    Context manager to handle records being added to database
    Useful reference: https://preshing.com/20110920/the-python-with-statement-by-example/
    """

    _tl = threading.local()

    def __init__(self, db, table, extras={}, dry_run=False):
        self.previous_record = None
        self.table = table
        self.db = db
        self.runtime = db.runtime
        self.dry_run = dry_run

        # A set of attributes for the record
        self.attrs = {
            'time.unix': int(round(time.time() * 1000)),  # utc milliseconds since epoch
            'time.iso': self.runtime.timestamp(),  # for humans
            'runtime.uuid': self.runtime.uuid,
            'runtime.user': self.runtime.user or '',
            'group': self.runtime.group_config['name'],
        }

        for jenkins_var in ['BUILD_NUMBER', 'BUILD_URL', 'JOB_NAME', 'NODE_NAME', 'JOB_URL']:
            self.attrs[f'jenkins.{jenkins_var.lower()}'] = os.getenv(jenkins_var, '')

        self.attrs.update(extras)

    def __enter__(self):
        if hasattr(self._tl, 'record'):
            self.previous_record = self._tl.record
        self._tl.record = self
        return self

    def __exit__(self, *args):
        with self.db.runtime.get_named_semaphore('dblib::mysql'):

            attr_payload = {}

            for k, v in self.attrs.items():

                if v is None or v is Missing or v == '':
                    continue
                else:
                    attr_payload[self.db.rename_to_valid_column(k)] = v
            self.db.create_payload_entry(attr_payload, self.table, self.dry_run)

        self._tl.record = self.previous_record

    @classmethod
    def set(cls, name, value):
        """
        Sets an attribute name=value for the most recent Record context
        :param name: The name of the attribute to set (limit 1024 chars)
        :param value: The value of the attribute to set (limit 1024 chars)
        :return:
        """
        if not hasattr(cls._tl, "record"):
            # Caller is not within runtime.record(...):
            raise IOError(f'No database record context has been established. Unable to set {name}={value}.')
        cls._tl.record.attrs[name] = value

    @classmethod
    def update(cls, a_dict):
        """
        Sets attributes for the most recent Record context
        :param a_dict: key / values to set
        :return:
        """
        if not hasattr(cls._tl, "record"):
            # Caller is not within runtime.record(...):
            raise IOError(f'No database record context has been established. Unable to set {a_dict}.')
        cls._tl.record.attrs.update(a_dict)

    @classmethod
    def current(cls):
        """
        :return: Returns the current Record object or None
        """
        if not hasattr(cls._tl, "record"):
            return None
        return cls._tl.record
