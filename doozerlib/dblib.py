from __future__ import absolute_import, print_function, unicode_literals
import time
import os
import sys
import threading
import hashlib
import boto3
import traceback
from .model import Missing

# Records need unique names. Use this int atomically for extra protection.
record_sequence = 0


class DB(object):

    def __init__(self, runtime, environment, dry_run=False):
        """
        :param runtime: The runtime
        :param environment: int / stage / prod  for database environment. None will not write any data.
        :param dry_run: If True, not data will be written
        """
        self.lock = threading.Lock()
        self.existing_domains = {}  # maps existing domain names to True
        self.runtime = runtime
        self.environment = environment
        self.cmd_line = ','.join(sys.argv)
        m = hashlib.md5()
        m.update(self.cmd_line.encode('utf-8'))
        self.uniq = m.hexdigest()  # help guarantee record uniqueness

        self.client = None
        if environment and not dry_run:
            try:
                self.client = boto3.client('sdb', region_name='us-east-1')
                for name in self.client.list_domains()['DomainNames']:
                    self.existing_domains[name] = True
            except:
                traceback.print_exc()
                runtime.logger.error('Unable to acquire simpledb client; please check AWS creds')

    def create_domain_if_missing(self, domain):
        if self.client:
            with self.lock:
                if domain in self.existing_domains:
                    # Already exists
                    return
                self.client.create_domain(
                    DomainName=domain,
                )
                self.existing_domains[domain] = True

    def record(self, operation, metadata=None, dry_run=False):
        """
        :param operation: The type of operation being tracked
        :param metadata: Distgit metadata if any
        :param dry_run: If true, this record will not be written to the datastore. e.g. scratch builds.
        :return:
        """
        domain = f'ART_{self.environment}_{operation}'
        self.create_domain_if_missing(domain)

        extras = {}
        if metadata:
            extras['dg.name'] = metadata.name
            extras['dg.namespace'] = metadata.namespace
            extras['dg.qualified_key'] = metadata.qualified_key
            extras['dg.qualified_name'] = metadata.qualified_name

        return Record(self, domain, extras, dry_run)

    def select(self, expression, consistent_read=True, limit=0):
        """
        https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/UsingSelect.html
        :param expression: Select expression to execute
        :param consistent_read: If True, ensures consistency before read. Minor performance penalty.
        :param limit: If specified, return a maximum of [limit] records.
        :return: An array of item records. e.g.
                [
                    { 'Name': 'test.202', 'Attributes': [{'Name': 'jenkins.build_url', 'Value': '..''}, ...] },
                    { 'Name': 'test.203', 'Attributes': [{'Name': 'jenkins.build_url', 'Value': '..''}, ...] },
                    ...
                ]

        """
        items = []

        """
        If a select expression specifies a simple 'limit X', the initial return of self.client.select will contain
        <= limit element. However, it will also return a NextToken which will encourage the loop to continue.
        Thus we would return all selected records, [limit] at a time, with multiple calls to the client.
        Thus, we need to break the query loop with limited is True.
        """
        limited = limit > 0
        if limited:
            expression += ' limit {}'.format(limit)

        self.runtime.logger.info(f'Running db query: {expression}')
        if self.client:
            nt = ''
            while True:
                r = self.client.select(
                    SelectExpression=expression,
                    NextToken=nt,
                    ConsistentRead=consistent_read,
                )
                nt = r.get('NextToken', None)
                items.extend(r.get('Items', []))
                if not nt:
                    break

                if limited and len(items) >= limit:
                    items = items[:limit]
                    break

            return items
        else:
            raise IOError('No simpledb client has been initialized')


class Record(object):
    """
    Context manager to handle records being added to datastore
    """

    _tl = threading.local()

    def __init__(self, db, domain, extras={}, dry_run=False):
        global record_sequence
        self.previous_record = None
        self.domain = domain
        self.db = db
        self.runtime = db.runtime
        self.dry_run = dry_run

        with self.runtime.mutex:
            self.seq = record_sequence
            record_sequence += 1

        self.name = f'{self.runtime.uuid}.{self.seq}.{self.db.uniq}'

        # A set of attributes for the record
        self.attrs = {
            'time.unix': str(int(round(time.time() * 1000))),  # utc milliseconds since epoch
            'time.iso': self.runtime.timestamp(),  # for humans
            'record.seq': self.seq,
            'runtime.uuid': str(self.runtime.uuid),
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
        if not self.dry_run and self.db.client:
            with self.db.runtime.get_named_lock('package::boto3'):
                try:
                    attr_payload = []
                    for k, v in self.attrs.items():

                        if v is None or v is Missing:
                            v = ''

                        attr_payload.append({
                            'Name': k,
                            'Value': str(v),
                            'Replace': True
                        })
                    self.db.client.put_attributes(
                        DomainName=self.domain,
                        ItemName=self.name,
                        Attributes=attr_payload
                    )
                except:
                    traceback.print_exc()
                    self.runtime.logger.error(f'Unable to write to simpledb: {self.name}={self.attrs}')

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
