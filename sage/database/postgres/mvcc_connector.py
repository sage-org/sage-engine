# mvcc_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
import json
from datetime import datetime
from uuid import uuid4

from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgres.connector import PostgresConnector
from sage.database.postgres.mvcc_queries import (get_delete_query,
                                                 get_insert_query,
                                                 get_resume_query,
                                                 get_start_query)


def parse_date(str_date):
    """Convert a PostgreSQL date into a Python datetime object"""
    if str_date == 'infinity':
        return datetime.max
    return datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S+%f')


class MVCCPostgresIterator(DBIterator):
    """
        An MVCCPostgresIterator implements a DBIterator for a triple pattern evaluated using a MVCC-enabled PostgreSQL database.

        Constructor args:
            - cursor `psycopg.cursor` - Psycopg cursor used to query the database
            - start_time `datetime` - Timestamp at which the iterator should read
            - start_query `str` - Prepared SQL query used to start iteration
            - start_params `tuple` - SQL params to apply to the `start_query`
            - table_name `str` - Name of the SQL table to scan
            - pattern `dict` - Triple pattern evaluated
            - fetch_size `int`: how many RDF triples are fetched per SQL query (default to 500)
    """

    def __init__(self, cursor, start_time, start_query, start_params, table_name, pattern, fetch_size=2000):
        super(MVCCPostgresIterator, self).__init__(pattern)
        self._cursor = cursor
        self._start_time = start_time
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        # execute the starting SQL query to pre-fetch results
        self._cursor.execute(self._current_query, start_params)
        # always keep the current set of rows buffered inside the iterator
        self._last_reads = self._cursor.fetchmany(size=self._fetch_size)

    def __del__(self):
        """Destructor"""
        self._cursor.close()

    def last_read(self):
        """Return the index ID of the last element read"""
        if not self.has_next():
            return ''
        triple = self._last_reads[0]
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2],
            'ins': triple[3].isoformat(),
            'del': triple[4].isoformat(),
            'ts': self._start_time.isoformat()
        }, separators=(',', ':'))

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        if not self.has_next():
            return None
        triple = self._last_reads.pop(0)

        # extract timestamps from the RDF triple
        insert_t = triple[3]
        delete_t = triple[4]

        # case 1: the current triple is in the valid version, so it is a match
        if insert_t <= self._start_time and self._start_time < delete_t:
            return (triple[0], triple[1], triple[2])
        # case 2: do a NONE forward to trigger another iteration loop
        # to find a matching RDF triple
        return None

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0


class MVCCPostgresConnector(PostgresConnector):
    """
        A MVCCPostgresConnector search for RDF triples in a PostgreSQL database using a timestamp-based multi-version concurrency control protocol.

        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - dbname `str`: the database name
            - user `str`: user name used to authenticate
            - password `str`: password used to authenticate
            - host `str`: database host address (default to UNIX socket if not provided)
            - port `int`: connection port number (default to 5432 if not provided)
            - fetch_size `int`: how many RDF triples are fetched per SQL query (default to 2000)
    """

    def __init__(self, table_name, dbname, user, password, host='', port=5432, fetch_size=2000):
        super(MVCCPostgresConnector, self).__init__(table_name, dbname, user, password, host, port, fetch_size)

    def search(self, subject, predicate, obj, last_read=None, as_of=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan
                - as_of ``datetime=None`` ``optional`` - Perform all reads against a consistent snapshot represented by a timestamp.

            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        # do warmup if necessary
        self.open()

        # format triple patterns for the PostgreSQL API
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        # pick a start transaction timestamp
        # NB: It will be overwritten if we reload a scan from a saved state
        timestamp = datetime.now() if as_of is None else as_of

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator
        # otherwise, we might reset a cursor whose results were not fully consumed
        if not self._manager.is_open():
            self._manager.open_connection()
        cursor = self._manager.get_connection().cursor(str(uuid4()))

        # create a SQL query to start a new index scan
        if last_read is None:
            start_query, start_params = get_start_query(subject, predicate, obj, self._table_name)
        else:
            # empty last_read key => the scan has already been completed
            if len(last_read) == 0:
                return EmptyIterator(pattern), 0

            # decode the saved state to get the timestamp & the last RDF triple read
            last_read = json.loads(last_read)
            # parse ISO timestamps into datetime objects
            timestamp = datetime.fromisoformat(last_read["ts"])
            last_ins_t = datetime.fromisoformat(last_read["ins"])
            last_del_t = datetime.fromisoformat(last_read["del"])

            last_triple = (last_read["s"], last_read["p"], last_read["o"], last_ins_t, last_del_t)

            # create a SQL query to resume the index scan
            start_query, start_params = get_resume_query(subject, predicate, obj, last_triple, self._table_name)

        # create the iterator to yield the matching RDF triples
        iterator = MVCCPostgresIterator(cursor, timestamp, start_query, start_params, self._table_name, pattern, fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config):
        """Build a MVCCPostgresConnector from a configuration object"""
        if 'dbname' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')

        table_name = config['name']
        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 2000

        return MVCCPostgresConnector(table_name, config['dbname'], config['user'], config['password'], host=host, port=port, fetch_size=fetch_size)

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
        """
        # do warmup if necessary
        self.open()
        # start transaction
        self.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            insert_query = get_insert_query(self._table_name)
            self._update_cursor.execute(insert_query, (subject, predicate, obj))

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple from the RDF Graph.
        """
        # do warmup if necessary
        self.open()
        # start transaction
        self.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            delete_query = get_delete_query(self._table_name)
            self._update_cursor.execute(delete_query, (subject, predicate, obj))
