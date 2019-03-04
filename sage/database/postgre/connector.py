# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgre.cursors import create_start_cursor, create_resume_cursor
from sage.database.estimators import pattern_shape_estimate
import psycopg2
import json


class PostgreIterator(DBIterator):
    """An PostgreIterator implements a DBIterator for a triple pattern evaluated using a Postgre database file"""

    def __init__(self, cursor_name, connection, root_cursor, pattern):
        super(PostgreIterator, self).__init__(pattern)
        self._cursor_name = cursor_name
        self._connection = connection
        self._query_cursor = self._connection.cursor(self._cursor_name)
        self._root_cursor = root_cursor
        # always keep the current row buffered inside the iterator
        self._last_read = self._query_cursor.fetchone()

    def __del__(self):
        """Destructor, which commit all reads and then close cursors"""
        # TODO activating the following line yield "psycopg2.ProgrammingError: named cursor isn't valid anymore"
        # my guess: the cursor is not re-open, so the driver uses the previous cursor which belongs to a committed transaction
        # self._connection.commit()
        self._query_cursor.close()
        self._root_cursor.close()

    def last_read(self):
        """Return the index ID of the last element read"""
        triple = self._last_read
        if triple is None:
            return ''
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2]
        }, separators=(',', ':'))

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        triple = self._last_read
        if triple is None:
            return None
        self._last_read = self._query_cursor.fetchone()
        return triple

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        return self._last_read is not None


class PostgreConnector(DatabaseConnector):
    """
        A PostgreConnector search for RDF triples in a PostgreSQL database.

        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - dbname `str`: the database name
            - user `str`: user name used to authenticate
            - password `str`: password used to authenticate
            - host `str`: database host address (defaults to UNIX socket if not provided)
            - port `int`: connection port number (defaults to 5432 if not provided)
    """

    def __init__(self, table_name, dbname, user, password, host='', port=5432):
        super(PostgreConnector, self).__init__()
        self._table_name = table_name
        self._dbname = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._connection = None

    def open(self):
        """Open the database connection"""
        if self._connection is None:
            self._connection = psycopg2.connect(dbname=self._dbname, user=self._user, password=self._password, host=self._host, port=self._port)

    def close(self):
        """Close the database connection"""
        if self._connection is not None:
            self._connection.commit()
            self._connection.close()
            self._connection = None

    def search(self, subject, predicate, obj, last_read=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan

            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        if self._connection is None:
            self.open()
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        # cursor used to create the query cursor
        parent_cursor = self._connection.cursor()
        # open a cursor to start a new index scan
        if last_read is None:
            cursor_name = create_start_cursor(parent_cursor, self._table_name, subject, predicate, obj)
        else:
            # empty last_read key => the scan has already been completed
            if len(last_read) == 0:
                return EmptyIterator(pattern), 0
            # otherwise, open a cursor to resume the index scan
            p = (subject, predicate, obj)
            last_read = json.loads(last_read)
            cursor_name = create_resume_cursor(parent_cursor, self._table_name, p, last_read['s'], last_read['p'], last_read['o'])
        # create the iterator to yield the matching RDF triples
        iterator = PostgreIterator(cursor_name, self._connection, parent_cursor, pattern)
        card = pattern_shape_estimate(subject, predicate, object) if iterator.has_next() else 0
        return iterator, card

    def from_config(config):
        """Build a DatabaseConnector from a dictionnary"""
        if 'dbname' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')
        table_name = config['name']
        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        return PostgreConnector(table_name, config['dbname'], config['user'], config['password'], host=host, port=port)
