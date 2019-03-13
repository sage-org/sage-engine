# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgre.queries import get_start_query, get_resume_query, get_insert_query
from sage.database.estimators import pattern_shape_estimate
import psycopg2
import json


class PostgreIterator(DBIterator):
    """An PostgreIterator implements a DBIterator for a triple pattern evaluated using a Postgre database file"""

    def __init__(self, cursor, connection, start_query, start_params, table_name, pattern):
        super(PostgreIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        # resume query execution with a SQL query
        self._cursor.execute(self._current_query, start_params)
        # self._connection.commit()
        # always keep the current row buffered inside the iterator
        self._last_read = self._cursor.fetchone()

    def __advance_iteration(self, last_read):
        query, params = get_resume_query(self._pattern["subject"], self._pattern["predicate"], self._pattern["object"], last_read, self._table_name, symbol=">")
        if query is not None and params is not None:
            self._current_query = query
            # self._connection.commit()
            self._cursor.execute(self._current_query, params)

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
        self._last_read = self._cursor.fetchone()
        # truy to fetch the next page
        if self._last_read is None:
            self.__advance_iteration(triple)
            self._last_read = self._cursor.fetchone()
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

    def __init__(self, table_name, dbname, user, password, host='', port=5432, fetch_size=100):
        super(PostgreConnector, self).__init__()
        self._table_name = table_name
        self._dbname = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._fetch_size = fetch_size
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
        cursor = self._connection.cursor()
        # create a SQL query to start a new index scan
        if last_read is None:
            start_query, start_params = get_start_query(subject, predicate, obj, self._table_name, fetch_size=self._fetch_size)
        else:
            # empty last_read key => the scan has already been completed
            if len(last_read) == 0:
                return EmptyIterator(pattern), 0
            # otherwise, create a SQL query to resume the index scan
            last_read = json.loads(last_read)
            t = (last_read["s"], last_read["p"], last_read["o"])
            start_query, start_params = get_resume_query(subject, predicate, obj, t, self._table_name, fetch_size=self._fetch_size)
        # create the iterator to yield the matching RDF triples
        iterator = PostgreIterator(cursor, self._connection, start_query, start_params, self._table_name, pattern)
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

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
            If not overrided, this method raises an exception as it consider the graph as read-only.
        """
        if self._connection is None:
            self.open()
        if subject is not None and predicate is not None and obj is not None:
            cursor = self._connection.cursor()
            insert_query = get_insert_query(self._table_name)
            cursor.execute(insert_query, (subject, predicate, obj))
            self._connection.commit()
            cursor.close()

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple into the RDF Graph.
            If not overrided, this method raises an exception as it consider the graph as read-only.
        """
        pass
