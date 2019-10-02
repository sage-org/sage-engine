# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgres.queries import get_start_query, get_resume_query, get_insert_query, get_insert_many_query, get_delete_query
import psycopg2
from psycopg2.extras import execute_values
import json
from math import ceil


def fetch_histograms(cursor, table_name, attribute_name):
    """
        Download PostgreSQL histograms from a given table and attribute
    """
    base_query = "SELECT null_frac, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = '{}' AND attname = '{}'".format(table_name, attribute_name)
    cursor.execute(base_query)
    record = cursor.fetchone()
    null_frac, n_distinct, most_common_vals, most_common_freqs = record
    # build selectivity table
    selectivities = {}
    cpt = 0
    for common_val in most_common_vals[1:-1].split(","):
        if cpt < len(most_common_freqs):
            selectivities[common_val] = most_common_freqs[cpt]
        cpt += 1
    return (null_frac, n_distinct, selectivities, sum(most_common_freqs))


class PostgresIterator(DBIterator):
    """An PostgresIterator implements a DBIterator for a triple pattern evaluated using a Postgre database file"""

    def __init__(self, cursor, connection, start_query, start_params, table_name, pattern):
        super(PostgresIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        # resume query execution with a SQL query
        self._cursor.execute(self._current_query, start_params)
        # always keep the current row buffered inside the iterator
        self._last_read = self._cursor.fetchone()

    def __del__(self):
        """Destructor"""
        self._cursor.close()

    def __advance_iteration(self, last_read):
        """Advance iteration by fetching the next page of results using a SQL query"""
        query, params = get_resume_query(self._pattern["subject"], self._pattern["predicate"], self._pattern["object"], last_read, self._table_name, symbol=">")
        if query is not None and params is not None:
            self._current_query = query
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


class PostgresConnector(DatabaseConnector):
    """
        A PostgresConnector search for RDF triples in a PostgreSQL database.

        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - dbname `str`: the database name
            - user `str`: user name used to authenticate
            - password `str`: password used to authenticate
            - host `str`: database host address (defaults to UNIX socket if not provided)
            - port `int`: connection port number (defaults to 5432 if not provided)
    """

    def __init__(self, table_name, dbname, user, password, host='', port=5432, fetch_size=500):
        super(PostgresConnector, self).__init__()
        self._table_name = table_name
        self._dbname = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._fetch_size = fetch_size
        self._connection = None
        self._update_cursor = None
        self._warmup = True

        # Data used for cardinality estimation
        # They are initialized using PostgreSQL histograms, after the 1st connection to the DB
        self._avg_row_count = 0
        self._subject_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }
        self._predicate_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }
        self._object_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }

    def open(self):
        """Open the database connection"""
        if self._connection is None:
            self._connection = psycopg2.connect(dbname=self._dbname, user=self._user, password=self._password, host=self._host, port=self._port)
            # disable autocommit
            self._connection.autocommit = False

        # Do warmup phase if required, i.e., gather stats for query execution
        if self._warmup:
            cursor = self._connection.cursor()
            # fetch estimated table cardinality
            cursor.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '{}'".format(self._table_name))
            self._avg_row_count = cursor.fetchone()[0]
            # fetch subject histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'subject')
            self._subject_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch predicate histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'predicate')
            self._predicate_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch object histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'object')
            self._object_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # commit & close cursor
            self._connection.commit()
            cursor.close()
            self._warmup = False

    def close(self):
        """Close the database connection"""
        if self._update_cursor is not None:
            self._update_cursor.close()
            self._update_cursor = None
        if self._connection is not None:
            self._connection.commit()
            self._connection.close()
            self._connection = None

    def start_transaction(self):
        """Start a PostgreSQL transaction"""
        if self._connection is None:
            self.open()
        if self._update_cursor is None:
            self._update_cursor = self._connection.cursor()

    def commit_transaction(self):
        """Commit any ongoing transaction"""
        if self._connection is not None:
            self._connection.commit()
        if self._update_cursor is not None:
            self._update_cursor.close()
            self._update_cursor = None

    def abort_transaction(self):
        """Abort any ongoing transaction"""
        if self._connection is not None:
            self._connection.rollback()
        if self._update_cursor is not None:
            self._update_cursor.close()
            self._update_cursor = None

    def _estimate_cardinality(self, subject, predicate, obj):
        """
            Estimate the cardinality of a triple pattern using PostgreSQL histograms.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern

            Returns:
                The estimated cardinality of the triple pattern
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None

        # estimate the selectivity of the triple pattern using PostgreSQL histograms
        selectivity = 1
        # compute the selectivity of a bounded subject
        if subject is not None:
            if subject in self._subject_histograms['selectivities']:
                selectivity *= self._subject_histograms['selectivities'][subject]
            else:
                selectivity *= (1 - self._subject_histograms['sum_freqs'])/(self._subject_histograms['n_distinct'] - len(self._subject_histograms['selectivities']))
        # compute the selectivity of a bounded predicate
        if predicate is not None:
            if predicate in self._predicate_histograms['selectivities']:
                selectivity *= self._predicate_histograms['selectivities'][predicate]
            else:
                selectivity *= (1 - self._predicate_histograms['sum_freqs'])/(self._predicate_histograms['n_distinct'] - len(self._predicate_histograms['selectivities']))
        # compute the selectivity of a bounded object
        if obj is not None:
            if obj in self._object_histograms['selectivities']:
                selectivity *= self._object_histograms['selectivities'][obj]
            else:
                selectivity *= (1 - self._object_histograms['sum_freqs'])/(self._object_histograms['n_distinct'] - len(self._object_histograms['selectivities']))
        # estimate the cardinality from the estimated selectivity
        cardinality = int(ceil(selectivity * self._avg_row_count))
        return cardinality if cardinality > 0 else 1

    def search(self, subject, predicate, obj, last_read=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan

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

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator
        # otherwise, we might reset a cursor whose results were not fully consumed
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
        iterator = PostgresIterator(cursor, self._connection, start_query, start_params, self._table_name, pattern)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config):
        """Build a PostgresConnector from a configuration object"""
        if 'dbname' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')
        table_name = config['name']
        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        return PostgresConnector(table_name, config['dbname'], config['user'], config['password'], host=host, port=port)

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
        """
        # do warmup if necessary
        self.open()
        # start transaction
        self.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            # cursor = self._connection.cursor()
            insert_query = get_insert_query(self._table_name)
            self._update_cursor.execute(insert_query, (subject, predicate, obj))
            # self._connection.commit()
            # cursor.close()

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple from the RDF Graph.
        """
        # do warmup if necessary
        self.open()
        # start transaction
        self.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            # cursor = self._connection.cursor()
            delete_query = get_delete_query(self._table_name)
            self._update_cursor.execute(delete_query, (subject, predicate, obj))
            # self._connection.commit()
            # cursor.close()
