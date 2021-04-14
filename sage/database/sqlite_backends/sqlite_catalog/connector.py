import json
import logging
import coloredlogs

from math import ceil
from time import time
from functools import reduce

from sage.database.utils import get_kind
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.sqlite_backends.connector import SQliteConnector
from sage.database.sqlite_backends.sqlite_catalog.queries import get_start_query, get_resume_query
from sage.database.sqlite_backends.sqlite_catalog.queries import get_insert_query, get_delete_query, get_catalog_insert_query
from sage.database.sqlite_backends.sqlite_catalog.queries import get_locate_query

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


class CatalogSQliteIterator(DBIterator):
    """A CatalogSQliteIterator implements a DBIterator for a triple pattern evaluated using a SQlite database file"""

    def __init__(self, cursor, connection, start_query, start_params, table_name, pattern, fetch_size=2000):
        super(CatalogSQliteIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        self._cursor.execute(self._current_query, start_params)
        # always keep the current set of rows buffered inside the iterator
        self._last_reads = self._cursor.fetchmany(size=1)

    def __del__(self):
        """Destructor (close the database cursor)"""
        self._cursor.close()

    def last_read(self):
        """Return the index ID of the last element read"""
        if not self.has_next():
            return ''
        triple = self._last_reads[0]
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2]
        }, separators=(',', ':'))

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        if not self.has_next():
            return None
        return self._last_reads.pop(0)

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0


class CatalogSQliteConnector(SQliteConnector):
    """
        A CatalogSQliteConnector search for RDF triples in a SQlite database.
        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - database `str`: the name of the sqlite database file.
            - fetch_size `int`: how many RDF triples are fetched per SQL query (default to 2000)
    """

    def __init__(self, table_name, database, fetch_size=2000):
        super(CatalogSQliteConnector, self).__init__(table_name, database, fetch_size)

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

        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator.
        # Otherwise, we might reset a cursor whose results were not fully consumed.
        cursor = self._manager.get_connection().cursor()

        # create a SQL query to start a new index scan
        if last_read is None:
            start_query, start_params = get_start_query(subject, predicate, obj, self._table_name)
        else:
            # empty last_read key => the scan has already been completed
            if len(last_read) == 0:
                return EmptyIterator(pattern), 0
            # otherwise, create a SQL query to resume the index scan
            last_read = json.loads(last_read)
            t = (last_read["s"], last_read["p"], last_read["o"])
            start_query, start_params = get_resume_query(subject, predicate, obj, t, self._table_name)

        # create the iterator to yield the matching RDF triples
        iterator = CatalogSQliteIterator(
            cursor, self._manager.get_connection(),
            start_query, start_params,
            self._table_name,
            pattern,
            fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config):
        """Build a SQliteConnector from a configuration object"""
        if 'database' not in config:
            raise SyntaxError(
                'A valid configuration for a SQlite connector must contains the database file')

        table_name = config['name']
        database = config['database']
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 5000

        return CatalogSQliteConnector(table_name, database, fetch_size=fetch_size)

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            # Insert triple terms into a SQlite database
            insert_query = get_catalog_insert_query()
            values = dict()
            values[subject] = 0
            values[predicate] = 0
            values[obj] = 0
            transaction.executemany(insert_query, [ [x] for x in values.keys() ])
            # Retrieve inserted RDF terms identifier
            select_id_query = get_locate_query()
            transaction.execute(select_id_query, [subject])
            subject_id = transaction.fetchone()[0]
            transaction.execute(select_id_query, [predicate])
            predicate_id = transaction.fetchone()[0]
            transaction.execute(select_id_query, [obj])
            obj_id = transaction.fetchone()[0]
            # Insert a new RDF triple into a SQlite database
            insert_query = get_insert_query(self._table_name)
            transaction.execute(insert_query, (subject_id, predicate_id, obj_id))
            self._manager.commit()

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple from the RDF Graph.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            delete_query = get_delete_query(self._table_name)
            transaction.execute(delete_query, (subject, predicate, obj))
            self._manager.commit()
