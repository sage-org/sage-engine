import json
import logging
import coloredlogs

from datetime import datetime
from math import ceil
from typing import Optional, List, Dict, Tuple
from uuid import uuid4
from time import time

from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgres_backends.connector import PostgresConnector
from sage.database.postgres_backends.postgres.queries import get_delete_query, get_insert_query
from sage.database.postgres_backends.postgres.queries import get_start_query, get_resume_query

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


class DefaultPostgresIterator(DBIterator):
    """A DefaultPostgresIterator fetches RDF triples from a PostgreSQL table using batch queries and lazy loading.

    Args:
      * cursor: A psycopg cursor. This cursor must only be used for this iterator, to avoid side-effects.
      * connection: A psycopg connection.
      * start_query: Prepared SQL query executed to fetch RDF triples as SQL rows.
      * start_params: Parameters to use with the prepared SQL query.
      * pattern: Triple pattern scanned.
      * fetch_size: The number of SQL rows/RDF triples to fetch per batch.
    """

    def __init__(self, cursor, connection, start_query: str, start_params: List[str], pattern: Dict[str, str], fetch_size: int = 2000):
        super(DefaultPostgresIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._fetch_size = fetch_size
        # resume query execution with a SQL query
        self._cursor.execute(self._current_query, start_params)
        # always keep the current set of rows buffered inside the iterator
        self._last_reads = self._cursor.fetchmany(size=self._fetch_size)

    def __del__(self) -> None:
        """Destructor (close the database cursor)"""
        self._cursor.close()

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        if not self.has_next():
            return ''
        triple = self._last_reads[0]
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2]
        }, separators=(',', ':'))

    def next(self) -> Optional[Dict[str, str]]:
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        start = time()
        if not self.has_next():
            return None
        triple = self._last_reads.pop(0)
        logger.debug(f'database access time: {(time() - start) * 1000}ms')
        return triple

    def has_next(self) -> bool:
        """Return True if there is still results to read, False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0


class DefaultPostgresConnector(PostgresConnector):
    """A DefaultPostgresConnector search for RDF triples in a PostgreSQL database.

    Args:
      * table_name: Name of the SQL table containing RDF data.
      * dbname: the database name.
      * user: user name used to authenticate.
      * password: password used to authenticate.
      * host: database host address (defaults to UNIX socket if not provided).
      * port: connection port number (defaults to 5432 if not provided).
      * fetch_size: The number of SQL rows/RDF triples to fetch per batch (defaults to 2000).
    """

    def __init__(self, table_name: str, dbname: str, user: str, password: str, host: str = '', port: int = 5432, fetch_size: int = 2000):
        super(DefaultPostgresConnector, self).__init__(table_name, dbname, user, password, host, port, fetch_size)

    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[DefaultPostgresIterator, int]:
        """Get an iterator over all RDF triples matching a triple pattern.

        Args:
          * subject: Subject of the triple pattern.
          * predicate: Predicate of the triple pattern.
          * object: Object of the triple pattern.
          * last_read: A RDF triple ID. When set, the search is resumed for this RDF triple.
          * as_of: A version timestamp. When set, perform all reads against a consistent snapshot represented by this timestamp.

        Returns:
          A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern.
        """
        # do warmup if necessary
        self.open()

        # format triple patterns for the PostgreSQL API
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator.
        # Otherwise, we might reset a cursor whose results were not fully consumed.
        cursor = self._manager.get_connection().cursor(str(uuid4()))

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
        iterator = DefaultPostgresIterator(cursor, self._manager.get_connection(), start_query, start_params, pattern, fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config: dict):
        """Build a DefaultPostgresConnector from a configuration object.

        The configuration object must contains the following fields: 'dbname', 'name', 'user' and 'password'.
        Optional fields are: 'host', 'port' and 'fetch_size'.
        """
        if 'dbname' not in config or 'name' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')

        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 2000

        return DefaultPostgresConnector(config['name'], config['dbname'], config['user'], config['password'], host=host, port=port, fetch_size=fetch_size)

    def insert(self, subject: str, predicate: str, obj: str) -> None:
        """Insert a RDF triple into the RDF graph.

        Args:
          * subject: Subject of the RDF triple.
          * predicate: Predicate of the RDF triple.
          * obj: Object of the RDF triple.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            insert_query = get_insert_query(self._table_name)
            transaction.execute(insert_query, (subject, predicate, obj))
            self._manager.commit()

    def delete(self, subject: str, predicate: str, obj: str) -> None:
        """Delete a RDF triple from the RDF graph.

        Args:
          * subject: Subject of the RDF triple.
          * predicate: Predicate of the RDF triple.
          * obj: Object of the RDF triple.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            delete_query = get_delete_query(self._table_name)
            transaction.execute(delete_query, (subject, predicate, obj))
            self._manager.commit()
