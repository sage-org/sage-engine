# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
import json
from datetime import datetime
from math import ceil
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgres.queries import (get_delete_query, get_insert_query,
                                            get_resume_query, get_start_query)
from sage.database.postgres.transaction_manager import TransactionManager
from sage.database.postgres.utils import id_to_predicate


def fetch_histograms(cursor, table_name: str, attribute_name: str) -> Tuple[int, int, Dict[str, float], int]:
    """Download PostgreSQL histograms from a given table and attribute.

    Args:
      * cursor: A psycopg cursor.
      * table_name: Name of the SQL table from which we should retrieve histograms.
      * attribute_name: Table attribute from which we should retrieve histograms.
    
    Returns:
      A tuple (`null_frac`, `n_distinct`, `selectivities`, `sum_most_common_freqs`) where:
      * `null_frac` is the fraction of null values in the histogram.
      * `n_distinct` is the estimated number of distinct values for this attribute.
      * `selectivities` is the estimated selectivities of the attribute's values in the table.
      * `sum_most_common_freqs` is the num of the frequencies of the most common values for this attribute.
    """
    base_query = f"SELECT null_frac, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = '{table_name}' AND attname = '{attribute_name}'"
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
    """A PostgresIterator fetches RDF triples from a PostgreSQL table using batch queries and lazy loading.
    
    Args:
      * cursor: A psycopg cursor. This cursor must only be used for this iterator, to avoid side-effects.
      * connection: A psycopg connection.
      * start_query: Prepared SQL query executed to fetch RDF triples as SQL rows.
      * start_params: Parameters to use with the prepared SQL query.
      * pattern: Triple pattern scanned.
      * fetch_size: The number of SQL rows/RDF triples to fetch per batch.
    """

    def __init__(self, cursor, connection, start_query: str, start_params: List[str], pattern: Dict[str, str], fetch_size: int = 2000):
        super(PostgresIterator, self).__init__(pattern)
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
        if not self.has_next():
            return None
        triple = self._last_reads.pop(0)
        # decode the triple's predicate (if needed)
        triple = triple[0], id_to_predicate(triple[1]), triple[2]
        return triple

    def has_next(self) -> bool:
        """Return True if there is still results to read, False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0


class PostgresConnector(DatabaseConnector):
    """A PostgresConnector search for RDF triples in a PostgreSQL database.

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
        super(PostgresConnector, self).__init__()
        self._table_name = table_name
        self._manager = TransactionManager(dbname, user, password, host=host, port=port)
        self._fetch_size = fetch_size
        self._warmup = True

        # Data used for cardinality estimation.
        # They are initialized using PostgreSQL histograms, after the 1st connection to the DB.
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

    def open(self) -> None:
        """Open the connection to the PostgreSQL database and initialize histograms."""
        if self._manager.is_open():
            self._manager.open_connection()

        # Do warmup phase if required, i.e., fetch stats for query execution
        if self._warmup:
            cursor = self._manager.start_transaction()
            # fetch estimated table cardinality
            cursor.execute(f"SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '{self._table_name}'")
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
            self._manager.commit()
            self._warmup = False

    def close(self) -> None:
        """Close the database connection"""
        # commit, then close the cursor and the connection
        self._manager.close_connection()

    def start_transaction(self) -> None:
        """Start a PostgreSQL transaction"""
        self._manager.start_transaction()

    def commit_transaction(self) -> None:
        """Commit any ongoing transaction"""
        self._manager.commit()

    def abort_transaction(self) -> None:
        """Abort any ongoing transaction"""
        self._manager.abort()

    def _estimate_cardinality(self, subject: Optional[str], predicate: Optional[str], obj: Optional[str]) -> int:
        """Estimate the cardinality of a triple pattern using PostgreSQL histograms.

        Args:
          * subject: Subject of the triple pattern.
          * predicate: Predicate of the triple pattern.
          * obj: Object of the triple pattern.

        Returns:
          The estimated cardinality of the triple pattern.
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        # try to encode predicate if needed
        # if predicate is not None:
        #     predicate = predicate_to_id(predicate)

        # estimate the selectivity of the triple pattern using PostgreSQL histograms
        selectivity = 1
        # avoid division per zero when some histograms are not fully up-to-date
        try:
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
        except ZeroDivisionError:
            pass
        # estimate the cardinality from the estimated selectivity
        cardinality = int(ceil(selectivity * self._avg_row_count))
        return cardinality if cardinality > 0 else 1

    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[PostgresIterator, int]:
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
        # try to encode predicate (if needed)
        # if predicate is not None:
        #     predicate = predicate_to_id(predicate)

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
        iterator = PostgresIterator(cursor, self._manager.get_connection(), start_query, start_params, pattern, fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config: dict):
        """Build a PostgresConnector from a configuration object.
        
        The configuration object must contains the following fields: 'dbname', 'name', 'user' and 'password'.
        Optional fields are: 'host', 'port' and 'fetch_size'.
        """
        if 'dbname' not in config or 'name' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')

        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 2000

        return PostgresConnector(config['name'], config['dbname'], config['user'], config['password'], host=host, port=port, fetch_size=fetch_size)

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
