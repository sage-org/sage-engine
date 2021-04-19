# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
from math import ceil
from typing import Dict, List, Optional, Tuple

from sage.database.db_connector import DatabaseConnector
from sage.database.postgres_backends.transaction_manager import TransactionManager


class PostgresConnector(DatabaseConnector):
    """A PostgresConnector search for RDF triples in a PostgreSQL database.

    Args:
      * table_name: Name of the SQL table containing RDF data.
      * dbname: the database name.
      * user: user name used to authenticate.
      * password: password used to authenticate.
      * host: database host address (defaults to UNIX socket if not provided).
      * port: connection port number (defaults to 5432 if not provided).
      * fetch_size: The number of SQL rows/RDF triples to fetch per batch (defaults to 500).
    """

    def __init__(self, table_name: str, dbname: str, user: str, password: str, host: str = '', port: int = 5432, fetch_size: int = 500):
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

    def _fetch_histograms(self, cursor, table_name: str, attribute_name: str) -> Tuple[int, int, Dict[str, float], int]:
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
            (null_frac, n_distinct, selectivities, sum_freqs) = self._fetch_histograms(cursor, self._table_name, 'subject')
            self._subject_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch predicate histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = self._fetch_histograms(cursor, self._table_name, 'predicate')
            self._predicate_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch object histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = self._fetch_histograms(cursor, self._table_name, 'object')
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

    def _estimate_cardinality(self, subject, predicate, obj) -> int:
        """Estimate the cardinality of a triple pattern using PostgreSQL histograms.

        Args:
          * subject: Subject of the triple pattern.
          * predicate: Predicate of the triple pattern.
          * obj: Object of the triple pattern.

        Returns:
          The estimated cardinality of the triple pattern.
        """
        # estimate the selectivity of the triple pattern using PostgreSQL histograms
        selectivity = 1
        # avoid division per zero when some histograms are not fully up-to-date
        try:
            # compute the selectivity of a bounded subject
            if subject is not None:
                if subject in self._subject_histograms['selectivities']:
                    selectivity *= self._subject_histograms['selectivities'][str(subject)]
                else:
                    selectivity *= (1 - self._subject_histograms['sum_freqs'])/(self._subject_histograms['n_distinct'] - len(self._subject_histograms['selectivities']))
            # compute the selectivity of a bounded predicate
            if predicate is not None:
                if predicate in self._predicate_histograms['selectivities']:
                    selectivity *= self._predicate_histograms['selectivities'][str(predicate)]
                else:
                    selectivity *= (1 - self._predicate_histograms['sum_freqs'])/(self._predicate_histograms['n_distinct'] - len(self._predicate_histograms['selectivities']))
            # compute the selectivity of a bounded object
            if obj is not None:
                if obj in self._object_histograms['selectivities']:
                    selectivity *= self._object_histograms['selectivities'][str(obj)]
                else:
                    selectivity *= (1 - self._object_histograms['sum_freqs'])/(self._object_histograms['n_distinct'] - len(self._object_histograms['selectivities']))
        except ZeroDivisionError:
            pass
        # estimate the cardinality from the estimated selectivity
        cardinality = int(ceil(selectivity * self._avg_row_count))
        return cardinality if cardinality > 0 else 1
