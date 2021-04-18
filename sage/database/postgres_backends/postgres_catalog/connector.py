import json
import logging
import coloredlogs

from datetime import datetime
from math import ceil
from uuid import uuid4
from typing import Optional, Dict, Tuple
from psycopg2.extras import execute_values

from sage.database.db_iterator import EmptyIterator
from sage.database.postgres_backends.connector import PostgresConnector
from sage.database.postgres_backends.postgres_catalog.iterator import PostgresIterator
from sage.database.postgres_backends.postgres_catalog.queries import get_delete_query, get_insert_query, get_catalog_insert_many_query
from sage.database.postgres_backends.postgres_catalog.queries import get_start_query, get_resume_query
from sage.database.postgres_backends.postgres_catalog.queries import get_extract_query, get_locate_query

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


class CatalogPostgresConnector(PostgresConnector):
    """A CatalogPostgresConnector search for RDF triples in a PostgreSQL database.

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
        super(CatalogPostgresConnector, self).__init__(table_name, dbname, user, password, host, port, fetch_size)

    def _fetch_histograms(self, cursor, table_name: str, attribute_name: str) -> Tuple[int, int, Dict[str, float], int]:
        """Download PostgreSQL histograms from a given table and attribute when using a catalog schema.

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
        for common_val_identifier in most_common_vals[1:-1].split(","):
            if cpt < len(most_common_freqs):
                cursor.execute(get_extract_query(), [(common_val_identifier, )])
                result = cursor.fetchone()
                common_val = "" if result is None else result[0]
                selectivities[common_val] = most_common_freqs[cpt]
            cpt += 1
        return (null_frac, n_distinct, selectivities, sum(most_common_freqs))

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

    def from_config(config: dict) -> PostgresConnector:
        """Build a CatalogPostgresConnector from a configuration object.

        The configuration object must contains the following fields: 'dbname', 'name', 'user' and 'password'.
        Optional fields are: 'host', 'port' and 'fetch_size'.
        """
        if 'dbname' not in config or 'name' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError('A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')

        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 500

        return CatalogPostgresConnector(config['name'], config['dbname'], config['user'], config['password'], host=host, port=port, fetch_size=fetch_size)

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
            # Insert triple terms into a PostgreSQL database
            insert_query = get_catalog_insert_many_query()
            values = dict()
            values[subject] = 0
            values[predicate] = 0
            values[obj] = 0
            execute_values(transaction, insert_query, [ [x] for x in values.keys() ])
            # Retrieve inserted RDF terms identifier
            select_id_query = get_locate_query()
            transaction.execute(select_id_query, [subject])
            subject_id = transaction.fetchone()[0]
            transaction.execute(select_id_query, [predicate])
            predicate_id = transaction.fetchone()[0]
            transaction.execute(select_id_query, [obj])
            obj_id = transaction.fetchone()[0]
            # Insert a new RDF triple into a PostgreSQL database
            insert_query = get_insert_query(self._table_name)
            transaction.execute(insert_query, (subject_id, predicate_id, obj_id))
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
