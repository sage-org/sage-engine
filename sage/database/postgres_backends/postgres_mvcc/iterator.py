import json

from typing import Optional, List, Dict, Tuple

from sage.database.db_iterator import DBIterator

class PostgresIterator(DBIterator):
    """A PostgresIterator fetches RDF triples from a versionned PostgreSQL table using batch queries and lazy loading.

    Args:
      * cursor: Psycopg cursor used to query the database.
      * start_time: Timestamp at which the iterator should read.
      * start_query: Prepared SQL query used to start iteration.
      * start_params: SQL params to apply to the prepared SQL query.
      * table_name: Name of the SQL table to scan.
      * pattern: Triple pattern scanned.
      * fetch_size: The number of SQL rows/RDF triples to fetch per batch.
    """

    def __init__(self, cursor, start_time: datetime, start_query: str, start_params: List[str], table_name: str, pattern: Dict[str, str], fetch_size: int = 500):
        super(PostgresIterator, self).__init__(pattern)
        self._cursor = cursor
        self._start_time = start_time
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        self._cursor.execute(self._current_query, start_params)
        self._last_reads = self._cursor.fetchmany(size=1)

    def __del__(self) -> None:
        """Destructor"""
        self._cursor.close()

    def last_read(self) -> str:
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

    def next(self) -> Optional[Dict[str, str]]:
        """Return the next solution mapping or None if there are no more solutions"""
        if not self.has_next():
            return None
        triple = self._last_reads.pop(0)

        # extract timestamps from the RDF triple
        insert_t = triple[3]
        delete_t = triple[4]

        triple = self._last_reads.pop(0)

        # case 1: the current triple is in the valid version, so it is a match
        if insert_t <= self._start_time and self._start_time < delete_t:
            return (triple[0], triple[1], triple[2])
        # case 2: do a NONE forward to trigger another iteration loop
        # to find a matching RDF triple
        return None

    def has_next(self) -> bool:
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0
