import json

from datetime import datetime
from typing import Optional, List, Dict, Tuple

from sage.database.backends.db_iterator import DBIterator


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
        self._buffer = self._cursor.fetchmany(size=1)
        self._last_read = None

    def __del__(self) -> None:
        """Destructor"""
        self._cursor.close()

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        if self._last_read is None or self._last_read == '':
            return self._last_read
        else:
            return json.dumps({
                's': self._last_read[0],
                'p': self._last_read[1],
                'o': self._last_read[2],
                'ins': self._last_read[3].isoformat(),
                'del': self._last_read[4].isoformat()
            }, separators=(',', ':'))

    def next(self) -> Optional[Tuple[str, str, str, Optional[datetime], Optional[datetime]]]:
        """Return the next solution mapping or None if there are no more solutions"""
        if len(self._buffer) == 0:
            self._buffer = self._cursor.fetchmany(size=self._fetch_size)
        if len(self._buffer) == 0:
            self._last_read = ''  # scan complete
            return None
        else:
            self._last_read = self._buffer.pop(0)
            return self._last_read
