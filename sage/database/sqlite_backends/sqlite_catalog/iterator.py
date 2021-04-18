import json

from typing import Optional, List, Dict, Tuple

from sage.database.db_iterator import DBIterator

class SQliteIterator(DBIterator):
    """A SQliteIterator implements a DBIterator for a triple pattern evaluated using a SQlite database file"""

    def __init__(self, cursor, connection, start_query: str, start_params: List[str], table_name: str, pattern: Dict[str, str], fetch_size: int = 500):
        super(SQliteIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        self._cursor.execute(self._current_query, start_params)
        self._last_reads = self._cursor.fetchmany(size=1)

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
        """Return the next solution mapping or None if there are no more solutions"""
        if not self.has_next():
            return None
        return self._last_reads.pop(0)

    def has_next(self) -> bool:
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0
