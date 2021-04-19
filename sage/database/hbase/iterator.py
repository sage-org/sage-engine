# iterator.py
# Author: Thomas MINIER - MIT License 2019
from typing import Optional, List, Dict, Tuple

from sage.database.db_iterator import DBIterator


class HBaseIterator(DBIterator):
    """A HBaseIterator scan for results"""

    def __init__(self, connection, table, row_key, pattern):
        super(HBaseIterator, self).__init__(pattern)
        self._table = table
        self._connection = connection
        self._subject = pattern['subject'].encode('utf-8') if pattern['subject'] is not None else None
        self._predicate = pattern['predicate'].encode('utf-8') if pattern['predicate'] is not None else None
        self._object = pattern['object'].encode('utf-8') if pattern['object'] is not None else None
        self._current_page = list()
        self._has_next_page = False
        self._last_read_key = row_key
        self.__fetch_many(limit=1, skip_first=False)

    def __is_relevant_triple(self, triple: Dict[bytes, str]) -> bool:
        """Return True if the RDF triple matches the triple pattern scanned"""
        if self._subject is not None and triple[b'rdf:subject'] != self._subject:
            return False
        elif self._predicate is not None and triple[b'rdf:predicate'] != self._predicate:
            return False
        elif self._object is not None and triple[b'rdf:object'] != self._object:
            return False
        return True

    def __decode_triple(self, triple):
        """Return a RDF triple where terms are string to be conformed with SaGe"""
        return (
            triple[b'rdf:subject'].decode('utf-8'),
            triple[b'rdf:predicate'].decode('utf-8'),
            triple[b'rdf:object'].decode('utf-8')
        )

    def __fetch_many(self, limit=500, skip_first=True):
        scanner = self._table.scan(row_start=self._last_read_key, limit=limit + 1, batch_size=limit + 1)
        self._has_next_page = False
        for key, triple in scanner:
            if not self.__is_relevant_triple(triple):
                break
            self._current_page.append((key, self.__decode_triple(triple)))
        if len(self._current_page) == (limit + 1):
            self._has_next_page = True
        if skip_first:
            self._current_page.pop(0)
        scanner.close()

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        if not self.has_next():
            return self._last_read_key # whatever as long as the key is not in the HBase database...
        (key, triple) = self._current_page[0]
        return key

    def next(self) -> Optional[Dict[str, str]]:
        """Return the next solution mapping or None if there are no more solutions"""
        if not self.has_next():
            return None
        self._last_read_key, triple = self._current_page.pop(0)
        return triple

    def has_next(self) -> bool:
        """Return True if there is still results to read, False otherwise"""
        if len(self._current_page) == 0 and self._has_next_page:
            self.__fetch_many()
        return len(self._current_page) > 0
