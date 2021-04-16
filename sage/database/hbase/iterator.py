# iterator.py
# Author: Thomas MINIER - MIT License 2019
from typing import Optional, List, Dict, Tuple

from sage.database.db_iterator import DBIterator


class HBaseIterator(DBIterator):
    """A HBaseIterator scan for results"""

    def __init__(self, connection, scanner, pattern):
        super(HBaseIterator, self).__init__(pattern)
        self._connection = connection
        self._scanner = scanner
        self._last_read_key, self._last_read_triple = '', None
        self._subject = pattern['subject'].encode('utf-8') if pattern['subject'] is not None else None
        self._predicate = pattern['predicate'].encode('utf-8') if pattern['predicate'] is not None else None
        self._object = pattern['object'].encode('utf-8') if pattern['object'] is not None else None
        self._warmup = True

    def __del__(self) -> None:
        """Destructor (close the database connection)"""
        # self._connection.close()
        pass

    def __fetch_one(self) -> None:
        try:
            self._last_read_key, self._last_read_triple = next(self._scanner)
        except StopIteration:
            self._last_read_key, self._last_read_triple = '', None
        finally:
            self._warmup = False

    def __is_relevant_triple(self, triple: Dict[bytes, str]) -> bool:
        """Return True if the RDF triple matches the triple pattern scanned"""
        if self._subject is not None and triple[b'rdf:subject'] != self._subject:
            return False
        elif self._predicate is not None and triple[b'rdf:predicate'] != self._predicate:
            return False
        elif self._object is not None and triple[b'rdf:object'] != self._object:
            return False
        return True

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        return self._last_read_key

    def next(self) -> Optional[Dict[str, str]]:
        """Return the next solution mapping"""
        if self._warmup:
            # self._connection.open()
            self.__fetch_one()
        triple = self._last_read_triple
        if triple is None or not self.__is_relevant_triple(triple):
            return None
        self.__fetch_one()
        triple = (triple[b'rdf:subject'].decode('utf-8'), triple[b'rdf:predicate'].decode('utf-8'), triple[b'rdf:object'].decode('utf-8'))
        return triple

    def has_next(self) -> bool:
        """Return True if there are still results to read, and False otherwise"""
        if self._warmup:
            # self._connection.open()
            self.__fetch_one()
        return self._last_read_triple is not None and self.__is_relevant_triple(self._last_read_triple)
