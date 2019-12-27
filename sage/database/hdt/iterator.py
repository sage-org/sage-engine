# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Tuple

from hdt import TripleIterator

from sage.database.db_iterator import DBIterator


class HDTIterator(DBIterator):
    """An HDTIterator implements a DBIterator for scanning RDF triples in a HDT file.
    
    Args:
      * source: HDT iterator which scans for RDF triples from a HDT file.
      * pattern: Triple pattern scanned.
      * start_offset: Initial offset of the source iterator. Used to compute the `last_read` triple when preemption occurs.
    """

    def __init__(self, source: TripleIterator, pattern: Dict[str, str], start_offset=0):
        super(HDTIterator, self).__init__(pattern)
        self._source = source
        self._start_offset = start_offset

    def last_read(self) -> str:
        """Return the ID of the last element read"""
        return str(self._source.nb_reads + self._start_offset)

    def next(self) -> Tuple[str, str, str]:
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        return next(self._source)

    def has_next(self) -> bool:
        """Return True if there is still results to read, and False otherwise"""
        return self._source.has_next()
