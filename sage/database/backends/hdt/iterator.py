from typing import Dict, Tuple, Optional
from datetime import datetime
from hdt import TripleIterator

from sage.database.backends.db_iterator import DBIterator


class HDTIterator(DBIterator):
    """
    An HDTIterator implements a DBIterator for scanning RDF triples in a HDT file.

    Parameters
    ----------
    source: TripleIterator
        HDT iterator which scans for RDF triples from a HDT file.
    pattern: Dict[str, str]
        Triple pattern scanned.
    start_offset: int
        Initial offset of the source iterator. Used to compute the `last_read`
        triple when preemption occurs.
    """

    def __init__(
        self, source: TripleIterator, pattern: Dict[str, str],
        start_offset: int = 0
    ) -> None:
        super(HDTIterator, self).__init__(pattern)
        self._source = source
        self._start_offset = start_offset

    def last_read(self) -> str:
        """
        Returns the ID of the last element read.
        """
        return str(self._source.nb_reads + self._start_offset)

    def next(self) -> Optional[Tuple[str, str, str, Optional[datetime], Optional[datetime]]]:
        """
        Returns the next solution mappings or None if there are no more solutions.
        """
        try:
            triple = next(self._source)
            return (triple[0], triple[1], triple[2], None, None)
        except StopIteration:
            return None
