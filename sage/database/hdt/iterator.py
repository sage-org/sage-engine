# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
from sage.database.db_iterator import DBIterator


class HDTIterator(DBIterator):
    """An HDTIterator implements a DBIterator for a triple pattern evaluated using an HDT file"""

    def __init__(self, source, pattern, start_offset=0):
        super(HDTIterator, self).__init__(pattern)
        self._source = source
        self._start_offset = start_offset

    def last_read(self):
        """Return the ID of the last element read"""
        return str(self._source.nb_reads + self._start_offset)

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        return next(self._source)

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        return self._source.has_next()
