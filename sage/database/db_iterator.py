# db_iterator.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class DBIterator(ABC):
    """
        A DBIterator follows the iterator protocol and evaluates a triple pattern against a RDF dataset.
        Typically, a subclass of this iterator is returned by a call to DBConnector#search_pattern.
    """

    def __init__(self, pattern):
        super(DBIterator, self).__init__()
        self._pattern = pattern

    @property
    def subject(self):
        return self._pattern["subject"]

    @property
    def predicate(self):
        return self._pattern["predicate"]

    @property
    def object(self):
        return self._pattern["object"]

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @abstractmethod
    def last_read(self):
        """Return the index ID of the last element read"""
        pass

    @abstractmethod
    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        pass

    @abstractmethod
    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        pass
