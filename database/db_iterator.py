# db_iterator.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class DBIterator(ABC):
    """
        A DBIterator follows the iterator protocol and gives access to RDF triples matching a given triple pattern.
        Typically, a subclass of this iterator is returned by a call to DBConnector#search_pattern.
    """

    def __init__(self, pattern, limit=0, offset=0):
        super(DBIterator, self).__init__()
        self._pattern = pattern
        self._limit = limit
        self._offset = offset

    @property
    def subject(self):
        return self._pattern["subject"]

    @property
    def predicate(self):
        return self._pattern["predicate"]

    @property
    def object(self):
        return self._pattern["object"]

    @property
    def limit(self):
        return self._limit

    @property
    def offset(self):
        return self._pattern["offset"]

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @abstractmethod
    def next(self):
        pass

    @abstractmethod
    def has_next(self):
        pass
