# db_iterator.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from typing import Dict, Tuple


class DBIterator(ABC):
    """
        A DBIterator follows the iterator protocol and evaluates a triple pattern against a RDF dataset.
        Typically, a subclass of this iterator is returned by a call to DBConnector#search_pattern.
    """

    def __init__(self, pattern: Dict[str, str]):
        super(DBIterator, self).__init__()
        self._pattern = pattern

    @property
    def subject(self) -> str:
        return self._pattern["subject"]

    @property
    def predicate(self) -> str:
        return self._pattern["predicate"]

    @property
    def object(self) -> str:
        return self._pattern["object"]

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @abstractmethod
    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        pass

    @abstractmethod
    def next(self) -> Tuple[str, str, str]:
        """Return the next RDF triple or raise `StopIteration` if there are no more triples to scan"""
        pass

    @abstractmethod
    def has_next(self) -> bool:
        """Return True if there is still results to read, and False otherwise"""
        pass


class EmptyIterator(DBIterator):
    """An iterator that yields nothing and completes immediatly"""

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        return ''

    def next(self) -> None:
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        return None

    def has_next(self) -> bool:
        """Return True if there is still results to read, and False otherwise"""
        return False
