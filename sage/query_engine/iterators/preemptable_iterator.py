# preemptable_iterator.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class PreemptableIterator(ABC):
    """An abstract class for a preemptable iterator"""

    @abstractmethod
    def serialized_name(self):
        """Get the name of the operator when serialized"""
        pass

    @abstractmethod
    async def next(self):
        """
        Get the next item from the iterator, following the iterator protocol.
        Raise `StopIteration` is the iterator cannot produce more items.
        Warning: this function may contains `non interruptible` clauses.
        """
        pass

    @abstractmethod
    def has_next(self):
        """Return True if the iterator has more item to yield"""
        pass

    @abstractmethod
    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        pass
