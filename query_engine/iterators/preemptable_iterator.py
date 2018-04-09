# preemptable_iterator.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class PreemptableIterator(ABC):
    """An abstract class for a preemptable iterator"""

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @abstractmethod
    def next(self):
        """
        Get the next item from the iterator, following the iterator protocol.
        Raise `StopIteration` is the iterator cannot peoduce more items.
        Warning: this function may contains `non interruptible` clauses.
        """
        pass

    @property
    @abstractmethod
    def is_closed(self):
        """Return True if the iterator will yield no more items"""
        pass

    @abstractmethod
    def stop(self):
        """Stop the iterator from producing items"""
        pass

    @abstractmethod
    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        pass

    @abstractmethod
    def load(self, state=None):
        """Reload the state of an iterator from a saved tstate (as produced by a call to iter.save())"""
        pass
