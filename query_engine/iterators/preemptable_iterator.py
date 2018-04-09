# preemptable_iterator.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class PreemptableIterator(ABC):
    """An abstract class for a preemptable iterator"""

    @abstractmethod
    async def next(self):
        """
        Get the next item from the iterator, following the iterator protocol.
        Raise `StopIteration` is the iterator cannot peoduce more items.
        Warning: this function may contains `non interruptible` clauses.
        """
        pass

    @property
    @abstractmethod
    def has_next(self):
        """Return True if the iterator has more item to yield"""
        pass

    @abstractmethod
    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        pass

    # @abstractmethod
    # def load(self, state=None):
    #     """Reload the state of an iterator from a saved tstate (as produced by a call to iter.save())"""
    #     pass
