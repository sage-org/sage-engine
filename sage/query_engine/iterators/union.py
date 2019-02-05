# union.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBagUnionIterator
from random import random


class BagUnionIterator(PreemptableIterator):
    """BagUnionIterator performs a Bag Union between two input iterators"""

    def __init__(self, left, right):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right

    def __repr__(self):
        return '<BagUnionIterator {%s} UNION {%s}>' % (self._left, self._right)

    def serialized_name(self):
        return "union"

    def has_next(self):
        return self._left.has_next() or self._right.has_next()

    async def next(self):
        """
        Get the next item from the iterator, reading from the left source and then the right source
        """
        if not self.has_next():
            raise StopIteration()
        elif self._left.has_next():
            return await self._left.next()
        else:
            return await self._right.next()

    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        saved_union = SavedBagUnionIterator()
        # export left source
        left_field = self._left.serialized_name() + '_left'
        getattr(saved_union, left_field).CopyFrom(self._left.save())
        # export right source
        right_field = self._right.serialized_name() + '_right'
        getattr(saved_union, right_field).CopyFrom(self._right.save())
        return saved_union


class RandomBagUnionIterator(BagUnionIterator):
    """RandomBagUnionIterator performs a Bag Union between two input iterators,
    and randomly select the iterator to read at each call to next()
    """

    def __init__(self, left, right):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right

    async def next(self):
        """
        Get the next item from the iterator, reading from a source randomly selected
        """
        if not self.has_next():
            raise StopIteration()
        elif random() < 0.5:
            if self._left.has_next():
                return await self._left.next()
            else:
                return await self._right.next()
        else:
            if self._right.has_next():
                return await self._right.next()
            else:
                return await self._left.next()
