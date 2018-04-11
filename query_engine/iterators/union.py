# union.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.preemptable_iterator import PreemptableIterator
from query_engine.protobuf.iterators_pb2 import SavedBagUnionIterator
from random import random


class BagUnionIterator(PreemptableIterator):
    """BagUnionIterator performs a Bag Union between two input iterators"""
    def __init__(self, left, right):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right

    def __repr__(self):
        return '<BagUnionIterator {%s} UNION {%s}>' % (self._left, self._right)

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
        savedUnion = SavedBagUnionIterator()
        savedLeft = self._left.save()
        savedRight = self._right.save()
        # export left source
        if type(self._left).__name__ == 'ScanIterator':
            savedUnion.scan_left.CopyFrom(savedLeft)
        elif type(self._left).__name__ == 'NestedLoopJoinIterator':
            savedUnion.nlj_left.CopyFrom(savedLeft)
        elif type(self._left).__name__ == 'BagUnionIterator':
            savedUnion.union_left.CopyFrom(savedLeft)
        else:
            raise Exception("Unknown left source type for BagUnion")
        # export right source
        if type(self._right).__name__ == 'ScanIterator':
            savedUnion.scan_right.CopyFrom(savedRight)
        elif type(self._right).__name__ == 'NestedLoopJoinIterator':
            savedUnion.nlj_right.CopyFrom(savedRight)
        elif type(self._right).__name__ == 'BagUnionIterator':
            savedUnion.union_right.CopyFrom(savedRight)
        else:
            raise Exception("Unknown right source type for BagUnion")
        return savedUnion


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
