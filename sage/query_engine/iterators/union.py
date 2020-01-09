# union.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional
from random import random

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBagUnionIterator


class BagUnionIterator(PreemptableIterator):
    """A BagUnionIterator performs a SPARQL UNION with bag semantics in a pipeline of iterators.

    This operator sequentially produces all solutions from the left operand, 
    and then do the same for the right operand.
    
    Args:
      * left: left operand of the union.
      * right: right operand of the union.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right

    def __repr__(self):
        return f"<BagUnionIterator {self._left} UNION {self._right}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "union"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._left.has_next() or self._right.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        elif self._left.has_next():
            return await self._left.next()
        else:
            return await self._right.next()

    def save(self) -> SavedBagUnionIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_union = SavedBagUnionIterator()
        # export left source
        left_field = self._left.serialized_name() + '_left'
        getattr(saved_union, left_field).CopyFrom(self._left.save())
        # export right source
        right_field = self._right.serialized_name() + '_right'
        getattr(saved_union, right_field).CopyFrom(self._right.save())
        return saved_union


class RandomBagUnionIterator(BagUnionIterator):
    """A RandomBagUnionIterator performs a SPARQL UNION with bag semantics in a pipeline of iterators.

    This operator randomly reads from the left and right operands to produce solution mappings.
    
    Args:
      * left: left operand of the union.
      * right: right operand of the union.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
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
