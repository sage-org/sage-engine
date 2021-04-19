# nlj.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedIndexJoinIterator, TriplePattern
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class IndexJoinIterator(PreemptableIterator):
    """A IndexJoinIterator implements an Index Loop join in a pipeline of iterators.

    Args:
      * left: Previous iterator in the pipeline, i.e., the outer relation of the join.
      * right: Next iterator in the pipeline, i.e., the inner relation of the join.
      * context: Information about the query execution.
      * current_mappings: The current mappings when the join is performed.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator, context: dict, current_mappings: Optional[Dict[str, str]] = None):
        super(IndexJoinIterator, self).__init__()
        self._left = left
        self._right = right
        self._current_mappings = current_mappings

    def __repr__(self) -> str:
        return f"<IndexJoinIterator ({self._left} JOIN {self._right} WITH {self._current_mappings})>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "join"

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._current_mappings = None
        self._left.next_stage(mappings)

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._left.has_next() or (self._current_mappings is not None and self._right.has_next())

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if not self.has_next():
            return None
        while self._current_mappings is None or not self._right.has_next():
            self._current_mappings = await self._left.next()
            if self._current_mappings is None:
                return None
            self._right.next_stage(self._current_mappings)
        mu = await self._right.next()
        if mu is not None:
            return {**self._current_mappings, **mu}
        return None

    def save(self) -> SavedIndexJoinIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_join = SavedIndexJoinIterator()
        # export left source
        left_field = self._left.serialized_name() + '_left'
        getattr(saved_join, left_field).CopyFrom(self._left.save())
        # export right source
        right_field = self._right.serialized_name() + '_right'
        getattr(saved_join, right_field).CopyFrom(self._right.save())
        if self._current_mappings is not None:
            pyDict_to_protoDict(self._current_mappings, saved_join.muc)
        return saved_join
