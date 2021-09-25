# nlj.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional, Set

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedIndexJoinIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class IndexJoinIterator(PreemptableIterator):
    """A IndexJoinIterator implements an Index Loop join in a pipeline of iterators.

    Args:
      * left: Previous iterator in the pipeline, i.e., the outer relation of the join.
      * right: Next iterator in the pipeline, i.e., the inner relation of the join.
      * context: Information about the query execution.
      * current_mappings: The current mappings when the join is performed.
    """

    def __init__(
        self, left: PreemptableIterator, right: PreemptableIterator, context: dict,
        produced: int = 0, consumed: int = 0,
        current_mappings: Optional[Dict[str, str]] = None
    ):
        super(IndexJoinIterator, self).__init__()
        self._left = left
        self._right = right
        self._current_mappings = current_mappings
        self._produced = produced
        self._consumed = consumed

    def __repr__(self) -> str:
        return f"<IndexJoinIterator ({self._left} JOIN {self._right} WITH {self._current_mappings})>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "join"

    def explain(self, height: int = 0, step: int = 3) -> None:
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}IndexJoinIterator MAPPINGS <{self._current_mappings}>')
        self._left.explain(height=(height + step), step=step)
        self._right.explain(height=(height + step), step=step)

    def variables(self) -> Set[str]:
        return self._left.variables().union(self._right.variables())

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._current_mappings = None
        self._left.next_stage(mappings)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        while True:
            if self._current_mappings is None:
                self._current_mappings = await self._left.next()
                if self._current_mappings is None:
                    return None
                self._consumed += 1
                self._right.next_stage(self._current_mappings)
            else:
                mappings = await self._right.next()
                if mappings is not None:
                    self._produced += 1
                    return mappings
                self._current_mappings = None

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
        # export statistics used to estimate the cardinality of the query
        saved_join.produced = self._produced
        saved_join.consumed = self._consumed
        return saved_join
