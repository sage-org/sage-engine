# limit.py
# Author: Pascal Molli - MIT License 2017-2020
from time import time
from typing import Dict, List, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedLimitIterator


class LimitIterator(PreemptableIterator):
    """A LimitIterator evaluates a SPARQL Limit (LIMIT) in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * limit: the limit to reach.
      * context: Information about the query execution.
    """

    def __init__(self, source: PreemptableIterator, context: dict, start=0,length=0):
        super(LimitIterator, self).__init__()
        self._source = source
        self._length=length
        self._start=start

    def __repr__(self) -> str:
        return f"<LimitIterator length:{self._length} start:{self._start} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "limit"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if not self.has_next():
            return None
        mappings = await self._source.next()
        if mappings is None:
            return None
        return mappings

    def save(self) -> SavedLimitIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_limit = SavedLimitIterator()
        saved_limit.length=self._length
        saved_limit.start=self._start
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_limit, source_field).CopyFrom(self._source.save())
        return saved_limit
