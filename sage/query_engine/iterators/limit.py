from typing import Any, Dict, Optional, Set

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedLimitIterator


class LimitIterator(PreemptableIterator):
    """A TOPKIterator evaluates a SPARQL TOP-K in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
    """

    def __init__(self, source: PreemptableIterator, limit: int, produced: int = 0):
        super(LimitIterator, self).__init__()
        self._source = source
        self._limit = limit
        self._produced = produced

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "limit"

    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}LimitIterator (limit={self._limit})')
        self._source.explain(height=(height + step), step=step)

    def variables(self, include_values: bool = False) -> Set[str]:
        """Return the domain of the iterator"""
        return self._source.variables(include_values=include_values)

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings)

    async def next(self, context: Dict[str, Any] = dict()) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if self._produced < self._limit:
            mappings = await self._source.next(context=context)
            if mappings is None:
                return None
            self._produced += 1
            return mappings
        return None

    def save(self) -> SavedLimitIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_limit = SavedLimitIterator()
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_limit, source_field).CopyFrom(self._source.save())
        saved_limit.limit = self._limit
        saved_limit.produced = self._produced
        return saved_limit
