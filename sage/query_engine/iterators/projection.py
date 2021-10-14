# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Set, Any

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedProjectionIterator


class ProjectionIterator(PreemptableIterator):
    """A ProjectionIterator evaluates a SPARQL projection (SELECT) in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * projection: Projection variables.
    """

    def __init__(self, source: PreemptableIterator, projection: List[str] = None):
        super(ProjectionIterator, self).__init__()
        self._source = source
        self._projection = projection

    def __repr__(self) -> str:
        return f"<ProjectionIterator SELECT {self._projection} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "proj"

    def explain(self, height: int = 0, step: int = 3) -> None:
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}ProjectionIterator SELECT {self._projection}')
        self._source.explain(height=(height + step), step=step)

    def cost(self, context: Dict[str, float] = {}) -> float:
        """Return a cost estimation of the iterator"""
        return self._source.cost(context=context)

    def variables(self) -> Set[str]:
        return set(self._projection)

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings)

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        mappings = await self._source.next(context=context)
        if mappings is None:
            return None
        elif self._projection is None:
            return mappings
        return {k: v for k, v in mappings.items() if k in self._projection}

    def save(self) -> SavedProjectionIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_proj = SavedProjectionIterator()
        saved_proj.values.extend(self._projection)
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_proj, source_field).CopyFrom(self._source.save())
        return saved_proj
