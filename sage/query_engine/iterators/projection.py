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
        self._coverage = 0.0
        self._cost = 0.0

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
        print(f'{prefix}ProjectionIterator (cost={self._cost}) (coverage={self._coverage}) <{self._projection}>')
        self._source.explain(height=(height + step), step=step)

    def variables(self, include_values: bool = False) -> Set[str]:
        if self._projection is None:
            return self._source.variables(include_values=include_values)
        else:
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

    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        self._coverage = self._source.update_coverage(context=context)
        return self._coverage

    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        self._cost = self._source.update_cost(context=context)
        return self._cost

    def save(self) -> SavedProjectionIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_proj = SavedProjectionIterator()
        saved_proj.values.extend(self._projection)
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_proj, source_field).CopyFrom(self._source.save())
        saved_proj.coverage = self._coverage
        saved_proj.cost = self._cost
        return saved_proj
