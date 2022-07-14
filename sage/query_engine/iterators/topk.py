from typing import Any, Dict, Optional, Set, List

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedTOPKIterator, SolutionMapping
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

from sage.query_engine.iterators.topk_struct import TOPKStruct
from sage.query_engine.iterators.utils import eval_rdflib_expr


class TOPKIterator(PreemptableIterator):
    """A TOPKIterator evaluates a SPARQL TOP-K in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
    """

    def __init__(
        self, source: PreemptableIterator,
        order_conditions: str, compiled_order_conditions: List[Any],
        limit: int, topk: List[Dict[str, str]] = []
    ):
        super(TOPKIterator, self).__init__()
        self._source = source
        self._order_conditions = order_conditions
        self._compiled_order_conditions = compiled_order_conditions
        self._limit = limit

        keys = []
        for index, order_condition in enumerate(self._compiled_order_conditions):
            if order_condition.order is None or order_condition.order == "ASC":
                order = "ASC"
            else:
                order = "DESC"
            keys.append((f"?order_condition_{index}", order))
        topk_struct = TOPKStruct(keys, limit=limit)
        for mappings in topk:
            topk_struct.insert(mappings)
        self._topk = topk_struct

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "topk"

    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}TOPKIterator (k={self._limit})')
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
        mappings = await self._source.next(context=context)
        while mappings is not None:
            for index, order_condition in enumerate(self._compiled_order_conditions):
                mappings[f"?order_condition_{index}"] = eval_rdflib_expr(
                    order_condition.expr, mappings)
            self._topk.insert(mappings)
            mappings = await self._source.next(context=context)
        while len(self._topk) > 0:
            return self._topk.pop()
        return None

    def save(self) -> SavedTOPKIterator:
        """Save and serialize the iterator as a Protobuf message"""

        saved_topk = SavedTOPKIterator()
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_topk, source_field).CopyFrom(self._source.save())

        saved_topk.expression = self._order_conditions
        saved_topk.limit = self._limit

        topk = list()
        for solution in self._topk.flatten():
            saved_solution = SolutionMapping()
            pyDict_to_protoDict(solution, saved_solution.bindings)
            topk.append(saved_solution)
        saved_topk.topk.extend(topk)

        return saved_topk
