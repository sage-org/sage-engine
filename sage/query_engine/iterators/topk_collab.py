from typing import Any, Dict, Optional, Set, List

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedTOPKCollabIterator

from sage.query_engine.exceptions import QuantumExhausted
from sage.query_engine.iterators.utils import eval_rdflib_expr


class TOPKCollabIterator(PreemptableIterator):
    """A TOPKCollabIterator evaluates a SPARQL TOP-K in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
    """

    def __init__(
        self, source: PreemptableIterator,
        order_conditions: str, compiled_order_conditions: List[Any],
        limit: int, threshold: Optional[Dict[str, str]] = None,
        threshold_refresh_rate: float = 1.0
    ):
        super(TOPKCollabIterator, self).__init__()
        self._source = source
        self._order_conditions = order_conditions
        self._compiled_order_conditions = compiled_order_conditions
        self._limit = limit
        self._threshold = threshold
        self._threshold_refresh_rate = threshold_refresh_rate
        self._produced = 0

        keys = []
        for index, order_condition in enumerate(self._compiled_order_conditions):
            if order_condition.order is None or order_condition.order == "ASC":
                order = "ASC"
            else:
                order = "DESC"
            keys.append((f"__order_condition_{index}", order))
        self._keys = keys

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "topkCollab"

    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}TOPKCollabIterator')
        self._source.explain(height=(height + step), step=step)

    def variables(self, include_values: bool = False) -> Set[str]:
        """Return the domain of the iterator"""
        return self._source.variables(include_values=include_values)

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings)

    def __greater_than_threshold__(self, mappings) -> bool:
        if self._threshold is None:
            return True
        for key, order in self._keys:
            if order == "DESC" and self._threshold[key] > mappings[key]:
                return False
            elif order == "ASC" and self._threshold[key] < mappings[key]:
                return False
        return True

    def __should_refresh_threshold__(self) -> bool:
        if self._threshold_refresh_rate == 0.0:
            return False
        elif self._threshold is None:
            return self._produced >= self._limit
        return self._produced >= self._limit * self._threshold_refresh_rate

    async def next(self, context: Dict[str, Any] = dict()) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        while True:
            if self.__should_refresh_threshold__():
                raise QuantumExhausted()
            mappings = await self._source.next(context=context)
            if mappings is None:
                return None
            for index, order_condition in enumerate(self._compiled_order_conditions):
                mappings[f"__order_condition_{index}"] = eval_rdflib_expr(
                    order_condition.expr, mappings)
            if self.__greater_than_threshold__(mappings):
                self._produced += 1
                return mappings

    def save(self) -> SavedTOPKCollabIterator:
        """Save and serialize the iterator as a Protobuf message"""

        saved_topk = SavedTOPKCollabIterator()
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_topk, source_field).CopyFrom(self._source.save())

        saved_topk.expression = self._order_conditions
        saved_topk.limit = self._limit
        saved_topk.threshold_refresh_rate = self._threshold_refresh_rate

        return saved_topk
