from typing import List, Optional, Dict, Set, Any
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedValuesIterator, SolutionMapping
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class ValuesIterator(PreemptableIterator):

    def __init__(
        self, values: List[Dict[str, str]],
        current_mappings: Optional[Dict[str, str]] = None,
        next_value: int = 0,
        produced: int = 0
    ):
        self._values = values
        self._current_mappings = current_mappings
        self._next_value = next_value
        self._cardinality = len(values)
        self._produced = produced
        self._coverage = 0.0
        self._cost = 0.0

    def __repr__(self) -> str:
        return f"<ValuesIterator ({self.variables()})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "values"

    def explain(self, height: int = 0, step: int = 3) -> None:
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}ValuesIterator (cost={self._cost}) (coverage={self._coverage}) <{self.variables()}>')

    def variables(self, include_values: bool = True) -> Set[str]:
        return set(self._values[0].keys()) if include_values else set()

    # def cardinality(self, instantiated: bool = False) -> int:
    #     return len(self._values)

    def next_stage(self, mappings: Dict[str, str], context: Dict[str, Any] = {}):
        self._current_mappings = mappings
        self._next_value = 0
        self._produced = 0

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        if self._next_value >= self._cardinality:
            return None
        mu = self._values[self._next_value]
        self._next_value += 1
        self._produced += 1
        if self._current_mappings is not None:
            mappings = {**self._current_mappings, **mu}
        else:
            mappings = mu
        return mappings

    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        context.setdefault('coverage__stop', False)
        context.setdefault('coverage__cardinalities', [])
        if self._produced == 0 or context['coverage__stop']:
            context['coverage__stop'] = True
            self._coverage = 0.0
            return 0.0
        cardinality = max(self._cardinality, self._produced)
        coverage = (self._produced - 1) / cardinality
        self._coverage = coverage
        for previous_table_cardinality in context['coverage__cardinalities']:
            coverage *= (1.0 / previous_table_cardinality)
        context['coverage__cardinalities'].append(cardinality)
        return coverage

    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        context.setdefault('cost__cout', 1)
        context['cost__cout'] *= self._cardinality
        return context['cost__cout']

    def save(self) -> SavedValuesIterator:
        saved_values = SavedValuesIterator()
        values = list()
        for value in self._values:
            solution_mapping = SolutionMapping()
            pyDict_to_protoDict(value, solution_mapping.bindings)
            values.append(solution_mapping)
        saved_values.values.extend(values)
        saved_values.next_value = self._next_value
        saved_values.produced = self._produced
        if self._current_mappings is not None:
            pyDict_to_protoDict(self._current_mappings, saved_values.muc)
        saved_values.coverage = self._coverage
        saved_values.cost = self._cost
        return saved_values
