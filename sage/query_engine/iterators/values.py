
from typing import List, Optional, Dict, Set
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedValuesIterator, SolutionMapping
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class ValuesIterator(PreemptableIterator):

    def __init__(
        self, values: List[str], next_value: int = 0, produced: int = 0,
        runtime_cardinality: Optional[int] = None
    ):
        self._values = values
        self._next_value = next_value
        self._produced = produced
        if runtime_cardinality is None:
            self._runtime_cardinality = len(self._values)
        else:
            self._runtime_cardinality = runtime_cardinality

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"<ValuesIterator ({self._values})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "values"

    def explain(self, height: int = 0, step: int = 3) -> None:
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}ValuesIterator <{self._values}>')

    def variables(self) -> Set[str]:
        return set(self._values[0].keys())

    def next_stage(self, mappings: Dict[str, str]):
        self._next_value = 0
        self._runtime_cardinality += len(self._values)

    async def next(self) -> Optional[Dict[str, str]]:
        if self._next_value >= len(self._values):
            return None
        else:
            value = self._values[self._next_value]
            self._next_value += 1
            self._produced += 1
            return value

    def save(self) -> SavedValuesIterator:
        saved_iterator = SavedValuesIterator()
        values = list()
        for value in self._values:
            solution_mapping = SolutionMapping()
            pyDict_to_protoDict(value, solution_mapping.bindings)
            values.append(solution_mapping)
        saved_iterator.values.extend(values)
        saved_iterator.next_value = self._next_value
        saved_iterator.produced = self._produced
        saved_iterator.runtime_cardinality = self._runtime_cardinality
        return saved_iterator
