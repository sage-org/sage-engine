# union.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional, Set, Any
from random import random

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBagUnionIterator


class BagUnionIterator(PreemptableIterator):
    """A BagUnionIterator performs a SPARQL UNION with bag semantics in a pipeline of iterators.

    This operator sequentially produces all solutions from the left operand,
    and then do the same for the right operand.

    Args:
      * left: left operand of the union.
      * right: right operand of the union.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__()
        self._left = left
        self._right = right
        self._coverage = 0.0
        self._cost = 0.0

    def __repr__(self):
        return f"<BagUnionIterator {self._left} UNION {self._right}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "union"

    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}BagUnionIterator (cost={self._cost}) (coverage={self._coverage})')
        self._left.explain(height=(height + step), step=step)
        self._right.explain(height=(height + step), step=step)

    def variables(self, include_values: bool = False) -> Set[str]:
        return self._left.variables(include_values=include_values).union(
            self._right.variables(include_values=include_values)
        )

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._left.next_stage(mappings)
        self._right.next_stage(mappings)

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        mappings = await self._left.next(context=context)
        if mappings is not None:
            return mappings
        return await self._right.next(context=context)

    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        self._coverage = self._left.update_coverage(context=context)
        self._coverage += self._right.update_coverage(context=context)
        self._coverage /= 2
        return self._coverage

    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        self._cost = self._left.update_cost(context=context)
        self._cost += self._right.update_cost(context=context)
        return self._cost

    def save(self) -> SavedBagUnionIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_union = SavedBagUnionIterator()
        # export left source
        left_field = f'{self._left.serialized_name()}_left'
        getattr(saved_union, left_field).CopyFrom(self._left.save())
        # export right source
        right_field = f'{self._right.serialized_name()}_right'
        getattr(saved_union, right_field).CopyFrom(self._right.save())
        saved_union.coverage = self._coverage
        saved_union.cost = self._cost
        return saved_union


class RandomBagUnionIterator(BagUnionIterator):
    """A RandomBagUnionIterator performs a SPARQL UNION with bag semantics in a pipeline of iterators.

    This operator randomly reads from the left and right operands to produce solution mappings.

    Args:
      * left: left operand of the union.
      * right: right operand of the union.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__(left, right)

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if random() < 0.5:
            left = self._left
            right = self._right
        else:
            left = self._right
            right = self._left
        mappings = await left.next(context=context)
        if mappings is not None:
            return mappings
        return await right.next(context=context)
