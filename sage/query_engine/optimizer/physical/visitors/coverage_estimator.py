from typing import Dict, Any, Union, List

from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.scan import ScanIterator


class CoverageEstimartor(PhysicalPlanVisitor):

    def visit(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        context.setdefault('progressions', [])
        return super().visit(node, context=context)

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> List[Union[ScanIterator, ValuesIterator]]:
        return self.visit(node._source, context=context)

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> List[Union[ScanIterator, ValuesIterator]]:
        return self.visit(node._source, context=context)

    def visit_join(
        self, node: IndexJoinIterator, context: Dict[str, Any] = {}
    ) -> List[Union[ScanIterator, ValuesIterator]]:
        left = self.visit(node._left, context=context)
        right = self.visit(node._right, context=context)
        return left + right

    def visit_union(
        self, node: BagUnionIterator, context: Dict[str, Any] = {}
    ) -> List[Union[ScanIterator, ValuesIterator]]:
        left = self.visit(node._left, context=context)
        right = self.visit(node._right, context=context)
        return (left + right) / 2

    def compute_progression_per_step(
        self, iterator: Union[ScanIterator, ValuesIterator]
    ) -> float:
        if iterator.serialized_name() == 'scan':
            cardinality = max(iterator._pattern_cardinality, iterator._produced)
            step = float(iterator._pattern_cardinality) / float(iterator._cardinality)
        else:
            cardinality = max(iterator._cardinality, iterator._produced)
            step = 1.0
        return step / cardinality

    def compute_coverage(
        self, node: Union[ScanIterator, ValuesIterator],
        context: Dict[str, Any] = {}
    ) -> float:
        if node._produced == 0:
            return 0.0
        progression_per_step = self.compute_progression_per_step(node)
        coverage = (node._produced - 1) * progression_per_step
        for previous_table_progression in context['progressions']:
            coverage *= previous_table_progression
        context['progressions'].append(progression_per_step)
        return coverage

    def visit_values(
        self, node: ValuesIterator, context: Dict[str, Any] = {}
    ) -> List[Union[ScanIterator, ValuesIterator]]:
        return self.compute_coverage(node, context=context)

    def visit_scan(
        self, node: ScanIterator, context: Dict[str, Any] = {}
    ) -> float:
        return self.compute_coverage(node, context=context)
