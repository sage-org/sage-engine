from __future__ import annotations

from sage.query_engine.types import QueryContext
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.optimizer.physical.filter_push_down import FilterPushDown
from sage.query_engine.optimizer.physical.filter_to_values import FilterToValues


class PhysicalPlanOptimizer():

    def __init__(self) -> None:
        self._visitors = []

    @staticmethod
    def get_default() -> PhysicalPlanOptimizer:
        optimizer = PhysicalPlanOptimizer()
        optimizer.add_visitor(FilterPushDown())
        optimizer.add_visitor(FilterToValues())
        return optimizer

    def add_visitor(self, visitor: PhysicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(
        self, physical_plan: PreemptableIterator, context: QueryContext = {}
    ) -> PreemptableIterator:
        for visitor in self._visitors:
            physical_plan = visitor.visit(physical_plan, context=context)
        return physical_plan
