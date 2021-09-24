from __future__ import annotations
from typing import Dict, Any
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.optimizer.physical.visitors.filter_push_down import FilterPushDown


class PhysicalPlanOptimizer():

    def __init__(self):
        self._visitors = []

    @staticmethod
    def get_default(context: Dict[str, Any]) -> PhysicalPlanOptimizer:
        optimizer = PhysicalPlanOptimizer()
        optimizer.add_visitor(FilterPushDown(context))
        return optimizer

    def add_visitor(self, visitor: PhysicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(self, physical_plan: PreemptableIterator) -> PreemptableIterator:
        for visitor in self._visitors:
            physical_plan = visitor.visit(physical_plan)
        return physical_plan
