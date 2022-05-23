from __future__ import annotations
from typing import Dict, Any

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.optimizer.physical.visitors.filter_push_down import FilterPushDown
from sage.query_engine.optimizer.physical.visitors.values_push_down import ValuesPushDown


class PhysicalPlanOptimizer():

    def __init__(self):
        self._visitors = []

    @staticmethod
    def get_default(dataset: Dataset) -> PhysicalPlanOptimizer:
        optimizer = PhysicalPlanOptimizer()
        if dataset.do_filter_push_down:
            optimizer.add_visitor(FilterPushDown())
        if dataset.do_values_push_down:
            optimizer.add_visitor(ValuesPushDown())
        return optimizer

    def add_visitor(self, visitor: PhysicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(
        self, physical_plan: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        for visitor in self._visitors:
            physical_plan = visitor.visit(physical_plan, context=context)
        return physical_plan
