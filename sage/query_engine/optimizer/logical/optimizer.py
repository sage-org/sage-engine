from __future__ import annotations
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, Node
from sage.query_engine.optimizer.logical.visitors.filter_splitter import FilterSplitter


class LogicalPlanOptimizer():

    def __init__(self):
        self._visitors = []

    @staticmethod
    def get_default() -> LogicalPlanOptimizer:
        optimizer = LogicalPlanOptimizer()
        optimizer.add_visitor(FilterSplitter())
        return optimizer

    def add_visitor(self, visitor: LogicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(self, logical_plan: Node) -> Node:
        for visitor in self._visitors:
            logical_plan = visitor.visit(logical_plan)
        return logical_plan
