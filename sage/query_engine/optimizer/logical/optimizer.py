from __future__ import annotations
from typing import Dict, Any

from sage.database.core.dataset import Dataset
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, Node
from sage.query_engine.optimizer.logical.visitors.plan_rewriter import PlanRewriter


class LogicalPlanOptimizer():

    def __init__(self):
        self._visitors = [PlanRewriter()]

    @staticmethod
    def get_default(dataset: Dataset) -> LogicalPlanOptimizer:
        return LogicalPlanOptimizer()

    def add_visitor(self, visitor: LogicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(self, logical_plan: Node, context: Dict[str, Any] = {}) -> Node:
        for visitor in self._visitors:
            logical_plan = visitor.visit(logical_plan, context=context)
        return logical_plan
