from __future__ import annotations

from sage.query_engine.types import RDFLibNode, QueryContext
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor
from sage.query_engine.optimizer.logical.plan_rewriter import PlanRewriter


class LogicalPlanOptimizer():

    def __init__(self) -> None:
        self._visitors = [PlanRewriter()]

    @staticmethod
    def get_default() -> LogicalPlanOptimizer:
        return LogicalPlanOptimizer()

    def add_visitor(self, visitor: LogicalPlanVisitor) -> None:
        self._visitors.append(visitor)

    def optimize(self, logical_plan: RDFLibNode, context: QueryContext = {}) -> RDFLibNode:
        for visitor in self._visitors:
            logical_plan = visitor.visit(logical_plan, context=context)
        return logical_plan
