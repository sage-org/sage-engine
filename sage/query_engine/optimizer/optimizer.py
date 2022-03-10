from __future__ import annotations
from datetime import datetime
from typing import Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.optimizer.logical.plan_visitor import Node
from sage.query_engine.optimizer.logical.visitors.pipeline_builder import PipelineBuilder
from sage.query_engine.optimizer.logical.optimizer import LogicalPlanOptimizer
from sage.query_engine.optimizer.physical.optimizer import PhysicalPlanOptimizer
from sage.query_engine.optimizer.physical.visitors.cost_estimator import CostEstimartor
from sage.query_engine.optimizer.physical.visitors.cardinality_estimator import CardinalityEstimartor
from sage.query_engine.optimizer.physical.visitors.coverage_estimator import CoverageEstimartor


class Optimizer():

    def __init__(self):
        self._logical_optimizer = None
        self._physical_optimizer = None

    @staticmethod
    def get_default(dataset: Dataset) -> Optimizer:
        optimizer = Optimizer()
        optimizer.set_logical_optimizer(LogicalPlanOptimizer.get_default(dataset))
        optimizer.set_physical_optimizer(PhysicalPlanOptimizer.get_default(dataset))
        return optimizer

    def set_logical_optimizer(self, optimizer: LogicalPlanOptimizer) -> None:
        self._logical_optimizer = optimizer

    def set_physical_optimizer(self, optimizer: PhysicalPlanOptimizer) -> None:
        self._physical_optimizer = optimizer

    def optimize(
        self, logical_plan: Node, dataset: Dataset, default_graph: str,
        as_of: Optional[datetime] = None
    ) -> PreemptableIterator:
        if self._logical_optimizer is not None:
            logical_plan = self._logical_optimizer.optimize(logical_plan)
        physical_plan, cardinalities = PipelineBuilder(
            dataset, default_graph, as_of=as_of).visit(logical_plan)
        if self._physical_optimizer is not None:
            physical_plan = self._physical_optimizer.optimize(physical_plan)
        return physical_plan, cardinalities

    def cost(self, physical_plan: PreemptableIterator) -> float:
        return CostEstimartor().visit(physical_plan, context={})

    def cardinality(self, physical_plan: PreemptableIterator) -> float:
        return CardinalityEstimartor().visit(physical_plan, context={})

    def coverage(self, physical_plan: PreemptableIterator) -> float:
        return CoverageEstimartor().visit(physical_plan, context={})
