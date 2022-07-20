from __future__ import annotations

from sage.query_engine.types import RDFLibNode, QueryContext
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.optimizer.logical.pipeline_builder import PipelineBuilder
from sage.query_engine.optimizer.logical.optimizer import LogicalPlanOptimizer
from sage.query_engine.optimizer.physical.optimizer import PhysicalPlanOptimizer


class Optimizer():

    def __init__(self):
        self._logical_optimizer = None
        self._physical_optimizer = None

    @staticmethod
    def get_default() -> Optimizer:
        optimizer = Optimizer()
        optimizer.set_logical_optimizer(LogicalPlanOptimizer.get_default())
        optimizer.set_physical_optimizer(PhysicalPlanOptimizer.get_default())
        return optimizer

    def set_logical_optimizer(self, optimizer: LogicalPlanOptimizer) -> None:
        self._logical_optimizer = optimizer

    def set_physical_optimizer(self, optimizer: PhysicalPlanOptimizer) -> None:
        self._physical_optimizer = optimizer

    def optimize(
        self, logical_plan: RDFLibNode, context: QueryContext = {}
    ) -> PreemptableIterator:
        if self._logical_optimizer is not None:
            logical_plan = self._logical_optimizer.optimize(logical_plan, context=context)
        physical_plan = PipelineBuilder().visit(logical_plan, context=context)
        if self._physical_optimizer is not None:
            physical_plan = self._physical_optimizer.optimize(physical_plan, context=context)
        return physical_plan
