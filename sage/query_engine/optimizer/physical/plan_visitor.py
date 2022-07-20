from abc import ABC
from typing import Any

from sage.query_engine.types import QueryContext
from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk.topk import TOPKIterator
from sage.query_engine.iterators.topk.rank_filter import RankFilterIterator


class PhysicalPlanVisitor(ABC):

    def visit(self, node: PreemptableIterator, context: QueryContext = {}) -> Any:
        if node.name == "proj":
            return self.visit_projection(node, context=context)
        elif node.name == "join":
            return self.visit_join(node, context=context)
        elif node.name == "union":
            return self.visit_union(node, context=context)
        elif node.name == "filter":
            return self.visit_filter(node, context=context)
        elif node.name == "rank_filter":
            return self.visit_rank_filter(node, context=context)
        elif node.name == "values":
            return self.visit_values(node, context=context)
        elif node.name == "scan":
            return self.visit_scan(node, context=context)
        elif node.name == "limit":
            return self.visit_limit(node, context=context)
        elif node.name == "topk_server":
            return self.visit_topk(node, context=context)
        elif node.name == "partial_topk":
            return self.visit_topk(node, context=context)
        raise UnsupportedSPARQL(f"Unsupported SPARQL iterator: {node.name}")

    def visit_projection(self, node: ProjectionIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_values(self, node: ValuesIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_filter(self, node: FilterIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_rank_filter(self, node: RankFilterIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_join(self, node: IndexJoinIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_union(self, node: BagUnionIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_scan(self, node: ScanIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_limit(self, node: LimitIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")

    def visit_topk(self, node: TOPKIterator, context: QueryContext = {}) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} iterator is not implemented")
