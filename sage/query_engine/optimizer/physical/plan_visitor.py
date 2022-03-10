from abc import ABC
from typing import Any, Dict

from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.scan import ScanIterator


class PhysicalPlanVisitor(ABC):

    def __init__(self):
        pass

    def visit(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        if node.serialized_name() == 'proj':
            return self.visit_projection(node, context=context)
        elif node.serialized_name() == 'join':
            return self.visit_join(node, context=context)
        elif node.serialized_name() == 'union':
            return self.visit_union(node, context=context)
        elif node.serialized_name() == 'filter':
            return self.visit_filter(node, context=context)
        elif node.serialized_name() == 'values':
            return self.visit_values(node, context=context)
        elif node.serialized_name() == 'scan':
            return self.visit_scan(node, context=context)
        raise UnsupportedSPARQL(f'Unsupported SPARQL iterator: {node.serialized_name()}')

    def visit_projection(self, node: ProjectionIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_values(self, node: ValuesIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_filter(self, node: FilterIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_join(self, node: IndexJoinIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_union(self, node: BagUnionIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_scan(self, node: ScanIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')
