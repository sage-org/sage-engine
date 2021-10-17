from abc import ABC
from typing import Any, Dict

from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


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
        else:
            raise UnsupportedSPARQL(f'Unsupported SPARQL iterator: {node.serialized_name()}')

    def visit_projection(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_values(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_filter(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_join(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_union(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')

    def visit_scan(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> Any:
        raise UnsupportedSPARQL(f'The {node.serialized_name()} iterator is not implemented')
