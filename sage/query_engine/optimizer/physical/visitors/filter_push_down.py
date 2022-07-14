from hashlib import md5
from typing import Tuple, List, Dict, Any, Union

from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk import TOPKIterator
from sage.query_engine.iterators.topk_collab import TOPKCollabIterator

SOURCE = 0
LEFT = 1
RIGHT = 2


class FilterTargets(PhysicalPlanVisitor):

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._source, context=context)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._source.variables().issuperset(context['variables']):
            return [(node, SOURCE)]
        # projection is the top iterator, something wrong...
        raise Exception('Malformed FILTER clause')

    def visit_limit(
        self, node: LimitIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        return self.visit(node._source, context=context)

    def visit_topk(
        self, node: Union[TOPKIterator, TOPKCollabIterator], context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        return self.visit(node._source, context=context)

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._source, context=context)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._source.variables().issuperset(context['variables']):
            return [(node, SOURCE)]
        # cannot be moved after this iterator
        return []

    def visit_join(
        self, node: IndexJoinIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._left, context=context)
        if len(targets) > 0:
            return targets
        targets = self.visit(node._right, context=context)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._left.variables().issuperset(context['variables']):
            return [(node, LEFT)]
        if node._right.variables().issuperset(context['variables']):
            return [(node, RIGHT)]
        # cannot be moved after this iterator
        return []

    def visit_union(
        self, node: BagUnionIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._left, context=context) + self.visit(node._right, context=context)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        targets = []
        if node._left.variables().issuperset(context['variables']):
            targets.append((node, LEFT))
        if node._right.variables().issuperset(context['variables']):
            targets.append((node, RIGHT))
        return targets

    def visit_values(
        self, node: ValuesIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        return []  # cannot be moved after a leaf iterator

    def visit_scan(
        self, node: ScanIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        return []  # cannot be moved after a leaf iterator


class FilterPushDown(PhysicalPlanVisitor):

    def __init__(self):
        super().__init__()
        self._moved = dict()

    def __has_already_been_moved__(self, filter: FilterIterator) -> bool:
        key = md5(filter._expression.encode()).hexdigest()
        if key not in self._moved:
            self._moved[key] = None
            return False
        return True

    def __push_filter__(
        self, filter: FilterIterator,
        targets: List[Tuple[PreemptableIterator, int]]
    ) -> bool:
        if self.__has_already_been_moved__(filter):
            return False
        for (iterator, position) in targets:
            if position == SOURCE:
                iterator._source = FilterIterator(
                    iterator._source, filter._expression, filter._constrained_variables, filter._compiled_expression)
            elif position == LEFT:
                iterator._left = FilterIterator(
                    iterator._left, filter._expression, filter._constrained_variables, filter._compiled_expression)
            elif position == RIGHT:
                iterator._right = FilterIterator(
                    iterator._right, filter._expression, filter._constrained_variables, filter._compiled_expression)
            else:
                message = f'Unexpected relative position {position}'
                raise Exception(f'FilterPushDownError: {message}')
        return True

    def __process_unary_iterator__(
        self, node: Union[ProjectionIterator, FilterIterator],
        context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._source = self.visit(node._source, context=context)
        if node._source.serialized_name() == 'filter':
            targets = FilterTargets().visit(
                context['root'],
                {'variables': node._source.constrained_variables()})
            if self.__push_filter__(node._source, targets):
                node._source = node._source._source
        return node

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        context['root'] = node
        return self.__process_unary_iterator__(node, context=context)

    def visit_limit(
        self, node: LimitIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._source = self.visit(node._source, context=context)
        return node

    def visit_topk(
        self, node: Union[TOPKIterator, TOPKCollabIterator], context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._source = self.visit(node._source, context=context)
        return node

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return self.__process_unary_iterator__(node, context=context)

    def __process_binary_iterator__(
        self, node: Union[IndexJoinIterator, BagUnionIterator],
        context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._left = self.visit(node._left, context=context)
        if node._left.serialized_name() == 'filter':
            targets = FilterTargets().visit(
                context['root'],
                {'variables': node._left.constrained_variables()})
            if self.__push_filter__(node._left, targets):
                node._left = node._left._source
        node._right = self.visit(node._right, context=context)
        if node._right.serialized_name() == 'filter':
            targets = FilterTargets().visit(
                context['root'],
                {'variables': node._right.constrained_variables()})
            if self.__push_filter__(node._right, targets):
                node._right = node._right._source
        return node

    def visit_join(
        self, node: IndexJoinIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return self.__process_binary_iterator__(node, context=context)

    def visit_union(
        self, node: BagUnionIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return self.__process_binary_iterator__(node, context=context)

    def visit_values(
        self, node: ValuesIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return node

    def visit_scan(
        self, node: ScanIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return node
