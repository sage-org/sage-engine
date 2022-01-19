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

SOURCE = 0
LEFT = 1
RIGHT = 2


class ValuesTargets(PhysicalPlanVisitor):

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._source, context=context)
        if len(targets) > 0:
            return targets
        # cannot be moved elsewhere (not really efficient, can be improved...)
        return [(node, SOURCE)]

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator's children
        targets = self.visit(node._source, context=context)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._source.variables(include_values=False).issuperset(context['variables']):
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
        if node._left.variables(include_values=False).issuperset(context['variables']):
            return [(node, LEFT)]
        if node._right.variables(include_values=False).issuperset(context['variables']):
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
        if node._left.variables(include_values=False).issuperset(context['variables']):
            targets.append((node, LEFT))
        if node._right.variables(include_values=False).issuperset(context['variables']):
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


class ValuesPushDown(PhysicalPlanVisitor):

    def __init__(self):
        super().__init__()
        self._moved = dict()

    def __has_already_been_moved__(self, values: ValuesIterator) -> bool:
        key = md5(''.join(values.variables()).encode()).hexdigest()
        if key not in self._moved:
            self._moved[key] = None
            return False
        else:
            return True

    def __push_values__(
        self, values: ValuesIterator,
        targets: List[Tuple[PreemptableIterator, int]]
    ) -> bool:
        if self.__has_already_been_moved__(values):
            return False
        for (iterator, position) in targets:
            if position == SOURCE:
                iterator._source = IndexJoinIterator(
                    ValuesIterator(values._values), iterator._source
                )
            elif position == LEFT:
                iterator._left = IndexJoinIterator(
                    ValuesIterator(values._values), iterator._left
                )
            elif position == RIGHT:
                iterator._right = IndexJoinIterator(
                    ValuesIterator(values._values), iterator._right
                )
            else:
                message = f'Unexpected relative position {position}'
                raise Exception(f'ValuesPushDownError: {message}')
        return True

    def __process_unary_iterator__(
        self, node: Union[ProjectionIterator, FilterIterator],
        context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._source = self.visit(node._source, context=context)
        if node._source.serialized_name() == 'join':
            if node._source._left.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._source._left.variables()}
                )
                if self.__push_values__(node._source._left, targets):
                    node._source = node._source._right
            elif node._source._right.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._source._right.variables()}
                )
                if self.__push_values__(node._source._right, targets):
                    node._source = node._source._left
        return node

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        context['root'] = node
        return self.__process_unary_iterator__(node, context=context)

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return self.__process_unary_iterator__(node, context=context)

    def __process_binary_iterator__(
        self, node: Union[IndexJoinIterator, BagUnionIterator],
        context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        node._left = self.visit(node._left, context=context)
        if node._left.serialized_name() == 'join':
            if node._left._left.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._left._left.variables()}
                )
                if self.__push_values__(node._left._left, targets):
                    node._left = node._left._right
            elif node._left._right.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._left._right.variables()}
                )
                if self.__push_values__(node._left._right, targets):
                    node._left = node._left._left
        node._right = self.visit(node._right, context=context)
        if node._right.serialized_name() == 'join':
            if node._right._left.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._right._left.variables()}
                )
                if self.__push_values__(node._right._left, targets):
                    node._right = node._right._right
            elif node._right._right.serialized_name() == 'values':
                targets = ValuesTargets().visit(
                    context['root'],
                    {'variables': node._right._right.variables()}
                )
                if self.__push_values__(node._right._right, targets):
                    node._right = node._right._left
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
