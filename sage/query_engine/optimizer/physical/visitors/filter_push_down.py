from hashlib import md5
from typing import Any, Dict, Set, Tuple, List
from rdflib.term import Variable
from rdflib.plugins.sparql.parserutils import Expr
from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, RDFTerm
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.filter import FilterIterator

SOURCE = 0
LEFT = 1
RIGHT = 2


class FilterVariablesExtractor(LogicalPlanVisitor):

    def visit_rdfterm(self, node: RDFTerm) -> Set[str]:
        if isinstance(node, Variable):
            return set([node.n3()])
        else:
            return set()

    def visit_conditional_and_expression(self, node: Expr) -> Set[str]:
        variables = self.visit(node.expr)
        for other in node.other:
            variables.update(self.visit(other))
        return variables

    def visit_conditional_or_expression(self, node: Expr) -> Set[str]:
        variables = self.visit(node.expr)
        for other in node.other:
            variables.update(self.visit(other))
        return variables

    def visit_relational_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.expr)

    def visit_regex_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.text)

    def visit_not_exists_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.expr)

    def visit_str_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.arg)

    def visit_unary_not_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.expr)


class FilterTargets(PhysicalPlanVisitor):

    def __init__(self, filter: FilterIterator):
        super().__init__()
        if filter._expression.vars is None:
            variables = FilterVariablesExtractor().visit(filter._expression)
            filter._expression.vars = variables
        self._constrained_variables = filter._expression.vars

    def visit_projection(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator
        targets = self.visit(node._source)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._source.variables().issuperset(self._constrained_variables):
            return [(node, SOURCE)]
        # projection is the top iterator, something wrong...
        raise Exception('Malformed FILTER clause')

    def visit_filter(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator
        targets = self.visit(node._source)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._source.variables().issuperset(self._constrained_variables):
            return [(node, SOURCE)]
        # cannot be moved after this iterator
        return []

    def visit_join(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator
        targets = self.visit(node._left) + self.visit(node._right)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        if node._left.variables().issuperset(self._constrained_variables):
            return [(node, LEFT)]
        if node._right.variables().issuperset(self._constrained_variables):
            return [(node, RIGHT)]
        # cannot be moved after this iterator
        return []

    def visit_union(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        # can be moved at a lower level than the current iterator
        targets = self.visit(node._left) + self.visit(node._right)
        if len(targets) > 0:
            return targets
        # can be a child of the current iterator
        targets = []
        if node._left.variables().issuperset(self._constrained_variables):
            targets.append((node, LEFT))
        if node._right.variables().issuperset(self._constrained_variables):
            targets.append((node, RIGHT))
        if len(targets) > 0:
            return targets
        # cannot be moved after this iterator
        return targets

    def visit_values(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        return []  # cannot be moved after a leaf iterator

    def visit_scan(self, node: PreemptableIterator) -> List[Tuple[PreemptableIterator, int]]:
        return []  # cannot be moved after a leaf iterator


class FilterPushDown(PhysicalPlanVisitor):

    def __init__(self, context: Dict[str, Any]):
        super().__init__()
        self._context = context
        self._moved = dict()

    def __has_already_been_moved__(self, filter: PreemptableIterator) -> bool:
        key = md5(filter._raw_expression.encode()).hexdigest()
        if key not in self._moved:
            self._moved[key] = None
            return False
        else:
            return True

    def __push_filter__(self, filter, targets) -> bool:
        if self.__has_already_been_moved__(filter):
            return False
        for (iterator, position) in targets:
            if position == SOURCE:
                iterator._source = FilterIterator(
                    iterator._source, filter._raw_expression, filter._expression, self._context
                )
            elif position == LEFT:
                iterator._left = FilterIterator(
                    iterator._left, filter._raw_expression, filter._expression, self._context
                )
            elif position == RIGHT:
                iterator._right = FilterIterator(
                    iterator._right, filter._raw_expression, filter._expression, self._context
                )
            else:
                message = f'Unexpected relative position {position}'
                raise Exception(f'FilterPushDownError: {message}')
        return True

    def __process_unary_iterator__(self, node: PreemptableIterator) -> PreemptableIterator:
        updated = False
        if node._source.serialized_name() == 'filter':
            targets = FilterTargets(node._source).visit(node)
            updated = self.__push_filter__(node._source, targets)
        if updated:
            node._source = node._source._source
            return self.visit(node)
        node._source = self.visit(node._source)
        return node

    def visit_projection(self, node: PreemptableIterator) -> PreemptableIterator:
        return self.__process_unary_iterator__(node)

    def visit_filter(self, node: PreemptableIterator) -> PreemptableIterator:
        return self.__process_unary_iterator__(node)

    def __process_binary_iterator__(self, node: PreemptableIterator) -> PreemptableIterator:
        updated = False
        # remove the left filter if it has been moved
        if node._left.serialized_name() == 'filter':
            targets = FilterTargets(node._left).visit(node)
            updated = self.__push_filter__(node._left, targets)
        if updated:
            node._left = node._left._source
            return self.visit(node)
        # remove the right filter if it has been moved
        if node._right.serialized_name() == 'filter':
            targets = FilterTargets(node._right).visit(node)
            updated = self.__push_filter__(node._right, targets)
        if updated:
            node._right = node._right._source
            return self.visit(node)
        # continue the exploration of the tree
        node._left = self.visit(node._left)
        node._right = self.visit(node._right)
        return node

    def visit_join(self, node: PreemptableIterator) -> PreemptableIterator:
        return self.__process_binary_iterator__(node)

    def visit_union(self, node: PreemptableIterator) -> PreemptableIterator:
        return self.__process_binary_iterator__(node)

    def visit_values(self, node: PreemptableIterator) -> PreemptableIterator:
        return node

    def visit_scan(self, node: PreemptableIterator) -> PreemptableIterator:
        return node


# class FilterPushDown(PhysicalPlanVisitor):
#
#     def __init__(self, context: Dict[str, Any]):
#         super().__init__()
#         self._context = context
#
#     def __push_filter__(self, filter, targets) -> bool:
#         updated = False
#         for (iterator, position) in targets:
#             if iterator.variables() == filter.variables():
#                 continue
#             updated = True
#             if position == SOURCE:
#                 iterator._source = FilterIterator(
#                     iterator._source, filter._raw_expression, filter._expression, self._context
#                 )
#             elif position == LEFT:
#                 iterator._left = FilterIterator(
#                     iterator._left, filter._raw_expression, filter._expression, self._context
#                 )
#             elif position == RIGHT:
#                 iterator._right = FilterIterator(
#                     iterator._right, filter._raw_expression, filter._expression, self._context
#                 )
#             else:
#                 message = f'Unexpected relative position {position}'
#                 raise Exception(f'FilterPushDownError: {message}')
#         return updated
#
#     def __process_unary_iterator__(self, node: PreemptableIterator) -> PreemptableIterator:
#         updated = False
#         if node._source.serialized_name() == 'filter':
#             targets = FilterTargets(node._source).visit(node)
#             updated = self.__push_filter__(node._source, targets)
#         if updated:
#             node._source = node._source._source
#             return self.visit(node)
#         node._source = self.visit(node._source)
#         return node
#
#     def visit_projection(self, node: PreemptableIterator) -> PreemptableIterator:
#         return self.__process_unary_iterator__(node)
#
#     def visit_filter(self, node: PreemptableIterator) -> PreemptableIterator:
#         return self.__process_unary_iterator__(node)
#
#     def __process_binary_iterator__(self, node: PreemptableIterator) -> PreemptableIterator:
#         updated = False
#         # remove the left filter if it has been moved
#         if node._left.serialized_name() == 'filter':
#             targets = FilterTargets(node._left).visit(node)
#             updated = self.__push_filter__(node._left, targets)
#         if updated:
#             node._left = node._left._source
#             return self.visit(node)
#         # remove the right filter if it has been moved
#         if node._right.serialized_name() == 'filter':
#             targets = FilterTargets(node._right).visit(node)
#             updated = self.__push_filter__(node._right, targets)
#         if updated:
#             node._right = node._right._source
#             return self.visit(node)
#         # continue the exploration of the tree
#         node._left = self.visit(node._left)
#         node._right = self.visit(node._right)
#         return node
#
#     def visit_join(self, node: PreemptableIterator) -> PreemptableIterator:
#         return self.__process_binary_iterator__(node)
#
#     def visit_union(self, node: PreemptableIterator) -> PreemptableIterator:
#         return self.__process_binary_iterator__(node)
#
#     def visit_values(self, node: PreemptableIterator) -> PreemptableIterator:
#         return node
#
#     def visit_scan(self, node: PreemptableIterator) -> PreemptableIterator:
#         return node
