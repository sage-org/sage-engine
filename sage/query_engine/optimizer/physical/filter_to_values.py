from typing import List, Dict, Any
from rdflib.term import Literal, URIRef, Variable
from rdflib.plugins.sparql.parserutils import Expr

from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
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


class FilterToValues(PhysicalPlanVisitor):
    """
    This class implements an heuristic that consists in transforming FILTER to
    VALUES clauses.
    """

    def __transform_relational_expression__(
        self, expression: Expr
    ) -> List[Dict[str, str]]:
        if not isinstance(expression.expr, Variable):
            return []
        if not isinstance(expression.other, (Literal, URIRef)):
            return []
        if expression.op != "=":
            return []
        return [{expression.expr.n3(): expression.other.n3()}]

    def __transform_conditional_or_expression__(
        self, expression: Expr
    ) -> List[Dict[str, str]]:
        solutions = self.__transform_relational_expression__(expression.expr)
        for other in expression.other:
            if other.name == "RelationalExpression":
                solutions.extend(self.__transform_relational_expression__(other))
        if not all([mu.keys() == solutions[0].keys() for mu in solutions]):
            return []
        return solutions

    def __transform_filter_to_values__(self, node: FilterIterator) -> PreemptableIterator:
        solutions = []
        if node.expression.expression.name == "RelationalExpression":
            solutions = self.__transform_relational_expression__(node.expression.expression)
        elif node.expression.expression.name == "ConditionalOrExpression":
            solutions = self.__transform_conditional_or_expression__(node.expression.expression)
        if len(solutions) > 0:
            return IndexJoinIterator(ValuesIterator(solutions), node.source)
        return None

    def visit_projection(
        self, node: ProjectionIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.source.name == "filter" and node.source.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.source)
            if iterator is not None:
                node.source = iterator
        self.visit(node.source, context=context)
        return node

    def visit_values(
        self, node: ValuesIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return node

    def visit_filter(
        self, node: FilterIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.source.name == "filter" and node.source.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.source)
            if iterator is not None:
                node.source = iterator
        self.visit(node.source, context=context)
        return node

    def visit_rank_filter(
        self, node: RankFilterIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.source.name == "filter" and node.source.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.source)
            if iterator is not None:
                node.source = iterator
        self.visit(node.source, context=context)
        return node

    def visit_join(
        self, node: IndexJoinIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.left.name == "filter" and node.left.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.left)
            if iterator is not None:
                node.left = iterator
        self.visit(node.left, context=context)

        if node.right.name == "filter" and node.right.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.right)
            if iterator is not None:
                node.right = iterator
        self.visit(node.right, context=context)

        return node

    def visit_union(
        self, node: BagUnionIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.left.name == "filter" and node.left.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.left)
            if iterator is not None:
                node.left = iterator
        self.visit(node.left, context=context)

        if node.right.name == "filter" and node.right.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.right)
            if iterator is not None:
                node.right = iterator
        self.visit(node.right, context=context)

        return node

    def visit_scan(
        self, node: ScanIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        return node

    def visit_limit(
        self, node: LimitIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.source.name == "filter" and node.source.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.source)
            if iterator is not None:
                node.source = iterator
        self.visit(node.source, context=context)
        return node

    def visit_topk(
        self, node: TOPKIterator, context: Dict[str, Any] = {}
    ) -> PreemptableIterator:
        if node.source.name == "filter" and node.source.source.name == "scan":
            iterator = self.__transform_filter_to_values__(node.source)
            if iterator is not None:
                node.source = iterator
        self.visit(node.source, context=context)
        return node
