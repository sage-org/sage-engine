from typing import List
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.plugins.sparql.algebra import Filter
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor


class FilterSplitter(LogicalPlanVisitor):

    def __init__(self):
        super().__init__()

    def visit_select_query(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        return node

    def visit_projection(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        return node

    def visit_to_multiset(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        return node

    def visit_values(self, node: CompValue) -> CompValue:
        return node

    def visit_filter(self, node: CompValue) -> CompValue:
        filter_clauses = self.visit(node.expr)
        node.expr = filter_clauses.pop()
        while len(filter_clauses) > 0:
            node.p = Filter(filter_clauses.pop(), node.p)
        node.p = self.visit(node.p)
        return node

    def visit_join(self, node: CompValue) -> CompValue:
        node.p1 = self.visit(node.p1)
        node.p2 = self.visit(node.p2)
        return node

    def visit_union(self, node: CompValue) -> CompValue:
        node.p1 = self.visit(node.p1)
        node.p2 = self.visit(node.p2)
        return node

    def visit_bgp(self, node: CompValue) -> CompValue:
        return node

    def visit_expression(self, node: Expr) -> List[Expr]:
        if node.name == 'ConditionalAndExpression':
            return self.visit_conditional_and_expression(node)
        else:
            return [node]

    def visit_conditional_and_expression(self, node: Expr) -> List[Expr]:
        operands = self.visit(node.expr)
        for other in node.other:
            operands.extend(self.visit(other))
        return operands
