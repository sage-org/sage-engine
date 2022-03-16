from typing import List
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.plugins.sparql.algebra import Filter

from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor


class PlanRewriter(LogicalPlanVisitor):

    def __init__(self):
        super().__init__()

    def merge_bgps(self, node: CompValue) -> CompValue:
        if node.p1.name == 'BGP' or node.p1.name == 'ToMultiSet':
            if node.p2.name == 'BGP' or node.p2.name == 'ToMultiSet':
                values = {'triples': [], 'mappings': []}
                if node.p1.name == 'ToMultiSet':
                    values['mappings'].append(node.p1.p)
                else:
                    values['triples'].extend(node.p1.triples)
                    values['mappings'].extend(node.p1.mappings)
                if node.p2.name == 'ToMultiSet':
                    values['mappings'].append(node.p2.p)
                else:
                    values['triples'].extend(node.p2.triples)
                    values['mappings'].extend(node.p2.mappings)
                return CompValue('BGP', **values)
        return node

    def visit_select_query(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_projection(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_to_multiset(self, node: CompValue) -> CompValue:
        node.p = self.visit(node.p)
        return node

    def visit_values(self, node: CompValue) -> CompValue:
        return node

    def visit_filter(self, node: CompValue) -> CompValue:
        sub_expressions = self.visit(node.expr)
        node.expr = sub_expressions.pop()
        while len(sub_expressions) > 0:
            node.p = Filter(sub_expressions.pop(), node.p)
        node.p = self.visit(node.p)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_join(self, node: CompValue) -> CompValue:
        node.p1 = self.visit(node.p1)
        if node.p1.name == 'Join':
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2)
        if node.p2.name == 'Join':
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_union(self, node: CompValue) -> CompValue:
        node.p1 = self.visit(node.p1)
        if node.p1.name == 'Join':
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2)
        if node.p2.name == 'Join':
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_bgp(self, node: CompValue) -> CompValue:
        node.mappings = []
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
