import sage.query_engine.optimizer.utils as utils

from typing import List, Set, Dict, Any
from rdflib.term import Variable
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.plugins.sparql.algebra import Filter

from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, RDFTerm


class ExpressionStringifier(LogicalPlanVisitor):

    def __init__(self):
        super().__init__()

    def visit_rdfterm(self, node: RDFTerm, context: Dict[str, Any] = {}) -> str:
        return utils.format_term(node)

    def visit_conditional_and_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        expression = self.visit(node.expr, context=context)
        for other in node.other:
            expression = f'({expression} && {self.visit(other, context=context)})'
        return expression

    def visit_conditional_or_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        expression = self.visit(node.expr, context=context)
        for other in node.other:
            expression = f'({expression} || {self.visit(other, context=context)})'
        return expression

    def visit_regex_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        return f'regex({self.visit(node.text, context=context)}, {self.visit(node.pattern, context=context)})'

    def visit_relational_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        return f'({self.visit(node.expr, context=context)} {node.op} {self.visit(node.other, context=context)})'

    def visit_unary_not_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        return f'!({self.visit(node.expr, context=context)})'

    def visit_str_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        return f'STR({self.visit(node.arg, context=context)})'

    def visit_additive_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        expression = self.visit(node.expr, context=context)
        for index, operator in enumerate(node.op):
            expression += f' {operator} {self.visit(node.other[index], context=context)}'
        return f'({expression})'

    def visit_concat_expression(self, node: Expr, context: Dict[str, Any] = {}) -> str:
        return f'CONCAT({self.visit(node.arg, context=context)})'


class FilterVariablesExtractor(LogicalPlanVisitor):

    def visit_rdfterm(self, node: RDFTerm, context: Dict[str, Any] = {}) -> Set[str]:
        if isinstance(node, Variable):
            return set([node.n3()])
        else:
            return set()

    def visit_conditional_and_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        variables = self.visit(node.expr, context=context)
        for other in node.other:
            variables.update(self.visit(other, context=context))
        return variables

    def visit_conditional_or_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        variables = self.visit(node.expr, context=context)
        for other in node.other:
            variables.update(self.visit(other, context=context))
        return variables

    def visit_relational_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.expr, context=context)

    def visit_additive_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        variables = self.visit(node.expr, context=context)
        for other in node.other:
            variables.update(self.visit(other, context=context))
        return variables

    def visit_regex_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.text, context=context)

    def visit_not_exists_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.expr, context=context)

    def visit_str_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.arg, context=context)

    def visit_unary_not_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.expr, context=context)

    def visit_concat_expression(self, node: Expr, context: Dict[str, Any] = {}) -> Set[str]:
        return self.visit(node.arg, context=context)


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

    def visit_select_query(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.p = self.visit(node.p, context=context)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_limit_k(self, node: CompValue, context: Dict[str, Any]) -> CompValue:
        node.p = self.visit(node.p, context=context)
        return node

    def visit_orderby(self, node: CompValue, context: Dict[str, Any]) -> Any:
        for orderCond in node.expr:
            orderCond.repr = ExpressionStringifier().visit(orderCond.expr)
        node.p = self.visit(node.p, context=context)
        return node

    def visit_projection(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.p = self.visit(node.p, context=context)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_to_multiset(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.p = self.visit(node.p, context=context)
        return node

    def visit_values(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        return node

    def visit_filter(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        sub_expressions = self.visit(node.expr, context=context)
        node.expr = sub_expressions.pop()
        node.expr.vars = FilterVariablesExtractor().visit(node.expr)
        node.expr.repr = ExpressionStringifier().visit(node.expr)
        while len(sub_expressions) > 0:
            node.p = Filter(sub_expressions.pop(), node.p)
            node.p.expr.vars = FilterVariablesExtractor().visit(node.p.expr)
            node.p.expr.repr = ExpressionStringifier().visit(node.p.expr)
        node.p = self.visit(node.p, context=context)
        if node.p.name == 'Join':
            node.p = self.merge_bgps(node.p)
        return node

    def visit_join(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.p1 = self.visit(node.p1, context=context)
        if node.p1.name == 'Join':
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2, context=context)
        if node.p2.name == 'Join':
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_union(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.p1 = self.visit(node.p1, context=context)
        if node.p1.name == 'Join':
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2, context=context)
        if node.p2.name == 'Join':
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_bgp(self, node: CompValue, context: Dict[str, Any] = {}) -> CompValue:
        node.mappings = []
        return node

    def visit_expression(self, node: Expr, context: Dict[str, Any] = {}) -> List[Expr]:
        if node.name == 'ConditionalAndExpression':
            return self.visit_conditional_and_expression(node, context=context)
        else:
            return [node]

    def visit_conditional_and_expression(self, node: Expr, context: Dict[str, Any] = {}) -> List[Expr]:
        operands = self.visit(node.expr, context=context)
        for other in node.other:
            operands.extend(self.visit(other, context=context))
        return operands
