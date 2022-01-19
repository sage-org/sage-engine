from typing import Set
from rdflib.term import Variable
from rdflib.plugins.sparql.parserutils import Expr

from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, RDFTerm


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

    def visit_additive_expression(self, node: Expr) -> Set[str]:
        variables = self.visit(node.expr)
        for other in node.other:
            variables.update(self.visit(other))
        return variables

    def visit_regex_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.text)

    def visit_not_exists_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.expr)

    def visit_str_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.arg)

    def visit_unary_not_expression(self, node: Expr) -> Set[str]:
        return self.visit(node.expr)
