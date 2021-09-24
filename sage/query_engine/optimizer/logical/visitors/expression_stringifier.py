import sage.query_engine.optimizer.utils as utils

from rdflib.plugins.sparql.parserutils import Expr
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, RDFTerm


class ExpressionStringifier(LogicalPlanVisitor):

    def __init__(self):
        super().__init__()

    def visit_rdfterm(self, node: RDFTerm) -> str:
        return utils.format_term(node)

    def visit_conditional_and_expression(self, node: Expr) -> str:
        expression = self.visit(node.expr)
        for other in node.other:
            expression = f'({expression} && {self.visit(other)})'
        return expression

    def visit_conditional_or_expression(self, node: Expr) -> str:
        expression = self.visit(node.expr)
        for other in node.other:
            expression = f'({expression} || {self.visit(other)})'
        return expression

    def visit_regex_expression(self, node: Expr) -> str:
        return f'regex({self.visit(node.text)}, {self.visit(node.pattern)})'

    def visit_relational_expression(self, node: Expr) -> str:
        return f'({self.visit(node.expr)} {node.op} {self.visit(node.other)})'

    def visit_unary_not_expression(self, node: Expr) -> str:
        return f'!({self.visit(node.expr)})'

    def visit_str_expression(self, node: Expr) -> str:
        return f'str({self.visit(node.arg)})'
