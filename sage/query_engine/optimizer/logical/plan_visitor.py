from abc import ABC
from typing import Tuple, Any

from sage.query_engine.types import (
    RDFLibTerm, RDFLibTriplePattern, RDFLibNode, RDFLibExpr, RDFLibOperator,
    QueryContext)
from sage.query_engine.exceptions import UnsupportedSPARQL


class LogicalPlanVisitor(ABC):

    def visit(self, node: RDFLibNode, context: QueryContext = {}) -> Any:
        if isinstance(node, RDFLibExpr):
            return self.visit_expression(node, context=context)
        elif isinstance(node, RDFLibOperator):
            return self.visit_operator(node, context=context)
        elif isinstance(node, Tuple):
            return self.visit_scan(node, context=context)
        elif isinstance(node, RDFLibTerm):
            return self.visit_rdfterm(node)
        raise UnsupportedSPARQL(f"Unsupported SPARQL feature: {node}")

    def visit_operator(self, node: RDFLibOperator, context: QueryContext = {}) -> Any:
        if node.name == "SelectQuery":
            return self.visit_select_query(node, context=context)
        elif node.name == "Slice":
            return self.visit_limit_k(node, context=context)
        elif node.name == "OrderBy":
            return self.visit_orderby(node, context=context)
        elif node.name == "Project":
            return self.visit_projection(node, context=context)
        elif node.name == "ToMultiSet":
            return self.visit_to_multiset(node, context=context)
        elif node.name == "Filter":
            return self.visit_filter(node, context=context)
        elif node.name == "Join":
            return self.visit_join(node, context=context)
        elif node.name == "Union":
            return self.visit_union(node, context=context)
        elif node.name == "BGP":
            return self.visit_bgp(node, context=context)
        elif node.name == "values":
            return self.visit_values(node, context=context)
        elif node.name == "InsertData":
            return self.visit_insert(node, context=context)
        elif node.name == "DeleteData":
            return self.visit_delete(node, context=context)
        elif node.name == "Modify":
            return self.visit_modify(node, context=context)
        raise UnsupportedSPARQL(f"Unsupported SPARQL feature: {node.name}")

    def visit_expression(self, node: RDFLibExpr, context: QueryContext = {}) -> Any:
        if node.name == "ConditionalAndExpression":
            return self.visit_conditional_and_expression(node, context=context)
        elif node.name == "ConditionalOrExpression":
            return self.visit_conditional_or_expression(node, context=context)
        elif node.name == "RelationalExpression":
            return self.visit_relational_expression(node, context=context)
        elif node.name == "AdditiveExpression":
            return self.visit_additive_expression(node, context=context)
        elif node.name == "Builtin_REGEX":
            return self.visit_regex_expression(node, context=context)
        elif node.name == "Builtin_NOTEXISTS":
            return self.visit_not_exists_expression(node, context=context)
        elif node.name == "Builtin_STR":
            return self.visit_str_expression(node, context=context)
        elif node.name == "UnaryNot":
            return self.visit_unary_not_expression(node, context=context)
        elif node.name == "Builtin_CONCAT":
            return self.visit_concat_expression(node, context=context)
        raise UnsupportedSPARQL(f"Unsupported SPARQL expression: {node.name}")

    def visit_select_query(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_limit_k(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_orderby(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not preemptable")

    def visit_projection(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_to_multiset(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_values(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_filter(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_join(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_union(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_bgp(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_scan(self, node: RDFLibTriplePattern, context: QueryContext) -> Any:
        raise UnsupportedSPARQL("The Scan operator is not implemented")

    def visit_rdfterm(self, node: RDFLibTerm, context: QueryContext) -> Any:
        raise UnsupportedSPARQL("The rdf2python operator is not implemented")

    def visit_insert(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_delete(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_modify(self, node: RDFLibOperator, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} operator is not implemented")

    def visit_conditional_and_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_conditional_or_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_relational_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_additive_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_regex_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_not_exists_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_str_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_unary_not_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")

    def visit_concat_expression(self, node: RDFLibExpr, context: QueryContext) -> Any:
        raise UnsupportedSPARQL(f"The {node.name} expressions are not implemented")
