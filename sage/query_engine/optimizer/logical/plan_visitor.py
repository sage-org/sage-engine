from abc import ABC
from typing import Any, Union, Tuple
from rdflib.term import BNode, Literal, URIRef, Variable
from rdflib.plugins.sparql.parserutils import CompValue, Expr

from sage.query_engine.exceptions import UnsupportedSPARQL

RDFTerm = Union[BNode, Literal, URIRef, Variable]
TriplePattern = Tuple[RDFTerm, RDFTerm, RDFTerm]
Node = Union[CompValue, Expr, RDFTerm, TriplePattern]


class LogicalPlanVisitor(ABC):

    def __init__(self):
        pass

    def __is_expression__(self, node: Node) -> bool:
        return isinstance(node, Expr)

    def __is_operator__(self, node: Node) -> bool:
        return isinstance(node, CompValue)

    def __is_triple_pattern__(self, node: Node) -> bool:
        return isinstance(node, Tuple)

    def __is_rdf_term(self, node: Node) -> bool:
        return (
            isinstance(node, Variable) or
            isinstance(node, Literal) or
            isinstance(node, URIRef) or
            isinstance(node, BNode)
        )

    def visit(self, node: Node) -> Any:
        if self.__is_expression__(node):
            return self.visit_expression(node)
        elif self.__is_operator__(node):
            return self.visit_operator(node)
        elif self.__is_triple_pattern__(node):
            return self.visit_scan(node)
        elif self.__is_rdf_term(node):
            return self.visit_rdfterm(node)
        else:
            raise UnsupportedSPARQL(f'Unsupported SPARQL feature: {type(node)}')

    def visit_operator(self, node: CompValue) -> Any:
        if node.name == 'SelectQuery':
            return self.visit_select_query(node)
        elif node.name == 'Project':
            return self.visit_projection(node)
        elif node.name == 'ToMultiSet':
            return self.visit_to_multiset(node)
        elif node.name == 'Filter':
            return self.visit_filter(node)
        elif node.name == 'Join':
            return self.visit_join(node)
        elif node.name == 'Union':
            return self.visit_union(node)
        elif node.name == 'BGP':
            return self.visit_bgp(node)
        elif node.name == 'values':
            return self.visit_values(node)
        elif node.name == 'InsertData':
            return self.visit_insert(node)
        elif node.name == 'DeleteData':
            return self.visit_delete(node)
        elif node.name == 'Modify':
            return self.visit_modify(node)
        else:
            raise UnsupportedSPARQL(f'Unsupported SPARQL feature: {node.name}')

    def visit_expression(self, node: Expr) -> Any:
        if node.name == 'ConditionalAndExpression':
            return self.visit_conditional_and_expression(node)
        elif node.name == 'ConditionalOrExpression':
            return self.visit_conditional_or_expression(node)
        elif node.name == 'RelationalExpression':
            return self.visit_relational_expression(node)
        elif node.name == 'AdditiveExpression':
            return self.visit_additive_expression(node)
        elif node.name == 'Builtin_REGEX':
            return self.visit_regex_expression(node)
        elif node.name == 'Builtin_NOTEXISTS':
            return self.visit_not_exists_expression(node)
        elif node.name == 'Builtin_STR':
            return self.visit_str_expression(node)
        elif node.name == 'UnaryNot':
            return self.visit_unary_not_expression(node)
        else:
            raise UnsupportedSPARQL(f'Unsupported SPARQL feature: {node.name}')

    def visit_select_query(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_projection(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_to_multiset(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_values(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_filter(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_join(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_union(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_bgp(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_scan(self, node: TriplePattern) -> Any:
        raise UnsupportedSPARQL('The Scan operator is not implemented')

    def visit_rdfterm(self, node: RDFTerm) -> Any:
        raise UnsupportedSPARQL('The rdf2python operator is not implemented')

    def visit_insert(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_delete(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_modify(self, node: CompValue) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} operator is not implemented')

    def visit_conditional_and_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_conditional_or_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_relational_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_additive_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_regex_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_not_exists_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_str_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')

    def visit_unary_not_expression(self, node: Expr) -> Any:
        raise UnsupportedSPARQL(f'The {node.name} expressions are not implemented')
