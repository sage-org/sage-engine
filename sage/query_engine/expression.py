from __future__ import annotations

from typing import Union, Any, Set

from rdflib.term import URIRef, BNode, Variable, Literal
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.util import from_n3

from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.types import RDFLibExpr, RDFLibTerm, Mappings


class Expression():
    """
    This class is used to make the manipulation of RDFLib expressions easier.

    Parameters
    ----------
    expression: RDFLibExpr | RDFLibTerm
        An RDFLib expression or an RDFTerm.
    """

    def __init__(self, expression: Union[RDFLibExpr, RDFLibTerm]) -> None:
        self._expression = expression

    @property
    def expression(self) -> Union[RDFLibExpr, RDFLibTerm]:
        return self._expression

    @staticmethod
    def parse(expression: str) -> Expression:
        query = f"SELECT * WHERE {{ ?s ?p ?o . FILTER({expression}) }}"
        compiled_expression = translateQuery(parseQuery(query)).algebra.p.p.expr
        return Expression(compiled_expression)

    def __variables__(self, expr: Union[RDFLibExpr, RDFLibTerm]) -> Set[str]:
        if isinstance(expr, (URIRef, BNode, Literal)):
            return set()
        elif isinstance(expr, Variable):
            return set([expr.n3()])
        elif expr.name == "ConditionalAndExpression":
            variables = self.__variables__(expr.expr)
            for other in expr.other:
                variables.update(self.__variables__(other))
            return variables
        elif expr.name == "ConditionalOrExpression":
            variables = self.__variables__(expr.expr)
            for other in expr.other:
                variables.update(self.__variables__(other))
            return variables
        elif expr.name == "RelationalExpression":
            variables = self.__variables__(expr.expr)
            variables.update(self.__variables__(expr.other))
            return variables
        elif expr.name == "AdditiveExpression":
            variables = self.__variables__(expr.expr)
            for other in expr.other:
                variables.update(self.__variables__(other))
            return variables
        elif expr.name == "Builtin_REGEX":
            return self.__variables__(expr.text)
        elif expr.name == "Builtin_STR":
            return self.__variables__(expr.arg)
        elif expr.name == "Builtin_CONCAT":
            return self.__variables__(expr.arg)
        elif expr.name == "UnaryNot":
            return self.__variables__(expr.expr)
        elif expr.name == "Builtin_STRSTARTS":
            variables = self.__variables__(expr.arg1)
            return variables.union(self.__variables__(expr.arg2))
        elif expr.name == "Builtin_STRSENDS":
            return self.__variables__(expr.text)
        elif expr.name == "Builtin_LANG":
            return self.__variables__(expr.arg)
        raise UnsupportedSPARQL(f"Unsupported SPARQL expression: {expr.name}")

    def variables(self) -> Set[str]:
        """
        Returns variables that appear in the RDFLib expression.

        Returns
        -------
        Set[str]
            The variables that appear in the RDFLib expression.
        """
        return self.__variables__(self.expression)

    def __stringify__(self, expr: Union[RDFLibExpr, RDFLibTerm]) -> str:
        if isinstance(expr, URIRef):
            return str(expr)
        elif isinstance(expr, BNode):
            return f"?v_{str(expr)}"
        elif isinstance(expr, (Variable, Literal)):
            return expr.n3()
        elif expr.name == "ConditionalAndExpression":
            repr = self.__stringify__(expr.expr)
            for other in expr.other:
                repr = f"({repr} && {self.__stringify__(other)})"
            return repr
        elif expr.name == "ConditionalOrExpression":
            repr = self.__stringify__(expr.expr)
            for other in expr.other:
                repr = f"({repr} || {self.__stringify__(other)})"
            return repr
        elif expr.name == "RelationalExpression":
            left_operand = self.__stringify__(expr.expr)
            right_operand = self.__stringify__(expr.other)
            return f"({left_operand} {expr.op} {right_operand})"
        elif expr.name == "AdditiveExpression":
            repr = self.__stringify__(expr.expr)
            for index, operator in enumerate(expr.op):
                repr += f" {operator} {self.__stringify__(expr.other[index])}"
            return f"({repr})"
        elif expr.name == "Builtin_REGEX":
            operand = self.__stringify__(expr.text)
            pattern = self.__stringify__(expr.pattern)
            return f"regex({operand}, {pattern})"
        elif expr.name == "Builtin_STR":
            return f"STR({self.__stringify__(expr.arg)})"
        elif expr.name == "UnaryNot":
            return f"!({self.__stringify__(expr.expr)})"
        elif expr.name == "Builtin_CONCAT":
            return f"CONCAT({self.__stringify__(expr.arg)})"
        elif expr.name == "Builtin_STRSTARTS":
            arg1 = self.__stringify__(expr.arg1)
            arg2 = self.__stringify__(expr.arg2)
            return f"STRSTARTS({arg1}, {arg2})"
        elif expr.name == "Builtin_STRSENDS":
            arg1 = self.__stringify__(expr.text)
            arg2 = self.__stringify__(expr.pattern)
            return f"STRSTENDS({arg1}, {arg2})"
        elif expr.name == "Builtin_LANG":
            return f"LANG({self.__stringify__(expr.arg)})"
        raise UnsupportedSPARQL(f"Unsupported SPARQL expression: {expr.name}")

    def stringify(self) -> str:
        """
        Stringifies an RDFLib expression into a string. It is used to save the
        expression in a Protobuf message.

        Returns
        -------
        str
            The RDFLib expression as a string.
        """
        return self.__stringify__(self.expression)

    def eval(self, mappings: Mappings) -> Any:
        """
        Evaluates the RDFLib expression for a solution mappings.

        Parameters
        ----------
        Mappings
            A solution mappings.

        Returns
        -------
        Any
            The result of evaluating the RDFLib expression with the given
            mappings.
        """
        if isinstance(self.expression, Variable):
            return mappings[self.expression.n3()]
        rdflib_mappings = {}
        for key, value in mappings.items():
            if value.startswith("http"):
                rdflib_mappings[Variable(key[1:])] = URIRef(value)
            elif "\"^^http" in value:
                index = value.find("\"^^http")
                value = f"{value[0:index+3]}<{value[index+3:]}>"
                rdflib_mappings[Variable(key[1:])] = from_n3(value)
            else:
                rdflib_mappings[Variable(key[1:])] = from_n3(value)
        context = QueryContext(bindings=Bindings(d=rdflib_mappings))
        return self.expression.eval(context)
