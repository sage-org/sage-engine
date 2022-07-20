from __future__ import annotations

from typing import List, Any, Tuple, Set
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from sage.query_engine.expression import Expression
from sage.query_engine.types import Mappings


class OrderConditions():
    """
    This class is used to make the manipulation of RDFLib ORDER BY conditions
    easier.

    An `OrderConditions` corresponds to one or more RDFLib expressions with an
    associated order (ASC, DESC).

    Parameters
    ----------
    conditions: List[Tuple[Expression, str]]
        The conditions that appear in an ORDER BY clause formated as a list of
        2-tuple (`expression`, `order`) where:
            - `expression`: Expression - is an RDFLib expression
            - `order`: str ("ASC" or "DESC") - defines if solutions mappings
              need to be ordered from the smallest (resp. largest) to the
              largest (resp. smallest) solution for the given key.
    """

    def __init__(
        self, conditions: List[Tuple[Expression, str]]
    ) -> None:
        self._conditions = conditions

    @property
    def conditions(self) -> List[Tuple[Expression, str]]:
        return self._conditions

    def variables(self) -> Set[str]:
        """
        Returns variables that appear in the RDFLib ORDER BY conditions.

        Returns
        -------
        Set[str]
            The variables that appear in the RDFLib ORDER BY conditions.
        """
        variables = set()
        for expression, order in self.conditions:
            variables.update(expression.variables())
        return variables

    @staticmethod
    def from_rdflib(rdflib_conditions: List[Any]) -> OrderConditions:
        """
        Creates an instance of `OrderConditions` from an RDFLib OrderConditions
        object.

        Parameters
        ----------
        rdflib_conditions: List[Any]
            An RDFLib OrderConditions object.

        Returns
        -------
        OrderConditions
            An instance of `OrderConditions` created from the RDFLib
            OrderConditions object.
        """
        conditions = []
        for rdflib_condition in rdflib_conditions:
            expression = Expression(rdflib_condition.expr)
            order = "DESC" if rdflib_condition.order == "DESC" else "ASC"
            conditions.append((expression, order))
        return OrderConditions(conditions)

    @staticmethod
    def parse(conditions: str) -> OrderConditions:
        """
        Creates an instance of `OrderConditions` from its string representation.

        Parameters
        ----------
        conditions: List[Any]
            The string representation of an `OrderConditions`.

        Returns
        -------
        OrderConditions
            An instance of `OrderConditions` created from its string
            representation.
        """
        query = f"SELECT * WHERE {{ ?s ?p ?o }} ORDER BY {conditions}"
        compiled_conditions = translateQuery(parseQuery(query)).algebra.p.p.expr
        return OrderConditions.from_rdflib(compiled_conditions)

    def stringify(self) -> str:
        """
        Returns the string representation of an `OrderConditions`. It is used to
        save an `OrderConditions` in a Protobuf message.

        Returns
        -------
        str
            The string representation of an `OrderConditions`.
        """
        conditions = []
        for expression, order in self.conditions:
            if order == "DESC":
                conditions.append(f"DESC({expression.stringify()})")
            else:
                conditions.append(expression.stringify())
        return " ".join(conditions)

    def eval(self, mappings: Mappings) -> List[Any]:
        """
        Evaluates the ORDER BY conditions with the given solution mappings.

        Parameters
        ----------
        Mappings
            A solution mappings.

        Returns
        -------
        List[Any]
            The result of evaluating the ORDER BY conditions with the given
            mappings.
        """
        results = []
        for expression, _ in self.conditions:
            results.append(expression.eval(mappings))
        return results

    def decompose(self) -> List[OrderConditions]:
        conditions = []
        for i in range(len(self.conditions)):
            partial_conditions = OrderConditions(self.conditions[:i + 1])
            is_partial = (i + 1) < len(self.conditions)
            conditions.append((partial_conditions, is_partial))
        return conditions
