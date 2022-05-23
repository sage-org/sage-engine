import logging

from typing import Dict, Optional, Union, Set, Any, List, Tuple
from rdflib.term import Literal, URIRef, Variable
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.util import from_n3

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedFilterIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


def to_rdflib_term(value: str) -> Union[Literal, URIRef, Variable]:
    """Convert a N3 term to a RDFLib Term.

    Argument: A RDF Term in N3 format.

    Returns: The RDF Term in rdflib format.
    """
    if value.startswith('http'):
        return URIRef(value)
    elif '"^^http' in value:
        index = value.find('"^^http')
        value = f"{value[0:index+3]}<{value[index+3:]}>"
    return from_n3(value)


class FilterIterator(PreemptableIterator):
    """A FilterIterator evaluates a FILTER clause in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * expression: A SPARQL FILTER expression.
      * context: Information about the query execution.
    """

    def __init__(
        self, source: PreemptableIterator,
        expression: str,
        constrained_variables: List[str],
        compiled_expression: Expr,
        mu: Optional[Dict[str, str]] = None,
        consumed: int = 0, produced: int = 0
    ):
        super(FilterIterator, self).__init__()
        self._source = source
        self._expression = expression
        self._constrained_variables = constrained_variables
        self._compiled_expression = compiled_expression
        self._mu = mu  # check if this attribute is necessary !!!
        self._consumed = consumed
        self._produced = produced
        self._cache = dict()
        self._coverage = 0.0
        self._cost = 0.0

    def __repr__(self) -> str:
        return f"<FilterIterator '{self._compiled_expression.name}' on {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "filter"

    def explain(self, height: int = 0, step: int = 3) -> None:
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        print(f'{prefix}FilterIterator (cost={self._cost}) (coverage={self._coverage}) <{str(self.constrained_variables())}>')
        self._source.explain(height=(height + step), step=step)

    def constrained_variables(self) -> List[str]:
        return self._constrained_variables

    def variables(self, include_values: bool = False) -> Set[str]:
        return self._source.variables(include_values=include_values)

    def __evaluate__(self, mappings: Dict[str, str]) -> bool:
        """Evaluate the FILTER expression with a set mappings.

        Argument: A set of solution mappings.

        Returns: The outcome of evaluating the SPARQL FILTER on the input set of solution mappings.
        """
        try:
            d = {Variable(key[1:]): to_rdflib_term(value) for key, value in mappings.items()}
            context = QueryContext(bindings=Bindings(d=d))
            return self._compiled_expression.eval(context)
        except Exception as error:
            logging.error(error)
            return False

    def next_stage(self, mappings: Dict[str, str], context: Dict[str, Any] = {}):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings, context=context)

    async def next(self, context: Dict[str, Any] = {}) -> Tuple[Optional[Dict[str, str]], List[int]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        mappings = None
        while mappings is None or not self.__evaluate__(mappings):
            mappings = await self._source.next(context=context)
            if mappings is None:
                return None
            self._consumed += 1
        self._produced += 1
        return mappings

    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        self._coverage = self._source.update_coverage(context=context)
        return self._coverage

    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        if self._produced == 0:
            selectivity = 1 / 3
        else:
            selectivity = self._produced / self._consumed
        self._cost = self._source.update_cost(context=context)
        self._cost *= selectivity
        context['cost__cout'] = self._cost
        return self._cost

    def save(self) -> SavedFilterIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_filter = SavedFilterIterator()
        source_field = f'{self._source.serialized_name()}_source'
        getattr(saved_filter, source_field).CopyFrom(self._source.save())
        saved_filter.expression = self._expression
        saved_filter.variables.extend(self._constrained_variables)
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_filter.mu)
        saved_filter.consumed = self._consumed
        saved_filter.produced = self._produced
        saved_filter.cost = self._cost
        return saved_filter
