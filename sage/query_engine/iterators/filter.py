# filter.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional, Union

from rdflib import Literal, URIRef, Variable
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.util import from_n3

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.primitives import PreemptiveLoop
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
      * mu: Last set of mappings read by the iterator.
    """

    def __init__(self, source: PreemptableIterator, expression: str, mu: Optional[Dict[str, str]] = None):
        super(FilterIterator, self).__init__()
        self._source = source
        self._raw_expression = expression
        self._mu = mu
        # compile the expression using rdflib
        compiled_expr = parseQuery(f"SELECT * WHERE {{?s ?p ?o . FILTER({expression})}}")
        compiled_expr = translateQuery(compiled_expr)
        self._prologue = compiled_expr.prologue
        self._compiled_expression = compiled_expr.algebra.p.p.expr

    def __repr__(self) -> str:
        return f"<FilterIterator '{self._raw_expression}' on {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "filter"

    def _evaluate(self, bindings: Dict[str, str]) -> bool:
        """Evaluate the FILTER expression with a set mappings.
        
        Argument: A set of solution mappings.

        Returns: The outcome of evaluating the SPARQL FILTER on the input set of solution mappings.
        """
        d = {Variable(key[1:]): to_rdflib_term(value) for key, value in bindings.items()}
        b = Bindings(d=d)
        context = QueryContext(bindings=b)
        context.prologue = self._prologue
        return self._compiled_expression.eval(context)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        if self._mu is None:
            self._mu = await self._source.next()
        with PreemptiveLoop() as loop:
            while not self._evaluate(self._mu):
                self._mu = await self._source.next()
                await loop.tick()
        if not self.has_next():
            raise StopAsyncIteration()
        mu = self._mu
        self._mu = None
        return mu

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._mu is not None or self._source.has_next()

    def save(self) -> SavedFilterIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_filter = SavedFilterIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_filter, source_field).CopyFrom(self._source.save())
        saved_filter.expression = self._raw_expression
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_filter.mu)
        return saved_filter
