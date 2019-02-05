# filter.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedFilterIterator
from sage.query_engine.iterators.utils import IteratorExhausted
from rdflib import URIRef, Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.sparql import QueryContext, Bindings
from rdflib.util import from_n3


def to_rdflib_term(value):
    """Convert a N3 term to a RDFLib Term"""
    if value.startswith('http'):
        return URIRef(value)
    elif '"^^http' in value:
        index = value.find('"^^http')
        value = "{}<{}>".format(value[0:index+3], value[index+3:])
    return from_n3(value)


class FilterIterator(PreemptableIterator):
    """A FilterIterator evaluates a FILTER clause on set of mappings"""

    def __init__(self, source, expression):
        super(FilterIterator, self).__init__()
        self._source = source
        self._raw_expression = expression
        # compile the expression using rdflib
        compiled_expr = parseQuery("SELECT * WHERE {?s ?p ?o FILTER(" + expression + ")}")
        compiled_expr = translateQuery(compiled_expr)
        self._compiled_expression = compiled_expr.algebra.p.p.expr

    def __repr__(self):
        return "<FilterIterator '{}' on {}>".format(self._raw_expression, self._source)

    def serialized_name(self):
        return "filter"

    def _evaluate(self, bindings):
        """Evaluate the FILTER expression with a set mappings"""
        d = {Variable(key[1:]): to_rdflib_term(value) for key, value in bindings.items()}
        b = Bindings(d=d)
        context = QueryContext(bindings=b)
        return self._compiled_expression.eval(context)

    async def next(self):
        if not self.has_next():
            raise IteratorExhausted()
        mu = await self._source.next()
        while not self._evaluate(mu):
            mu = await self._source.next()
        return mu

    def has_next(self):
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        saved_filter = SavedFilterIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_filter, source_field).CopyFrom(self._source.save())
        saved_filter.expression = self._raw_expression
        return saved_filter
