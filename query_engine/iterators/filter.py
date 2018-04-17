# filter.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.preemptable_iterator import PreemptableIterator
from query_engine.filter.utils import string_to_rdf
from query_engine.protobuf.iterators_pb2 import SavedFilterIterator
from query_engine.iterators.utils import IteratorExhausted
from string import Template


class FilterIterator(object):
    """A FilterIterator evaluates a FILTER clause on set of mappings"""
    def __init__(self, source, expression, variables):
        super(FilterIterator, self).__init__()
        self._source = source
        self._expression = expression
        self._template = Template(expression)
        self._variables = variables

    def __repr__(self):
        return "<FilterIterator '{}' (variables: {}) on {}>".format(self._expression, self._variables, self._source)

    def _evaluate(self, bindings):
        b = dict()
        for key, value in bindings.items():
            b[key[1:]] = string_to_rdf(value)
        return eval(self._template.substitute(b))

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
        savedFilter = SavedFilterIterator()
        if type(self._source) is FilterIterator:
            savedFilter.filter_source.CopyFrom(self._source.save())
        else:
            savedFilter.proj_source.CopyFrom(self._source.save())
        savedFilter.expression = self._expression
        savedFilter.variables.extend(self._variables)
        return savedFilter
